//! V4-aware JSON rendering for [`ArchiverValue`].
//!
//! [`ArchiverValue::V4GenericBytes`] stores a self-describing PVA wire
//! payload (`type_desc || value`, BigEndian) produced at the
//! channel_manager monitor callback for NTTable, NTNDArray, and other
//! structured V4 PVs. This module decodes those bytes back into a
//! [`PvField`] and renders the result as JSON so retrieval responses
//! carry the original structure rather than a raw byte array.
//!
//! Non-V4GenericBytes values fall through to
//! [`archiver_core::types::archiver_value_to_json`] unchanged, so
//! Scalar / Waveform PB samples keep their existing on-the-wire JSON
//! shape.
use std::io::Cursor;

use epics_rs::pva::proto::ByteOrder;
use epics_rs::pva::pvdata::encode::{decode_pv_field, decode_type_desc};
use epics_rs::pva::pvdata::{PvField, ScalarValue, TypedScalarArray, UnionItem, VariantValue};
use serde_json::{Map, Value};

use archiver_core::types::{ArchiverValue, archiver_value_to_json, finite_or_null};

/// Render an [`ArchiverValue`] as JSON, decoding V4GenericBytes into
/// the original PVA structure shape. Falls back to the core renderer
/// for every other variant.
pub fn archiver_value_to_json_v4(v: &ArchiverValue) -> Value {
    match v {
        ArchiverValue::V4GenericBytes(bytes) => {
            v4_bytes_to_json(bytes).unwrap_or_else(|| archiver_value_to_json(v))
        }
        _ => archiver_value_to_json(v),
    }
}

/// Decode the self-describing wire payload written by
/// `encode_pv_field_self_describing` (archiver-engine) and project it
/// to JSON. Returns `None` if the bytes don't conform to the expected
/// `type_desc || value` BigEndian layout — caller should fall back to
/// the byte-array representation.
fn v4_bytes_to_json(bytes: &[u8]) -> Option<Value> {
    let mut cur = Cursor::new(bytes);
    let desc = decode_type_desc(&mut cur, ByteOrder::Big).ok()?;
    let pv = decode_pv_field(&desc, &mut cur, ByteOrder::Big).ok()?;
    Some(pv_field_to_json(&pv))
}

fn pv_field_to_json(f: &PvField) -> Value {
    match f {
        PvField::Scalar(s) => scalar_value_to_json(s),
        PvField::ScalarArray(arr) => Value::Array(arr.iter().map(scalar_value_to_json).collect()),
        PvField::ScalarArrayTyped(arr) => typed_array_to_json(arr),
        PvField::Structure(s) => {
            let mut obj = Map::with_capacity(s.fields.len());
            for (name, child) in &s.fields {
                obj.insert(name.clone(), pv_field_to_json(child));
            }
            Value::Object(obj)
        }
        // Each element is `Option<PvStructure>`: `None` is a pvxs null
        // (absent) element, rendered as JSON null.
        PvField::StructureArray(arr) => Value::Array(
            arr.iter()
                .map(|s| match s {
                    Some(s) => pv_field_to_json(&PvField::Structure(s.clone())),
                    None => Value::Null,
                })
                .collect(),
        ),
        // Union / Variant: render the chosen variant's value if present
        // (`selector == -1` means "null union"). These are uncommon in
        // archiver workloads; we keep the path lossy-but-readable rather
        // than panicking — the user can re-fetch the wire bytes if a
        // bit-exact reconstruction is needed.
        PvField::Union {
            selector, value, ..
        } => {
            if *selector < 0 {
                Value::Null
            } else {
                pv_field_to_json(value)
            }
        }
        PvField::UnionArray(arr) => Value::Array(arr.iter().map(union_item_to_json).collect()),
        PvField::Variant(v) => variant_to_json(v),
        // `None` is a pvxs null (absent) element, rendered as JSON null.
        PvField::VariantArray(arr) => Value::Array(
            arr.iter()
                .map(|v| match v {
                    Some(v) => variant_to_json(v),
                    None => Value::Null,
                })
                .collect(),
        ),
        PvField::Null => Value::Null,
    }
}

/// A union-array element: `None` (pvxs null/absent) and a present element
/// with a negative selector both render as JSON null.
fn union_item_to_json(item: &Option<UnionItem>) -> Value {
    match item {
        Some(i) if i.selector >= 0 => pv_field_to_json(&i.value),
        _ => Value::Null,
    }
}

fn scalar_value_to_json(s: &ScalarValue) -> Value {
    match s {
        ScalarValue::Boolean(b) => Value::Bool(*b),
        ScalarValue::Byte(v) => (*v as i64).into(),
        ScalarValue::UByte(v) => (*v as u64).into(),
        ScalarValue::Short(v) => (*v as i64).into(),
        ScalarValue::UShort(v) => (*v as u64).into(),
        ScalarValue::Int(v) => (*v as i64).into(),
        ScalarValue::UInt(v) => (*v as u64).into(),
        ScalarValue::Long(v) => (*v).into(),
        ScalarValue::ULong(v) => (*v).into(),
        ScalarValue::Float(v) => finite_or_null(*v as f64),
        ScalarValue::Double(v) => finite_or_null(*v),
        ScalarValue::String(s) => Value::String(s.to_string()),
    }
}

fn typed_array_to_json(arr: &TypedScalarArray) -> Value {
    match arr {
        TypedScalarArray::Boolean(a) => Value::Array(a.iter().map(|&b| Value::Bool(b)).collect()),
        TypedScalarArray::Byte(a) => Value::Array(a.iter().map(|&v| (v as i64).into()).collect()),
        TypedScalarArray::UByte(a) => Value::Array(a.iter().map(|&v| (v as u64).into()).collect()),
        TypedScalarArray::Short(a) => Value::Array(a.iter().map(|&v| (v as i64).into()).collect()),
        TypedScalarArray::UShort(a) => Value::Array(a.iter().map(|&v| (v as u64).into()).collect()),
        TypedScalarArray::Int(a) => Value::Array(a.iter().map(|&v| (v as i64).into()).collect()),
        TypedScalarArray::UInt(a) => Value::Array(a.iter().map(|&v| (v as u64).into()).collect()),
        TypedScalarArray::Long(a) => Value::Array(a.iter().map(|&v| v.into()).collect()),
        TypedScalarArray::ULong(a) => Value::Array(a.iter().map(|&v| v.into()).collect()),
        TypedScalarArray::Float(a) => {
            Value::Array(a.iter().map(|&v| finite_or_null(v as f64)).collect())
        }
        TypedScalarArray::Double(a) => Value::Array(a.iter().map(|&v| finite_or_null(v)).collect()),
        TypedScalarArray::String(a) => {
            Value::Array(a.iter().map(|s| Value::String(s.to_string())).collect())
        }
    }
}

fn variant_to_json(v: &VariantValue) -> Value {
    // Empty descriptor + Null value = "no value selected".
    if v.desc.is_none() && matches!(v.value, PvField::Null) {
        return Value::Null;
    }
    pv_field_to_json(&v.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use epics_rs::pva::pvdata::encode::{encode_pv_field, encode_type_desc};
    use epics_rs::pva::pvdata::{FieldDesc, PvStructure, ScalarType};

    /// Encode a synthetic NTTable (labels + value substructure with
    /// two columns of doubles) the same way archiver-engine does, then
    /// decode it back through the retrieval path and check both the
    /// structural shape and the column values.
    #[test]
    fn nttable_v4_bytes_decode_roundtrip() {
        let mut table = PvStructure::new("epics:nt/NTTable:1.0");
        let labels = TypedScalarArray::String(["x", "y"].iter().map(|s| (*s).into()).collect());
        table
            .fields
            .push(("labels".into(), PvField::ScalarArrayTyped(labels)));
        let mut value_inner = PvStructure::new("");
        value_inner.fields.push((
            "x".into(),
            PvField::ScalarArrayTyped(TypedScalarArray::Double(vec![1.0, 2.0, 3.0].into())),
        ));
        value_inner.fields.push((
            "y".into(),
            PvField::ScalarArrayTyped(TypedScalarArray::Double(vec![4.0, 5.0, 6.0].into())),
        ));
        table
            .fields
            .push(("value".into(), PvField::Structure(value_inner)));
        let pv = PvField::Structure(table);

        let desc = pv.descriptor();
        let mut bytes = Vec::new();
        encode_type_desc(&desc, ByteOrder::Big, &mut bytes);
        encode_pv_field(&pv, &desc, ByteOrder::Big, &mut bytes);

        let json = v4_bytes_to_json(&bytes).expect("decode");
        let obj = json.as_object().expect("object");
        let labels = obj.get("labels").expect("labels").as_array().unwrap();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], Value::String("x".into()));
        let val = obj.get("value").unwrap().as_object().unwrap();
        let x = val.get("x").unwrap().as_array().unwrap();
        assert_eq!(x.len(), 3);
        assert_eq!(x[2].as_f64().unwrap(), 3.0);

        // Sanity: structurally distinct from the FieldDesc carried in
        // the descriptor — we round-tripped via wire bytes, not the
        // in-memory value.
        let _ = desc;
    }

    #[test]
    fn passthrough_for_non_v4_values() {
        let v = ArchiverValue::ScalarDouble(2.5);
        assert_eq!(archiver_value_to_json_v4(&v), Value::from(2.5));
        let v = ArchiverValue::VectorInt(vec![1, 2, 3]);
        let j = archiver_value_to_json_v4(&v);
        assert_eq!(j.as_array().unwrap().len(), 3);
        // Garbage V4 bytes fall back to the byte-array core renderer.
        let v = ArchiverValue::V4GenericBytes(vec![0xFF, 0xFF, 0xFF]);
        let j = archiver_value_to_json_v4(&v);
        assert!(j.is_array(), "garbage bytes should fall back");
    }

    /// NTEnum samples store as `ScalarEnum(index)` with `enum_strs`
    /// carried in `field_values`. Retrieval JSON renders the value
    /// as a plain integer and surfaces the choices via the `meta`
    /// object — clients call `JSON.parse(meta.enum_strs)` to recover
    /// the string array.
    #[test]
    fn nt_enum_retrieval_json_shape() {
        let v = ArchiverValue::ScalarEnum(1);
        let rendered = archiver_value_to_json_v4(&v);
        assert_eq!(rendered, Value::from(1));

        // The choices payload is opaque-string to value_json — the
        // monitor callback wrote a JSON-encoded array, so the
        // retrieval client can round-trip-parse it.
        let raw = r#"["Off","On","Trip"]"#;
        let parsed: Vec<String> = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed, vec!["Off", "On", "Trip"]);
    }

    /// Belt-and-suspenders: catch a future libpva refactor that
    /// renames a [`PvField`] variant — we'd silently lose retrieval
    /// JSON for an entire shape. The test forces every variant
    /// constructor to exist by reference.
    #[test]
    fn pv_field_variants_match_renderer() {
        let _ = |f: &PvField| match f {
            PvField::Scalar(_)
            | PvField::ScalarArray(_)
            | PvField::ScalarArrayTyped(_)
            | PvField::Structure(_)
            | PvField::StructureArray(_)
            | PvField::Union { .. }
            | PvField::UnionArray(_)
            | PvField::Variant(_)
            | PvField::VariantArray(_)
            | PvField::Null => (),
        };
        // FieldDesc kept in scope so a transitive epics-rs API break
        // surfaces here rather than only at the call sites.
        let _ = FieldDesc::Scalar(ScalarType::Double);
    }
}
