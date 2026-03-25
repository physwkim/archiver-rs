use std::io::Result;

fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config.btree_map(["."]);
    config.compile_protos(&["src/proto/EPICSEvent.proto"], &["src/proto/"])?;
    Ok(())
}
