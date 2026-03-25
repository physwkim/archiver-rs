pub mod epics_event {
    include!(concat!(env!("OUT_DIR"), "/epics.rs"));
}

pub use epics_event::*;
