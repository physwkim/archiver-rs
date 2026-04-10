use std::io::Result;

fn main() -> Result<()> {
    let file_descriptors = protox::compile(["src/proto/EPICSEvent.proto"], ["src/proto/"])
        .expect("failed to compile proto files");
    let mut config = prost_build::Config::new();
    config.btree_map(["."]);
    config.compile_fds(file_descriptors)?;
    Ok(())
}
