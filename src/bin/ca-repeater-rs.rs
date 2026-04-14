use epics_rs::ca::repeater::run_repeater;

#[tokio::main]
async fn main() {
    if let Err(e) = run_repeater().await {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            return;
        }
        eprintln!("ca-repeater: {e}");
        std::process::exit(1);
    }
}
