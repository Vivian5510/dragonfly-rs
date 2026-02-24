//! Binary entrypoint for `dfly-server`.

mod app;

fn main() {
    if let Err(err) = app::run() {
        eprintln!("failed to start dfly-server: {err}");
        std::process::exit(1);
    }
}
