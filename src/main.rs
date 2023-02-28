pub mod cli;
pub mod database;
pub mod remote;
pub mod sync;
pub mod uploader;

fn main() {
    if let Err(err) = cli::run() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
