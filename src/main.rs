pub mod cli;
pub mod compression;
pub mod database;
pub mod remote;
pub mod sync;

fn main() {
    if let Err(err) = cli::run() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
