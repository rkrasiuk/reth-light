use flate2::{write::GzEncoder, Compression};
use std::{
    fs::File,
    io::{copy, BufReader},
    path::Path,
    time::Instant,
};
use tempfile::tempfile;

pub fn compress_file(path: &Path) -> eyre::Result<File> {
    tracing::trace!(target: "compression", path = %path.display(), "Compressing file");
    let mut input = BufReader::new(File::open(path)?);
    let output = tempfile()?;
    let mut encoder = GzEncoder::new(output, Compression::default());
    let start = Instant::now();
    copy(&mut input, &mut encoder)?;
    let output = encoder.finish()?;
    let source_len = input.get_ref().metadata()?.len();
    let target_len = output.metadata()?.len();
    tracing::trace!(target: "compression", elapsed = start.elapsed().as_secs(), source_len, target_len, "Finished compressing");
    Ok(output)
}
