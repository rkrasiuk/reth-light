use aws_config::from_env;
use aws_sdk_s3::{
    error::{GetObjectError, GetObjectErrorKind},
    model::{Object, ObjectCannedAcl},
    Client, Region as AwsRegion,
};
use aws_smithy_http::byte_stream::ByteStream;
use filepath::FilePath;
use flate2::write::GzDecoder;
use std::{io::Write, path::Path};

use crate::compression::compress_file;

pub struct DigitalOceanStore {
    bucket: String,
    client: Client,
}

impl DigitalOceanStore {
    pub async fn new(region: String, bucket: String) -> Self {
        let endpoint = format!("https://{region}.digitaloceanspaces.com");
        Self {
            bucket,
            client: Client::new(
                &from_env().region(AwsRegion::new(region)).endpoint_url(endpoint).load().await,
            ),
        }
    }

    pub async fn list(&self, prefix: Option<&str>) -> eyre::Result<Vec<Object>> {
        tracing::trace!(target: "remote::digitalocean", ?prefix, "Listing objects");
        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_prefix(prefix.map(str::to_owned))
            .send()
            .await?;
        Ok(response.contents().unwrap_or_default().to_vec())
    }

    pub async fn retrieve(&self, path: &str) -> eyre::Result<Option<Vec<u8>>> {
        tracing::trace!(target: "remote::digitalocean", path, "Retrieving object");
        match self.client.get_object().bucket(&self.bucket).key(path).send().await {
            Ok(obj) => {
                let mut decoder = GzDecoder::new(Vec::new());
                decoder.write_all(&obj.body.collect().await?.to_vec())?;
                Ok(Some(decoder.finish()?))
            }
            Err(err) => match err.into_service_error() {
                GetObjectError { kind: GetObjectErrorKind::NoSuchKey(_), .. } => Ok(None),
                err @ _ => Err(err.into()),
            },
        }
    }

    pub async fn save(&self, path: &str, content_path: &Path) -> eyre::Result<()> {
        tracing::trace!(target: "remote::digitalocean", path, "Compressing contents");
        let compressed = compress_file(content_path)?;

        tracing::trace!(target: "remote::digitalocean", path, "Putting object");
        let compressed_path = compressed.path();
        tracing::trace!(target: "remote::digitalocean", compressed = %compressed_path.display(), "Creating body from compressed file");
        let body = ByteStream::from_path(compressed_path).await?;
        let _ = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(body)
            .acl(ObjectCannedAcl::Private)
            .send()
            .await?;

        Ok(())
    }

    pub async fn delete(&self, path: &str) -> eyre::Result<()> {
        tracing::trace!(target: "remote::digitalocean", path, "Deleting object");
        let _ = self.client.delete_object().bucket(&self.bucket).key(path).send().await?;

        Ok(())
    }
}
