use aws_config::from_env;
use aws_sdk_s3::{
    error::{GetObjectError, GetObjectErrorKind},
    model::{Object, ObjectCannedAcl},
    Client, Region as AwsRegion,
};
use flate2::{
    write::{GzDecoder, GzEncoder},
    Compression,
};
use std::io::Write;

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

    pub async fn save(&self, path: &str, content: &[u8]) -> eyre::Result<()> {
        tracing::trace!(target: "remote", path, "Compressing file");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(content)?;
        let compressed = encoder.finish()?;

        let _ = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(compressed.into())
            .acl(ObjectCannedAcl::Private)
            .send()
            .await?;

        Ok(())
    }

    pub async fn delete(&self, path: &str) -> eyre::Result<()> {
        let _ = self.client.delete_object().bucket(&self.bucket).key(path).send().await?;

        Ok(())
    }
}
