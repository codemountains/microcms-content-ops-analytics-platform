use std::env;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;

use crate::config::env_bool;

pub(crate) async fn client_from_env() -> S3Client {
    let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Ok(endpoint_url) = env::var("AWS_ENDPOINT_URL") {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
    }
    if env_bool("AWS_S3_FORCE_PATH_STYLE") {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }
    S3Client::from_conf(s3_config_builder.build())
}
