use grpc::{pb::file_service_server::FileServiceServer, MinioFileService};
use tonic::transport::Server;

mod grpc;

async fn start_server() -> anyhow::Result<()> {
    let addr = "0.0.0.0:3000".parse()?;

    let user = MinioFileService::default();

    log::info!("FileService listening on {}", addr);

    Server::builder()
        // GrpcWeb is over http1 so we must enable it.
        .add_service(FileServiceServer::new(user))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    start_server().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{env, process::exit, time::Duration};

    use grpc::pb::{file_service_client::FileServiceClient, FilePart, FileStreamRequest, StreamerType};
    use minior::{core::upload::upload_object::UploadObjectAdditionalOptions, Minio};
    use tokio::fs::File;
    use tokio_stream::StreamExt;
    use tonic::Status;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_client_6_mb_file() -> anyhow::Result<()> {
        env_logger::init();
        env::set_var("AWS_REGION", "us-east-1");
        env::set_var("AWS_ACCESS_KEY_ID", "testingpass");
        env::set_var("AWS_SECRET_ACCESS_KEY", "testingpass");
        env::set_var("AWS_ENDPOINT_URL", "http://127.0.0.1:9000");
        
        let client = Minio::new("http://127.0.0.1:9000").await;
        if !client.bucket_exists("my-bucket").await? {
            client.create_bucket("my-bucket").await?;
        }

        let buf = File::open("data/6mbfile.txt").await?;
        let _ = client.upload_object("my-bucket", "6mbfile.txt", buf, Some(UploadObjectAdditionalOptions::default())).await;

        tokio::select!(
            _ = start_server() => {
                log::info!("Server exited");
            },
            _ = async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let mut fs_client = FileServiceClient::connect("http://127.0.0.1:3000").await.expect("No file service conn");

                log::info!("Starting testing...");
                let res_stream = fs_client.stream_file(FileStreamRequest { 
                    filename: String::from("6mbfile.txt"),
                    transaction_id: String::from("transaction #1"),
                    stream_type: StreamerType::SimplerBetter.into()
                }).await.expect("No file stream");

                type FilePartResults = Vec<Result<FilePart, Status>>;
                let responses = res_stream.into_inner().collect::<FilePartResults>().await;

                assert_eq!(2, responses.len());
                log::info!("Finished DeepDived response.");

                let res_stream_simpler = fs_client.stream_file(FileStreamRequest { 
                    filename: String::from("6mbfile.txt"),
                    transaction_id: String::from("transaction #2"),
                    stream_type: StreamerType::DeepDived.into()
                }).await.expect("No file stream");

                let responses_simpler = res_stream_simpler.into_inner().collect::<FilePartResults>().await;

                assert_eq!(2, responses_simpler.len());
                log::info!("Finished SimplerBetter reponse.");
            } => {
                log::info!("Client exited.");
            }
        );
        Ok(())
    }
}