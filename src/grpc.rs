pub mod pb {
    tonic::include_proto!("sample");
}

use std::{borrow::BorrowMut, cell::{RefCell, RefMut}, fmt::Display, io::Write, pin::Pin, task::Poll};
use pb::FilePart;
use tokio_stream::Stream;
use std::task::ready;
use std::pin::pin;
use std::future::Future;
use tonic::{Response, Status, Result};
use minior::Minio;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};
use bytes::{Buf, BufMut, BytesMut};

use crate::grpc::pb::FileStreamRequest;

pub(crate) type StreamType = dyn Stream<Item = Result<FilePart, Status>> + Send;
pub(crate) type StreamFileStream = Pin<Box<StreamType>>;

#[derive(Default)]
pub struct MinioFileService;

pub trait IntoGrpcError<T> {
    fn into_grpc_result(self) -> Result<T, Status>;
}

impl<T, E: Display> IntoGrpcError<T> for Result<T, E> {
    fn into_grpc_result(self) -> Result<T, Status> {
        self.map_err(|x| Status::failed_precondition(format!("{}", x)))
    }
}

pub struct MinioObjectStream {
    receive_buffer: RefCell<BytesMut>,
    // If we are passing it back it must be Send
    // because it's dyn we need a box
    // because read_buf requires Unpin we need it pinned
    // This Pin<Box<dyn ASyncBufRead + Send>> pattern is so common there are convenience functions for some of this
    receive_source: RefCell<Pin<Box<dyn AsyncBufRead + Send>>>,

    transaction_id: String,
    current_offset: RefCell<i64>,
    total_size: i64
}

impl MinioObjectStream {
    fn send_grpc_response(transaction_id: String, offset: i64, size: i64, global_buf: &mut RefMut<BytesMut>) -> std::task::Poll<Option<Result<FilePart, Status>>> {
        let result = Some(
            Ok(
                FilePart {
                    transaction_id: transaction_id,
                    offset: offset,
                    size: size,
                    content: global_buf.to_vec(),
                }
            )
        );
        // Preserves capacity, clears the buffer
        global_buf.clear();
        Poll::Ready(result)
    }
}

impl Stream for MinioObjectStream 
{
    type Item = Result<FilePart, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let mut buf = BytesMut::with_capacity(1_000_000);
        let mut global_buf = self.receive_buffer.borrow_mut();
        let mut receive = self.receive_source.borrow_mut();
        let mut res = receive.read_buf(&mut buf);
        // If we got an error we get outta here, or if res gives us a Poll::Pending
        let bytes = match ready!(pin!(res).poll(cx)) {
            Ok(x) => x,
            Err(e) => {
                log::error!("Io error {}", e);
                return Poll::Ready(Some(Err(Status::from_error(Box::new(e)))));
            },
        };
        if buf.len() > 0 {
            global_buf.extend_from_slice(buf.chunk());
            let mut offset = self.current_offset.borrow_mut();
            *offset += bytes as i64;
        }
        let offset = self.current_offset.borrow().clone();
        if global_buf.len() > 3_000_000 && global_buf.len() < 4_000_000 {
            // Buffer full
            return MinioObjectStream::send_grpc_response(
                self.transaction_id.clone(),offset,self.total_size,&mut global_buf);
        }
        if bytes == 0 {
            if global_buf.len() > 0 {
                // EOF reached - but some still in buffer
                MinioObjectStream::send_grpc_response(
                    self.transaction_id.clone(),offset,self.total_size,&mut global_buf)
            } else {
                // EOF reached
                Poll::Ready(None)
            }
        } else {
            Poll::Pending
        }
    }
}

impl MinioFileService {
    async fn connect_to_minio(&self) -> anyhow::Result<Minio> {
        env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher

        let base_url = "http://127.0.0.1:9000";

        log::info!("Trying to connect to MinIO at: `{:?}`", base_url);

        Ok(
            Minio::new(base_url).await
        )
    }
}

#[tonic::async_trait]
impl pb::file_service_server::FileService for MinioFileService {
    type StreamFileStream = crate::grpc::StreamFileStream;

    async fn stream_file(&self, file_request: tonic::Request<FileStreamRequest>) -> Result<Response<Self::StreamFileStream>> {
        let client = match self.connect_to_minio().await {
            Ok(x) => x,
            Err(e) => {
                log::error!("Couldn't connect {}", e);
                return Err(Status::failed_precondition("Couldn't connect to upstream server."));
            }
        };

        if !client.bucket_exists("my-bucket").await.into_grpc_result()? {
            return Err(Status::not_found("Bucket not found"));
        }

        let mut file_stream = match client.get_object("my-bucket", &file_request.get_ref().filename).await.into_grpc_result()? {
            Some(x) => x,
            None => {
                return Err(Status::not_found("File not found"));
            }
        };

        // let stream = async_stream::stream! {
        //     let transaction_id = file_request.get_ref().transaction_id.clone();
        //     let mut buffer = BytesMut::with_capacity(4000000);
        //     let mut offset = 0i64;
        //     while let size = file_stream.read_buf(&mut buffer).await? {
        //         yield Ok(FilePart {
        //             transaction_id: transaction_id.clone(),
        //             offset: offset,
        //             size: 0,
        //             content: buffer.to_vec(),
        //         });
        //         offset += size as i64;
        //         buffer.clear();
        //     }
        // };

        // Ok(Response::new(
        //     Box::pin(stream) as Self::StreamFileStream
        // ))

        // Guess what the max size for a message in grpc is 4MB
        let wrapped_stream = MinioObjectStream {
            receive_buffer: RefCell::new(BytesMut::with_capacity(4_000_000)), 
            receive_source: RefCell::new(Box::pin(file_stream)), 
            transaction_id: file_request.get_ref().transaction_id.clone(), 
            current_offset: RefCell::new(0), 
            total_size: 0 // Should get it from minio first I guess
        };        

        Ok(Response::new(
            Box::pin(wrapped_stream) as Self::StreamFileStream
        ))
    }
}