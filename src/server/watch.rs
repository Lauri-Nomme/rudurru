use crate::proto::etcdserverpb;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct Watch;

#[tonic::async_trait]
impl etcdserverpb::watch_server::Watch for Watch {
    type WatchStream = ReceiverStream<Result<etcdserverpb::WatchResponse, Status>>;

    async fn watch(
        &self,
        _req: Request<tonic::Streaming<etcdserverpb::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Err(Status::unimplemented("not implemented")));
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
