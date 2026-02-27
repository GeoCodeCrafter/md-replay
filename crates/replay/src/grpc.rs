use crate::engine::{read_events, stream_with_pacing, ReplayConfig, ReplayError};
use crate::pb::replay_service_server::{ReplayService, ReplayServiceServer};
use crate::pb::{self, StreamRequest};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct ServiceState {
    log_path: PathBuf,
    index_path: Option<PathBuf>,
    defaults: ReplayConfig,
}

#[derive(Clone)]
struct ReplaySvc {
    state: ServiceState,
}

#[tonic::async_trait]
impl ReplayService for ReplaySvc {
    type StreamEventsStream = ReceiverStream<Result<pb::EventMessage, Status>>;

    async fn stream_events(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let config = merged_config(&self.state.defaults, &req);

        let events = read_events(
            &self.state.log_path,
            self.state.index_path.as_deref(),
            config.from_ns,
            config.to_ns,
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(stream_with_pacing(events, config, tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub async fn serve_grpc(
    log_path: PathBuf,
    index_path: Option<PathBuf>,
    addr: SocketAddr,
    defaults: ReplayConfig,
) -> Result<(), ReplayError> {
    let service = ReplaySvc {
        state: ServiceState {
            log_path,
            index_path,
            defaults,
        },
    };

    Server::builder()
        .add_service(ReplayServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

fn merged_config(defaults: &ReplayConfig, req: &StreamRequest) -> ReplayConfig {
    ReplayConfig {
        from_ns: if req.from_ns == 0 {
            defaults.from_ns
        } else {
            Some(req.from_ns)
        },
        to_ns: if req.to_ns == 0 {
            defaults.to_ns
        } else {
            Some(req.to_ns)
        },
        speed: if req.speed <= 0.0 {
            defaults.speed
        } else {
            req.speed
        },
        max_speed: defaults.max_speed || req.max_speed,
        step_mode: defaults.step_mode || req.step_mode,
    }
}
