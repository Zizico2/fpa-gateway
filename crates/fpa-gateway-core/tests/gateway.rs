use fpa_gateway_core::{Gateway, GatewayOptions};
use fpa_proto::v1::{
    self as pb,
    results_service_server::{ResultsService, ResultsServiceServer},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
#[derive(Clone, Default)]
struct Adapter {
    calls: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
}
#[tonic::async_trait]
impl ResultsService for Adapter {
    async fn ping(
        &self,
        _: Request<pb::PingRequest>,
    ) -> Result<Response<pb::PingResponse>, Status> {
        Ok(Response::new(pb::PingResponse {
            message: "ok".into(),
        }))
    }
    async fn list_seasons(
        &self,
        _: Request<pb::ListSeasonsRequest>,
    ) -> Result<Response<pb::ListSeasonsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    async fn list_competitions(
        &self,
        _: Request<pb::ListCompetitionsRequest>,
    ) -> Result<Response<pb::ListCompetitionsResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    async fn list_phases(
        &self,
        _: Request<pb::ListPhasesRequest>,
    ) -> Result<Response<pb::ListPhasesResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    async fn get_match(
        &self,
        _: Request<pb::GetMatchRequest>,
    ) -> Result<Response<pb::GetMatchResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    async fn get_phase(
        &self,
        request: Request<pb::GetPhaseRequest>,
    ) -> Result<Response<pb::GetPhaseResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(Status::unavailable("fixture failure"));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        let id = request
            .metadata()
            .get("authorization")
            .and_then(|s| s.to_str().ok())
            .unwrap_or("public")
            .to_string();
        let mut response = Response::new(pb::GetPhaseResponse {
            matches: vec![pb::Match {
                id,
                ..Default::default()
            }],
            ..Default::default()
        });
        response
            .metadata_mut()
            .insert("x-source", "fixture".parse().unwrap());
        Ok(response)
    }
}
async fn setup(ttl: Duration) -> (Gateway, Adapter, tokio::task::JoinHandle<()>) {
    let adapter = Adapter::default();
    let service = ResultsServiceServer::new(adapter.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let gateway = Gateway::connect(GatewayOptions {
        upstream_address: format!("http://{addr}"),
        cache_ttl_cap: Some(ttl),
        ..Default::default()
    })
    .await
    .unwrap();
    (gateway, adapter, server)
}
fn request() -> Request<pb::GetPhaseRequest> {
    Request::new(pb::GetPhaseRequest {
        competition_id: "10019".into(),
        phase_id: "28412".into(),
    })
}
#[tokio::test]
async fn concurrent_public_misses_coalesce_and_metadata_survives() {
    let (gateway, adapter, server) = setup(Duration::from_secs(60)).await;
    let (a, b) = tokio::join!(gateway.get_phase(request()), gateway.get_phase(request()));
    assert_eq!(a.unwrap().metadata().get("x-source").unwrap(), "fixture");
    assert_eq!(b.unwrap().get_ref().matches[0].id, "public");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let mut private = request();
    private
        .metadata_mut()
        .insert("authorization", "private".parse().unwrap());
    assert_eq!(
        gateway.get_phase(private).await.unwrap().get_ref().matches[0].id,
        "private"
    );
    assert_eq!(
        gateway
            .get_phase(request())
            .await
            .unwrap()
            .get_ref()
            .matches[0]
            .id,
        "public"
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
    server.abort();
}
#[tokio::test]
async fn errors_are_never_cached_and_expiry_reloads() {
    let (gateway, adapter, server) = setup(Duration::from_millis(50)).await;
    adapter.fail.store(true, Ordering::SeqCst);
    assert!(gateway.get_phase(request()).await.is_err());
    adapter.fail.store(false, Ordering::SeqCst);
    assert!(gateway.get_phase(request()).await.is_ok());
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(gateway.get_phase(request()).await.is_ok());
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    server.abort();
}
#[tokio::test]
async fn rejects_allowlisting_credentials() {
    let result = Gateway::connect(GatewayOptions {
        cache_neutral_metadata: vec!["authorization".into()],
        ..Default::default()
    })
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn phase_identifiers_partition_cache_entries() {
    let (gateway, adapter, server) = setup(Duration::from_secs(60)).await;
    gateway.get_phase(request()).await.unwrap();
    let mut other = request();
    other.get_mut().phase_id = "28413".into();
    gateway.get_phase(other).await.unwrap();
    gateway.get_phase(request()).await.unwrap();
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
    server.abort();
}
