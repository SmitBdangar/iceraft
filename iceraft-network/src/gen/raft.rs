/// A log entry.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entry {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub index: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(enumeration = "EntryType", tag = "4")]
    pub entry_type: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HardState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub vote: u64,
    #[prost(uint64, tag = "3")]
    pub commit: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotMetadata {
    #[prost(uint64, tag = "1")]
    pub index: u64,
    #[prost(uint64, tag = "2")]
    pub term: u64,
    #[prost(uint64, repeated, tag = "3")]
    pub conf_nodes: ::prost::alloc::vec::Vec<u64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Snapshot {
    #[prost(bytes = "vec", tag = "1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub metadata: ::core::option::Option<SnapshotMetadata>,
}
// ─── AppendEntries ─────────────────────────────────────────────────────────────
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub prev_log_term: u64,
    #[prost(message, repeated, tag = "5")]
    pub entries: ::prost::alloc::vec::Vec<Entry>,
    #[prost(uint64, tag = "6")]
    pub leader_commit: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
    #[prost(uint64, tag = "3")]
    pub conflict_index: u64,
    #[prost(uint64, tag = "4")]
    pub conflict_term: u64,
}
// ─── RequestVote ────────────────────────────────────────────────────────────────
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub candidate_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_log_term: u64,
    #[prost(bool, tag = "5")]
    pub pre_vote: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}
// ─── InstallSnapshot ────────────────────────────────────────────────────────────
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_included_term: u64,
    #[prost(uint64, tag = "5")]
    pub offset: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "7")]
    pub done: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
}
/// Entry type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum EntryType {
    EntryNormal = 0,
    EntryConfChange = 1,
}
impl EntryType {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::EntryNormal => "ENTRY_NORMAL",
            Self::EntryConfChange => "ENTRY_CONF_CHANGE",
        }
    }
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ENTRY_NORMAL" => Some(Self::EntryNormal),
            "ENTRY_CONF_CHANGE" => Some(Self::EntryConfChange),
            _ => None,
        }
    }
}
// ─── gRPC Service definitions (tonic) ────────────────────────────────────────

/// Generated client implementations.
pub mod raft_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;

    #[derive(Debug, Clone)]
    pub struct RaftServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RaftServiceClient<tonic::transport::Channel> {
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RaftServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RaftServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            RaftServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn append_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::AppendEntriesRequest>,
        ) -> std::result::Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/raft.RaftService/AppendEntries");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("raft.RaftService", "AppendEntries"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn request_vote(
            &mut self,
            request: impl tonic::IntoRequest<super::RequestVoteRequest>,
        ) -> std::result::Result<tonic::Response<super::RequestVoteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/raft.RaftService/RequestVote");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("raft.RaftService", "RequestVote"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn install_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::InstallSnapshotRequest>,
        ) -> std::result::Result<tonic::Response<super::InstallSnapshotResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(tonic::Code::Unknown, format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/raft.RaftService/InstallSnapshot");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("raft.RaftService", "InstallSnapshot"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod raft_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;

    #[async_trait::async_trait]
    pub trait RaftService: Send + Sync + 'static {
        async fn append_entries(
            &self,
            request: tonic::Request<super::AppendEntriesRequest>,
        ) -> std::result::Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status>;
        async fn request_vote(
            &self,
            request: tonic::Request<super::RequestVoteRequest>,
        ) -> std::result::Result<tonic::Response<super::RequestVoteResponse>, tonic::Status>;
        async fn install_snapshot(
            &self,
            request: tonic::Request<super::InstallSnapshotRequest>,
        ) -> std::result::Result<tonic::Response<super::InstallSnapshotResponse>, tonic::Status>;
    }

    #[derive(Debug)]
    pub struct RaftServiceServer<T: RaftService> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }

    impl<T: RaftService> RaftServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }

    impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftServiceServer<T>
    where
        T: RaftService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            let max_dec = self.max_decoding_message_size;
            let max_enc = self.max_encoding_message_size;
            match req.uri().path() {
                "/raft.RaftService/AppendEntries" => {
                    struct AppendEntriesSvc<T: RaftService>(pub Arc<T>);
                    impl<T: RaftService> tonic::server::UnaryService<super::AppendEntriesRequest> for AppendEntriesSvc<T> {
                        type Response = super::AppendEntriesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::AppendEntriesRequest>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).append_entries(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner;
                        let method = AppendEntriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(accept_compression_encodings, send_compression_encodings);
                        if let Some(limit) = max_dec { grpc = grpc.max_decoding_message_size(limit); }
                        if let Some(limit) = max_enc { grpc = grpc.max_encoding_message_size(limit); }
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/raft.RaftService/RequestVote" => {
                    struct RequestVoteSvc<T: RaftService>(pub Arc<T>);
                    impl<T: RaftService> tonic::server::UnaryService<super::RequestVoteRequest> for RequestVoteSvc<T> {
                        type Response = super::RequestVoteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::RequestVoteRequest>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).request_vote(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner;
                        let method = RequestVoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(accept_compression_encodings, send_compression_encodings);
                        if let Some(limit) = max_dec { grpc = grpc.max_decoding_message_size(limit); }
                        if let Some(limit) = max_enc { grpc = grpc.max_encoding_message_size(limit); }
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/raft.RaftService/InstallSnapshot" => {
                    struct InstallSnapshotSvc<T: RaftService>(pub Arc<T>);
                    impl<T: RaftService> tonic::server::UnaryService<super::InstallSnapshotRequest> for InstallSnapshotSvc<T> {
                        type Response = super::InstallSnapshotResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::InstallSnapshotRequest>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).install_snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner;
                        let method = InstallSnapshotSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(accept_compression_encodings, send_compression_encodings);
                        if let Some(limit) = max_dec { grpc = grpc.max_decoding_message_size(limit); }
                        if let Some(limit) = max_enc { grpc = grpc.max_encoding_message_size(limit); }
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }

    impl<T: RaftService> Clone for RaftServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }

    impl<T: RaftService> tonic::server::NamedService for RaftServiceServer<T> {
        const NAME: &'static str = "raft.RaftService";
    }
}
