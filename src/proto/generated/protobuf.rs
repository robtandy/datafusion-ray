// This file is @generated by prost-build.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfRayStageReaderExecNode {
    /// schema of the stage we will consume
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<::datafusion_proto::protobuf::Schema>,
    /// properties of the stage we will consume
    #[prost(message, optional, tag = "2")]
    pub partitioning: ::core::option::Option<::datafusion_proto::protobuf::Partitioning>,
    /// stage to read from
    #[prost(uint64, tag = "3")]
    pub stage_id: u64,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MaxRowsExecNode {
    #[prost(uint64, tag = "1")]
    pub max_rows: u64,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PrefetchExecNode {
    #[prost(uint32, tag = "1")]
    pub dummy: u32,
    #[prost(uint64, tag = "2")]
    pub buf_size: u64,
}
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct PartitionIsolatorExecNode {
    #[prost(float, tag = "1")]
    pub dummy: f32,
    #[prost(uint64, tag = "2")]
    pub partition_count: u64,
}
/// TODO: why, if FlightTicketData has  the uint64 field first can it also be decoded also
/// MaxRowsExecNode?  There is something I don't understand here
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightTicketData {
    /// stage id of the stream
    /// partition id of the stream
    #[prost(string, tag = "1")]
    pub query_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub stage_id: u64,
    #[prost(uint64, tag = "3")]
    pub partition: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TicketStatementData {
    /// identity of the query we want to consume
    #[prost(string, tag = "1")]
    pub query_id: ::prost::alloc::string::String,
}
