// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::{ProstMessageExt, TicketStatementQuery};
use arrow_flight::{FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo};
use datafusion::common::internal_datafusion_err;
use datafusion::execution::{FunctionRegistry, SessionState, SessionStateBuilder};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use futures::{Stream, TryStreamExt};
use local_ip_address::local_ip;
use log::{debug, error, info, trace};
use prost::Message;
use pyo3::conversion::FromPyObjectBound;
use tokio::net::TcpListener;

use tonic::service::interceptor::InterceptorLayer;
use tonic::transport::Server;
use tonic::{Request, Response, Status, async_trait};

use datafusion::error::Result as DFResult;

use arrow_flight::{Ticket, flight_service_server::FlightServiceServer};

use pyo3::prelude::*;

use parking_lot::{Mutex, RwLock};

use tokio::sync::mpsc::{Receiver, Sender, channel};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::Span;

use crate::flight::{FlightHandler, FlightServ, FlightSqlHandler, FlightSqlServ};
use crate::isolator::PartitionGroup;
use crate::protobuf::{FlightTicketData, TicketStatementData};
use crate::stage_reader::DFRayStageReaderExec;
use crate::util::{
    ResultExt, bytes_to_physical_plan, display_plan_with_partition_counts, input_stage_ids,
    make_client, register_object_store_for_paths_in_plan, stream_from_stage, wait_for_future,
};

type Addrs = HashMap<usize, HashMap<usize, Vec<String>>>;

struct DFRayProxyHandler {
    py_inner: PyObject,
    pub queries: RwLock<HashMap<String, (Schema, Addrs, usize)>>,
}

impl DFRayProxyHandler {
    pub fn new(py_inner: PyObject) -> Self {
        let queries = RwLock::new(HashMap::new());
        Self { py_inner, queries }
    }
    pub fn store(&self, query_id: String, schema: Schema, addrs: Addrs, partitions: usize) {
        self.queries
            .write()
            .insert(query_id, (schema, addrs, partitions));
    }
}

#[async_trait]
impl FlightSqlHandler for DFRayProxyHandler {
    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let query_id = Python::with_gil(|py| {
            let bound = self.py_inner.bind(py);
            bound
                .call_method1("prepare_query", (query.query,))
                .and_then(|res| res.extract::<String>())
        })
        .map_err(|e| Status::internal(format!("Could not prepare query {e}")))?;

        let (schema, addrs, partition_count) =
            self.queries
                .read()
                .get(&query_id)
                .cloned()
                .ok_or(Status::internal(format!(
                    "Could not find schema for query_id {query_id}"
                )))?;

        debug!(
            "get flight info: query id {}, addrs {:?}, schema:{}",
            query_id, addrs, schema
        );

        // there should only be ones stage in this map
        assert!(addrs.len() == 1);

        let mut fi = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Could not create flight info {e}")))?;

        for (partition, partition_addrs) in addrs.get(&0).unwrap() {
            let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(
                TicketStatementQuery {
                    statement_handle: TicketStatementData {
                        query_id: query_id.clone(),
                        stage_id: 0,
                        partition: 0,
                    }
                    .encode_to_vec()
                    .into(),
                }
                .as_any()
                .encode_to_vec(),
            ));
            fi = fi.with_endpoint(endpoint);
        }

        Ok(Response::new(fi))
    }

    async fn do_get_statement(
        &self,
        ticket: arrow_flight::sql::TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        trace!("do_get_statement");
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let tsd = TicketStatementData::decode(ticket.statement_handle)
            .map_err(|e| Status::internal(format!("Cannot parse statement handle {e}")))?;

        let (schema, addrs, partition_count) = self
            .queries
            .read()
            .get(&tsd.query_id)
            .cloned()
            .ok_or(Status::internal(format!(
                "Could not find schema for query_id {}",
                tsd.query_id
            )))?;
        trace!("retrieved schema");

        let plan = Arc::new(
            DFRayStageReaderExec::try_new(
                Partitioning::UnknownPartitioning(partition_count),
                Arc::new(schema.clone()),
                tsd.stage_id as usize,
            )
            .map_err(|e| Status::internal(format!("Unexpected error {e}")))?,
        ) as Arc<dyn ExecutionPlan>;

        trace!(
            "request for partition {} from {}",
            tsd.partition, remote_addr
        );

        let stream = stream_from_stage(tsd.partition as usize, addrs, plan)
            .await
            .map_err(|e| {
                Status::internal(format!("Unexpected error building stream from stage {e}"))
            })?
            .map_err(|e| FlightError::ExternalError(Box::new(e)));

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| Status::internal(format!("Unexpected error building stream {e}")));

        Ok(Response::new(Box::pin(out_stream)))
    }
}

/// DFRayProcessorService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
#[pyclass]
pub struct DFRayProxyService {
    listener: Option<TcpListener>,
    handler: Arc<DFRayProxyHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
    port: usize,
}

#[pymethods]
impl DFRayProxyService {
    #[new]
    pub fn new(py_inner: PyObject, port: usize) -> PyResult<Self> {
        debug!("Creating DFRayProxyService!");
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let handler = Arc::new(DFRayProxyHandler::new(py_inner));

        Ok(Self {
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
            port,
        })
    }

    pub fn store(
        &mut self,
        py: Python,
        query_id: String,
        schema: PyObject,
        addrs: PyObject,
        partition_count: usize,
    ) -> PyResult<()> {
        Python::with_gil(|py| {
            trace!("trying to decode schema");
            let schema = Schema::from_pyarrow_bound(schema.bind(py))?;
            trace!("schema: {:?}", schema);
            let addrs = addrs.bind(py).extract::<Addrs>()?;

            self.handler.store(query_id, schema, addrs, partition_count);
            Ok(())
        })
    }

    /// bind the listener to a socket.  This method must complete
    /// before any other methods are called.   This is separate
    /// from new() because Ray does not let you wait (AFAICT) on Actor inits to complete
    /// and we will want to wait on this with ray.get()
    pub fn start_up(&mut self, py: Python) -> PyResult<()> {
        let my_local_ip = local_ip().to_py_err()?;
        let my_host_str = format!("{my_local_ip}:{}", self.port);

        let fut = async move { TcpListener::bind(&my_host_str).await };
        self.listener = Some(wait_for_future(py, fut).to_py_err()?);

        self.addr = Some(format!(
            "{}",
            self.listener.as_ref().unwrap().local_addr().unwrap()
        ));

        Ok(())
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> PyResult<String> {
        self.addr.clone().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!("Couldn't get addr",))
        })
    }

    /// signal to the service that we can shutdown
    ///
    /// returns a python coroutine that should be awaited
    pub fn all_done<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sender = self.all_done_tx.lock().clone();

        let fut = async move {
            sender.send(()).await.to_py_err()?;
            Ok(())
        };
        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    /// start the service
    /// returns a python coroutine that should be awaited
    pub fn serve<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let signal = async move {
            all_done_rx
                .recv()
                .await
                .expect("problem receiving shutdown signal");
            info!("received shutdown signal");
        };

        let service = FlightSqlServ {
            handler: self.handler.clone(),
        };

        fn intercept(req: Request<()>) -> Result<Request<()>, Status> {
            println!("Intercepting request: {:?}", req);
            debug!("Intercepting request: {:?}", req);
            Ok(req)
        }
        //let svc = FlightServiceServer::new(service);
        let svc = FlightServiceServer::with_interceptor(service, intercept);

        let listener = self.listener.take().unwrap();

        let serv = async move {
            let out = Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    signal,
                )
                .await
                .inspect_err(|e| error!("ERROR serving {e}"))
                .to_py_err();
            info!("serv async block complete");
            out
        };

        pyo3_async_runtimes::tokio::future_into_py(py, serv)
    }
}
