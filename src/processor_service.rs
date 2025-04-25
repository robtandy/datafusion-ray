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
use std::error::Error;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::FlightClient;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{Stream, TryStreamExt};
use local_ip_address::local_ip;
use log::{debug, error, info, trace};
use prost::Message;
use tokio::net::TcpListener;

use tonic::transport::Server;
use tonic::{Request, Response, Status, async_trait};

use datafusion::error::Result as DFResult;

use arrow_flight::{Ticket, flight_service_server::FlightServiceServer};

use pyo3::prelude::*;

use parking_lot::{Mutex, RwLock};

use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::flight::{FlightHandler, FlightServ};
use crate::isolator::PartitionGroup;
use crate::protobuf::FlightTicketData;
use crate::stage_reader::QueryId;

use crate::util::{
    ResultExt, bytes_to_physical_plan, display_plan_with_partition_counts, get_client_map,
    input_stage_ids, register_object_store_for_paths_in_plan, reporting_stream, wait_for_future,
};

/// a map of stage_id, partition to a list FlightClients that can serve
/// this (stage_id, and partition).   It is assumed that to consume a partition, the consumer
/// will consume the partition from all clients and merge the results.
pub(crate) struct ServiceClients(pub HashMap<(usize, usize), Mutex<Vec<FlightClient>>>);

pub(crate) struct CtxName(pub String);

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct PlanKey {
    query_id: String,
    stage_id: usize,
    partition: usize,
}

/// It only responds to the DoGet Arrow Flight method.
struct DFRayProcessorHandler {
    /// our name, useful for logging
    name: String,
    /// our map of query_id -> (session ctx, execution plan)
    plans: RwLock<HashMap<PlanKey, Vec<(SessionContext, Arc<dyn ExecutionPlan>)>>>,
}

impl DFRayProcessorHandler {
    pub fn new(name: String) -> Self {
        let plans = RwLock::new(HashMap::new());

        Self { name, plans }
    }

    pub async fn add_plan(
        &self,
        query_id: String,
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        partition_group: Vec<usize>,
        full_partitions: bool,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DFResult<()> {
        let partitions = if full_partitions {
            partition_group.clone()
        } else {
            // we need to be able to respond to any of the partitions as
            // they will be scatter gathering from all Processors hosting this stage
            (0..(plan.output_partitioning().partition_count())).collect::<Vec<usize>>()
        };

        trace!(
            "{} adding plan for stage {} partitions: {:?}",
            self.name, stage_id, partitions
        );

        let ctx = self
            .configure_ctx(
                query_id.clone(),
                stage_id,
                stage_addrs,
                plan.clone(),
                partition_group,
            )
            .await?;

        for partition in partitions.iter() {
            let key = PlanKey {
                query_id: query_id.clone(),
                stage_id,
                partition: *partition,
            };
            {
                let mut _guard = self.plans.write();
                if let Some(plan_vec) = _guard.get_mut(&key) {
                    plan_vec.push((ctx.clone(), plan.clone()));
                } else {
                    _guard.insert(key.clone(), vec![(ctx.clone(), plan.clone())]);
                }
                trace!("{} added plan for plan key {:?}", self.name, key);
            }
        }

        debug!("{} plans held {:?}", self.name, self.plans.read().len());
        Ok(())
    }

    async fn configure_ctx(
        &self,
        query_id: String,
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        plan: Arc<dyn ExecutionPlan>,
        partition_group: Vec<usize>,
    ) -> DFResult<SessionContext> {
        let stage_ids_i_need = input_stage_ids(&plan)?;

        let client_map = get_client_map(stage_ids_i_need, stage_addrs).await?;

        let ctx_name = format!(
            "{} stage: {} partition_group: {:?}",
            self.name, stage_id, partition_group
        );

        let mut config = SessionConfig::new()
            .with_extension(Arc::new(ServiceClients(client_map)))
            .with_extension(Arc::new(QueryId(query_id)))
            .with_extension(Arc::new(CtxName(ctx_name)));

        // this only matters if the plan includes an PartitionIsolatorExec, which looks for this
        // for this extension and will be ignored otherwise
        config = config.with_extension(Arc::new(PartitionGroup(partition_group.clone())));

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        register_object_store_for_paths_in_plan(&ctx, plan.clone())?;

        trace!("ctx configured for stage {}", stage_id);

        Ok(ctx)
    }

    fn make_stream(
        &self,
        query_id: &str,
        stage_id: usize,
        partition: usize,
    ) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static, Status> {
        let key = PlanKey {
            query_id: query_id.to_string(),
            stage_id,
            partition,
        };

        let (ctx, plan) = {
            let mut _guard = self.plans.write();
            let (plan_key, mut plan_vec) = _guard.remove_entry(&key).ok_or_else(|| {
                Status::internal(format!(
                    "{}, No plan found for plan key {:?}",
                    self.name, key
                ))
            })?;
            trace!(
                "{} found {} plans for plan key {:?}",
                self.name,
                plan_vec.len(),
                plan_key
            );
            //trace!("{} removed plan for plan key {:?}", self.name, plan_key);
            //let (ctx, plan) = plan_vec.pop().expect("plan_vec should not be empty");
            let (ctx, plan) = plan_vec.pop().expect("plan_vec should not be empty");
            if !plan_vec.is_empty() {
                _guard.insert(plan_key, plan_vec);
            }
            (ctx, plan)
        };

        let task_ctx = ctx.task_ctx();

        let ctx_name = task_ctx
            .session_config()
            .get_extension::<CtxName>()
            .ok_or_else(|| {
                Status::internal(format!("{}, CtxName not set in session config", self.name))
            })?
            .0
            .clone();

        let stream = plan
            .execute(partition, task_ctx)
            .inspect_err(|e| {
                error!(
                    "{}",
                    format!("Could not get partition stream from plan {e}")
                )
            })
            .map(|s| reporting_stream(&format!("{ctx_name} s:{stage_id} p:{partition}"), s))
            .map_err(|e| Status::internal(format!("Could not get partition stream from plan {e}")))?
            .map_err(|e| FlightError::from_external_error(Box::new(e)));

        info!("{} plans held {}", self.name, self.plans.read().len());

        Ok(stream)
    }
}

#[async_trait]
impl FlightHandler for DFRayProcessorHandler {
    async fn get_stream(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<crate::flight::DoGetStream>, Status> {
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let ticket = request.into_inner();

        let ftd = FlightTicketData::decode(ticket.ticket).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error extracting ticket {e}",
                self.name
            ))
        })?;

        let plan_key = PlanKey {
            query_id: ftd.query_id.clone(),
            stage_id: ftd.stage_id as usize,
            partition: ftd.partition as usize,
        };

        trace!(
            "{}, request for plan_key:{:?} from: {}",
            self.name, plan_key, remote_addr
        );

        let name = self.name.clone();
        let stream = self
            .make_stream(&ftd.query_id, ftd.stage_id as usize, ftd.partition as usize)
            .map_err(|e| {
                Status::internal(format!("{} Unexpected error making stream {e}", name))
            })?;

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| {
                Status::internal(format!("{} Unexpected error building stream {e}", name))
            });

        Ok(Response::new(Box::pin(out_stream)))
    }
}

/// DFRayProcessorService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
#[pyclass]
pub struct DFRayProcessorService {
    name: String,
    listener: Option<TcpListener>,
    handler: Arc<DFRayProcessorHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

#[pymethods]
impl DFRayProcessorService {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        let name = format!("[{}]", name);
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let handler = Arc::new(DFRayProcessorHandler::new(name.clone()));

        Ok(Self {
            name,
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// bind the listener to a socket.  This method must complete
    /// before any other methods are called.   This is separate
    /// from new() because Ray does not let you wait (AFAICT) on Actor inits to complete
    /// and we will want to wait on this with ray.get()
    pub fn start_up(&mut self, py: Python) -> PyResult<()> {
        let my_local_ip = local_ip().to_py_err()?;
        let my_host_str = format!("{my_local_ip}:0");

        self.listener = Some(wait_for_future(py, async move {
            TcpListener::bind(&my_host_str).await
        })?);

        self.addr = Some(format!(
            "{}",
            self.listener.as_ref().unwrap().local_addr().unwrap()
        ));

        Ok(())
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> PyResult<String> {
        self.addr.clone().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "{},Couldn't get addr",
                self.name
            ))
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

    /// tell this service to host another plan
    ///
    /// returns a python coroutine that should be awaited
    pub fn add_plan<'a>(
        &self,
        py: Python<'a>,
        query_id: String,
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        partition_group: Vec<usize>,
        full_partitions: bool,
        plan_bytes: &[u8],
    ) -> PyResult<Bound<'a, PyAny>> {
        let plan = bytes_to_physical_plan(&SessionContext::new(), plan_bytes)?;

        trace!(
            "{} Received New Plan: Stage:{} my addr: {}, partition_group {:?}, full:{}, stage_addrs:\n{:?}\nplan:\n{}",
            self.name,
            stage_id,
            self.addr()?,
            partition_group,
            full_partitions,
            stage_addrs,
            display_plan_with_partition_counts(&plan)
        );

        let handler = self.handler.clone();
        let name = self.name.clone();
        let fut = async move {
            handler
                .add_plan(
                    query_id,
                    stage_id,
                    stage_addrs,
                    partition_group.clone(),
                    full_partitions,
                    plan,
                )
                .await
                .to_py_err()?;
            info!(
                "{} [stage: {} pg:{:?}] updated plan",
                name, stage_id, partition_group
            );
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

        let flight_serv = FlightServ {
            handler: self.handler.clone(),
        };

        let svc = FlightServiceServer::new(flight_serv);

        let listener = self.listener.take().unwrap();
        let name = self.name.clone();

        let serv = async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    signal,
                )
                .await
                .inspect_err(|e| error!("{}, ERROR serving {e}", name))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{e}")))?;
            trace!("serve async block done");
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };

        let fut = async move {
            serv.await.to_py_err()?;
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }
}
