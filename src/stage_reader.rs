use std::{fmt::Formatter, sync::Arc};

use arrow_flight::{FlightClient, Ticket};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::error::Result;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use futures::StreamExt;
use futures::stream::TryStreamExt;
use log::trace;
use prost::Message;

use crate::processor_service::ServiceClients;
use crate::protobuf::FlightTicketData;
use crate::util::CombinedRecordBatchStream;

pub(crate) struct QueryId(pub String);

/// An [`ExecutionPlan`] that will produce a stream of batches fetched from another stage
/// which is hosted by a [`crate::stage_service::StageService`] separated from a network boundary
///
/// Note that discovery of the service is handled by populating an instance of [`crate::stage_service::ServiceClients`]
/// and storing it as an extension in the [`datafusion::execution::TaskContext`] configuration.
#[derive(Debug)]
pub struct DFRayStageReaderExec {
    properties: PlanProperties,
    schema: SchemaRef,
    pub stage_id: usize,
}

impl DFRayStageReaderExec {
    pub fn try_new_from_input(input: Arc<dyn ExecutionPlan>, stage_id: usize) -> Result<Self> {
        let properties = input.properties().clone();

        Self::try_new(properties.partitioning.clone(), input.schema(), stage_id)
    }

    pub fn try_new(partitioning: Partitioning, schema: SchemaRef, stage_id: usize) -> Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitioning.partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            properties,
            schema,
            stage_id,
        })
    }
}
impl DisplayAs for DFRayStageReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayStageReaderExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for DFRayStageReaderExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn name(&self) -> &str {
        "RayStageReaderExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: handle more general case
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let name = format!("RayStageReaderExec[{}-{}]:", self.stage_id, partition);
        let client_map = &context
            .session_config()
            .get_extension::<ServiceClients>()
            .ok_or(internal_datafusion_err!(
                "{name} Flight Client not in context"
            ))?
            .clone()
            .0;

        let query_id = &context
            .session_config()
            .get_extension::<QueryId>()
            .ok_or(internal_datafusion_err!("{} QueryId not in context", name))?
            .0;

        let clients = client_map
            .get(&(self.stage_id, partition))
            .ok_or(internal_datafusion_err!(
                "{} No flight clients found for {}:{}, have {:?}",
                name,
                self.stage_id,
                partition,
                client_map.keys()
            ))?
            .lock()
            .iter()
            .map(|c| {
                let inner_clone = c.inner().clone();
                FlightClient::new_from_inner(inner_clone)
            })
            .collect::<Vec<_>>();

        trace!(
            "{name} execute: partition {partition} num clients: {}",
            clients.len()
        );

        let ftd = FlightTicketData {
            query_id: query_id.clone(),
            stage_id: self.stage_id as u64,
            partition: partition as u64,
        };

        let ticket = Ticket {
            ticket: ftd.encode_to_vec().into(),
        };

        let schema = self.schema.clone();

        let num_clients = clients.len();

        let stream = async_stream::stream! {
            let mut error = false;

            let mut streams = vec![];
            for (i, mut client) in clients.into_iter().enumerate() {
                let name = name.clone();
                trace!("{name} Getting flight stream {}/{}",i+1, num_clients);
                match client.do_get(ticket.clone()).await {
                    Ok(flight_stream) => {
                        trace!("{name} Got flight stream. headers:{:?}", flight_stream.headers());
                        let rbr_stream = RecordBatchStreamAdapter::new(schema.clone(),
                            flight_stream
                                .map_err(move |e| internal_datafusion_err!("{} Error consuming flight stream: {}", name, e)));

                        streams.push(Box::pin(rbr_stream) as SendableRecordBatchStream);
                    },
                    Err(e) => {
                        error = true;
                        yield internal_err!("{} Error getting flight stream: {}", name, e);
                    }
                }
            }
            if !error {
                let mut combined = CombinedRecordBatchStream::new(schema.clone(),streams);

                while let Some(maybe_batch) = combined.next().await {
                    yield maybe_batch;
                }
            }

        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
