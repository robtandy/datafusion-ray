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

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use std::sync::Arc;

use crate::ray_stage::RayStageExec;

#[derive(Debug)]
pub struct RayShuffleOptimizerRule {}

impl Default for RayShuffleOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl RayShuffleOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for RayShuffleOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!(
            "optimizing physical plan:\n{}",
            displayable(plan.as_ref()).indent(false)
        );

        let mut stage_counter = 0;

        let up = |plan: Arc<dyn ExecutionPlan>| {
            //println!("examining plan: {}", displayable(plan.as_ref()).one_line());

            if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
                let stage = Arc::new(RayStageExec::new(plan, stage_counter.to_string()));
                stage_counter += 1;
                Ok(Transformed::yes(stage as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        let plan = plan.transform_up(up)?.data;
        let final_plan = Arc::new(RayStageExec::new(plan, stage_counter.to_string()));

        //println!(
        //    "optimized physical plan:\n{}",
        //    displayable(final_plan.as_ref()).indent(false)
        //);
        Ok(final_plan)
    }

    fn name(&self) -> &str {
        "RayShuffleOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
