// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::types::DataType::Boolean;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{
    CollectInputRef, CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType,
    ExprVisitor, FunctionCall, InputRef,
};
use crate::optimizer::plan_node::{
    LogicalApply, LogicalFilter, LogicalJoin, LogicalProject, PlanTreeNode, PlanTreeNodeBinary,
};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Push `LogicalJoin` down `LogicalApply`
/// D Apply (T1 join<p> T2)  ->  (D Apply T1) join<p and natural join D> (D Apply T2)
pub struct ApplyJoinRule {}
impl Rule for ApplyJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (apply_left, apply_right, apply_on, apply_join_type, correlated_id, correlated_indices) =
            apply.clone().decompose();
        assert_eq!(apply_join_type, JoinType::Inner);
        let join: &LogicalJoin = apply_right.as_logical_join()?;

        // shortcut
        // Check whether correlated_input_ref with same correlated_id exists below apply.
        // If no, bail out and left for ApplyScan rule to deal with
        let mut collect_cor_input_ref = PlanCorrelatedIdFinder::new();
        collect_cor_input_ref.visit(apply_right.clone());
        if !collect_cor_input_ref
            .correlated_id_set
            .contains(&correlated_id)
        {
            return None;
        }

        // TODO: if the Apply are only required on one side, just push it to the corresponding side

        let apply_left_len = apply_left.schema().len();
        let join_left_len = join.left().schema().len();
        let mut rewriter = Rewriter {
            join_left_len,
            join_left_offset: apply_left_len as isize,
            join_right_offset: 2 * apply_left_len as isize,
            index_mapping: ColIndexMapping::new(
                correlated_indices
                    .clone()
                    .into_iter()
                    .map(Some)
                    .collect_vec(),
            )
            .inverse(),
            correlated_id,
        };

        // rewrite join on condition and add natural join condition
        let natural_conjunctions = apply_left
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Self::create_equal_expr(
                    i,
                    field.data_type.clone(),
                    i + join_left_len + apply_left_len,
                    field.data_type.clone(),
                )
            })
            .collect_vec();
        let new_join_condition = Condition {
            conjunctions: join
                .on()
                .clone()
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .chain(natural_conjunctions.into_iter())
                .collect_vec(),
        };

        let mut left_apply_condition: Vec<ExprImpl> = vec![];
        let mut right_apply_condition: Vec<ExprImpl> = vec![];
        let mut other_condition: Vec<ExprImpl> = vec![];

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti => {
                left_apply_condition.extend(apply_on);
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                right_apply_condition.extend(apply_on);
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let mut d_t1_bit_set =
                    FixedBitSet::with_capacity(apply_left_len + join.base.schema.len());
                let mut d_t2_bit_set =
                    FixedBitSet::with_capacity(apply_left_len + join.base.schema.len());
                d_t1_bit_set.set_range(0..apply_left_len + join_left_len, true);
                d_t2_bit_set.set_range(0..apply_left_len, true);
                d_t2_bit_set.set_range(
                    apply_left_len + join_left_len..apply_left_len + join.base.schema.len(),
                    true,
                );

                for (key, group) in &apply_on.into_iter().group_by(|expr| {
                    let mut visitor =
                        CollectInputRef::with_capacity(apply_left_len + join.base.schema.len());
                    visitor.visit_expr(expr);
                    let collect_bit_set = FixedBitSet::from(visitor);
                    if collect_bit_set.is_subset(&d_t1_bit_set) {
                        0
                    } else if collect_bit_set.is_subset(&d_t2_bit_set) {
                        1
                    } else {
                        2
                    }
                }) {
                    let vec = group.collect_vec();
                    match key {
                        0 => left_apply_condition.extend(vec),
                        1 => right_apply_condition.extend(vec),
                        2 => other_condition.extend(vec),
                        _ => unreachable!(),
                    }
                }

                // rewrite right condition
                let mut right_apply_condition_rewriter = Rewriter {
                    join_left_len: apply_left_len,
                    join_left_offset: 0,
                    join_right_offset: -(join_left_len as isize),
                    index_mapping: ColIndexMapping::empty(0),
                    correlated_id,
                };

                right_apply_condition = right_apply_condition
                    .into_iter()
                    .map(|expr| right_apply_condition_rewriter.rewrite_expr(expr))
                    .collect_vec();
            }
        }

        let new_join_left = LogicalApply::create(
            apply_left.clone(),
            join.left().clone(),
            apply_join_type,
            Condition {
                conjunctions: left_apply_condition,
            },
            correlated_id,
            correlated_indices.clone(),
        );
        let new_join_right = LogicalApply::create(
            apply_left.clone(),
            join.right().clone(),
            apply_join_type,
            Condition {
                conjunctions: right_apply_condition,
            },
            correlated_id,
            correlated_indices,
        );
        let new_join = LogicalJoin::new(
            new_join_left.clone(),
            new_join_right.clone(),
            join.join_type(),
            new_join_condition,
        );

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
                Some(new_join.into())
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                // project use to provide a natural join
                let mut project_exprs: Vec<ExprImpl> = vec![];
                project_exprs.extend(
                    new_join_left
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(i, field.data_type.clone())))
                        })
                        .collect_vec(),
                );
                project_exprs.extend(
                    new_join_right
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .skip(apply_left_len)
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(
                                i + new_join_left.schema().fields.len(),
                                field.data_type.clone(),
                            )))
                        })
                        .collect_vec(),
                );

                let new_project = LogicalProject::create(new_join.into(), project_exprs);

                if !other_condition.is_empty() {
                    // left other condition for predicate push down to deal with
                    let new_filter = LogicalFilter::create(
                        new_project,
                        Condition {
                            conjunctions: other_condition,
                        },
                    );
                    Some(new_filter)
                } else {
                    Some(new_project)
                }
            }
        }
    }
}

impl ApplyJoinRule {
    fn create_equal_expr(
        left: usize,
        left_data_type: DataType,
        right: usize,
        right_data_type: DataType,
    ) -> ExprImpl {
        ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
            ExprType::Equal,
            vec![
                ExprImpl::InputRef(Box::new(InputRef::new(left, left_data_type))),
                ExprImpl::InputRef(Box::new(InputRef::new(right, right_data_type))),
            ],
            Boolean,
        )))
    }
}

impl ApplyJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyJoinRule {})
    }
}

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
struct Rewriter {
    join_left_len: usize,
    join_left_offset: isize,
    join_right_offset: isize,
    index_mapping: ColIndexMapping,
    correlated_id: CorrelatedId,
}
impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        if correlated_input_ref.get_correlated_id() == self.correlated_id {
            InputRef::new(
                self.index_mapping.map(correlated_input_ref.index()),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index < self.join_left_len {
            InputRef::new(
                (input_ref.index() as isize + self.join_left_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        } else {
            InputRef::new(
                (input_ref.index() as isize + self.join_right_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        }
    }
}

pub struct PlanCorrelatedIdFinder {
    pub correlated_id_set: HashSet<CorrelatedId>,
}

impl PlanCorrelatedIdFinder {
    pub fn new() -> Self {
        Self {
            correlated_id_set: HashSet::new(),
        }
    }
}

impl PlanVisitor<()> for PlanCorrelatedIdFinder {
    /// common subquery is project, filter and join

    fn visit_logical_join(&mut self, plan: &LogicalJoin) {
        let mut finder = ExprCorrelatedIdFinder::new();
        plan.on()
            .conjunctions
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        let mut iter = plan.inputs().into_iter();
        self.visit(iter.next().unwrap());
        iter.for_each(|input| {
            self.visit(input);
        });
    }

    fn visit_logical_filter(&mut self, plan: &LogicalFilter) {
        let mut finder = ExprCorrelatedIdFinder::new();
        plan.predicate()
            .conjunctions
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        let mut iter = plan.inputs().into_iter();
        self.visit(iter.next().unwrap());
        iter.for_each(|input| {
            self.visit(input);
        });
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) {
        let mut finder = ExprCorrelatedIdFinder::new();
        plan.exprs().iter().for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        let mut iter = plan.inputs().into_iter();
        self.visit(iter.next().unwrap());
        iter.for_each(|input| {
            self.visit(input);
        });
    }
}

struct ExprCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl ExprCorrelatedIdFinder {
    pub fn new() -> Self {
        Self {
            correlated_id_set: HashSet::new(),
        }
    }
}

impl ExprVisitor for ExprCorrelatedIdFinder {
    fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
        self.correlated_id_set
            .insert(correlated_input_ref.get_correlated_id());
    }
}