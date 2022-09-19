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

use std::sync::Arc;

use risingwave_common::array::Row;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::ScalarImpl;
use risingwave_connector::source::{SplitId, SplitMetaData};
use risingwave_pb::catalog::Table;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

pub struct SourceStateTableHandler<S: StateStore> {
    state_store: StateTable<S>,
}

impl<S: StateStore> SourceStateTableHandler<S> {
    pub fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        Self {
            state_store: StateTable::from_table_catalog(table_catalog, store, vnodes),
        }
    }

    fn string_to_scalar(rhs: impl ToString) -> ScalarImpl {
        ScalarImpl::Utf8(rhs.to_string())
    }

    async fn get(&mut self, key: SplitId, epoch: u64) -> StreamExecutorResult<Option<Row>> {
        self.state_store
            .get_row(&Row::new(vec![Some(Self::string_to_scalar(key))]), epoch)
            .await
            .map_err(StreamExecutorError::from)
    }

    async fn set(&mut self, key: SplitId, value: String, epoch: u64) -> StreamExecutorResult<()> {
        let row = Row::new(vec![
            Some(Self::string_to_scalar(key)),
            Self::string_to_scalar(value),
        ]);
        match self.get(key.clone(), epoch).await? {
            Some(prev_row) => {
                self.state_store.update(prev_row, row);
            }
            None => {
                self.state_store.insert(row);
            }
        }
        Ok(())
    }

    pub async fn take_snapshot<SS>(
        &mut self,
        states: Vec<SS>,
        epoch: u64,
    ) -> StreamExecutorResult<()>
    where
        SS: SplitMetaData,
    {
        if states.is_empty() {
            // TODO should be a clear Error Code
            bail!("states require not null");
        } else {
            for split_impl in states {
                self.set(split_impl.id(), split_impl.encode_to_string(), epoch)
                    .await?;
            }
            self.state_store.commit(epoch).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{DatabaseId, SchemaId};
    use risingwave_common::types::{Datum, ScalarImpl};
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::plan_common::{ColumnCatalog, ColumnDesc, ColumnOrder};
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    // align with schema defined in `LogicalSource::infer_internal_table_catalog`
    fn source_internal_table(id: u32) -> ProstTable {
        let make_column = |column_type: TypeName, column_id: i32| -> ColumnCatalog {
            ColumnCatalog {
                column_desc: Some(ColumnDesc {
                    column_type: Some(DataType {
                        type_name: column_type as i32,
                        ..Default::default()
                    }),
                    column_id,
                    ..Default::default()
                }),
                is_hidden: false,
            }
        };

        let columns = vec![
            make_column(TypeName::Varchar, 0),
            make_column(TypeName::Varchar, 1),
        ];
        ProstTable {
            id,
            schema_id: SchemaId::placeholder() as u32,
            database_id: DatabaseId::placeholder() as u32,
            name: String::new(),
            columns,
            is_index: false,
            index_on_id: 0,
            value_indices: vec![0, 1],
            order_key: vec![ColumnOrder {
                index: 0,
                order_type: 1,
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_from_table_catalog() {
        let store = MemoryStateStore::new();
        let mut state_table =
            StateTable::from_table_catalog(&source_internal_table(1), store, None);
        let a: Arc<str> = String::from("a").into();
        let a: Datum = Some(ScalarImpl::Utf8(a.as_ref().into()));
        let b: Arc<str> = String::from("b").into();
        let b: Datum = Some(ScalarImpl::Utf8(b.as_ref().into()));

        state_table.insert(Row::new(vec![a.clone(), b.clone()]));
        state_table.commit(100100).await.unwrap();

        // let c: Arc<str> = String::from("c").into();
        // let c: Datum = Some(ScalarImpl::Utf8(c.as_ref().into()));

        // state_table.insert(Row::new(vec![a, c]));
        // state_table.commit(100101).await.unwrap();

        let a: Arc<str> = String::from("a").into();
        let a: Datum = Some(ScalarImpl::Utf8(a.as_ref().into()));
        let resp = state_table
            .get_row(&Row::new(vec![a]), 100102)
            .await
            .unwrap();
        println!("{:?}", resp.unwrap().0[1].unwrap().get_ident());
    }
}
