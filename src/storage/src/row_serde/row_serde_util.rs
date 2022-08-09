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

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use memcomparable::from_slice;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::ordered::{OrderedRowSerializer, SENTINEL_CELL_ID};
use risingwave_common::util::value_encoding::serialize_cell;

use super::ColumnDescMapping;

type KeyBytes = Vec<u8>;
type ValueBytes = Vec<u8>;

#[allow(clippy::len_without_is_empty)]
impl ColumnDescMapping {
    fn new_inner(
        output_columns: Vec<ColumnDesc>,
        all_data_types: Vec<DataType>,
        output_index: Vec<usize>,
    ) -> Arc<Self> {
        let output_id_to_index = output_columns
            .iter()
            .enumerate()
            .map(|(index, d)| (d.column_id, index))
            .collect();
        Self {
            output_columns,
            output_id_to_index,
            all_data_types,
            output_index,
        }
        .into()
    }

    /// Create a mapping with given `output_columns`.
    pub fn new(output_columns: Vec<ColumnDesc>) -> Arc<Self> {
        let all_data_types = output_columns.iter().map(|d| d.data_type.clone()).collect();
        let output_index: Vec<usize> = output_columns
            .iter()
            .map(|c| c.column_id.get_id() as usize)
            .collect();
        Self::new_inner(output_columns, all_data_types, output_index)
    }

    /// Create a mapping with given `table_columns` projected on the `column_ids`.
    pub fn new_partial(table_columns: &[ColumnDesc], output_column_ids: &[ColumnId]) -> Arc<Self> {
        let all_data_types = table_columns.iter().map(|d| d.data_type.clone()).collect();
        let mut table_columns = table_columns
            .iter()
            .enumerate()
            .map(|(index, c)| (c.column_id, (c.clone(), index)))
            .collect::<HashMap<_, _>>();
        let (output_columns, output_index): (
            Vec<risingwave_common::catalog::ColumnDesc>,
            Vec<usize>,
        ) = output_column_ids
            .iter()
            .map(|id| table_columns.remove(id).unwrap())
            .unzip();
        Self::new_inner(output_columns, all_data_types, output_index)
    }

    /// Get the [`ColumnDesc`] and its index in the output with given `id`.
    pub fn get(&self, id: ColumnId) -> Option<(&ColumnDesc, usize)> {
        self.output_id_to_index
            .get(&id)
            .map(|&index| (&self.output_columns[index], index))
    }

    /// Get the length of output columns.
    pub fn len(&self) -> usize {
        self.output_columns.len()
    }
}

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Vec<u8> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    result
}

/// Serialize a row of data using cell-based serialization, and return corresponding vector of key
/// and value. If all data of this row are null, there will be one cell of column id `-1` to
/// represent a row of all null values.
///
/// The returned value is an iterator of `column_ids.len() + 1` length, where the first n items are
/// serialization result of the `column_id` items correspondingly, and the last item is the sentinel
/// cell.
///
/// If a cell is null, the corresponding position of that cell will be `None`. For example,
///
/// * Serialize [0, 1, 2] => [Some((pk, 0)), Some((pk, 1)), Some((pk, 2)), Some((pk, []))]
/// * Serialize [null, null, null] => [None, None, None, Some((pk, []))]
pub fn serialize_pk_and_row(
    pk_buf: &[u8],
    row: &Row,
    column_ids: &[ColumnId],
) -> Result<Vec<Option<(KeyBytes, ValueBytes)>>> {
    let values = &row.0;
    assert_eq!(values.len(), column_ids.len());
    let mut result = Vec::with_capacity(column_ids.len() + 1);

    for (column_id, value) in column_ids.iter().zip_eq(values) {
        match value {
            None => {
                // This is when the datum is null. If all the datum in a row is null,
                // we serialize this null row specially by only using one cell encoding.
                result.push(None);
            }
            datum => {
                let key = serialize_pk_and_column_id(pk_buf, column_id)?;
                let value = serialize_cell(datum)?;
                result.push(Some((key, value)));
            }
        }
    }

    let key = serialize_pk_and_column_id(pk_buf, &SENTINEL_CELL_ID)?;
    result.push(Some((key, vec![])));

    Ok(result)
}

/// Serialize function directly used by states, and will be deprecated in the future.
pub fn serialize_pk_and_row_state(
    pk_buf: &[u8],
    row: &Option<Row>,
    column_ids: &[ColumnId],
) -> Result<Vec<(KeyBytes, Option<ValueBytes>)>> {
    if let Some(values) = row.as_ref() {
        let result = serialize_pk_and_row(pk_buf, values, column_ids)?
            .into_iter()
            .filter_map(|x| x.map(|(k, v)| (k, Some(v))))
            .collect();
        Ok(result)
    } else {
        let mut result = Vec::with_capacity(column_ids.len() + 1);
        for column_id in column_ids.iter() {
            let key = serialize_pk_and_column_id(pk_buf, column_id)?;
            // A `None` of row means deleting that row, while the a `None` of datum
            // represents a null.
            result.push((key, None));
        }
        let key = serialize_pk_and_column_id(pk_buf, &SENTINEL_CELL_ID)?;
        result.push((key, None));
        Ok(result)
    }
}

pub fn serialize_column_id(column_id: &ColumnId) -> [u8; 4] {
    let id = column_id.get_id();
    (id as u32 ^ (1 << 31)).to_be_bytes()
}

pub fn deserialize_column_id(bytes: &[u8]) -> Result<ColumnId> {
    let column_id = from_slice::<u32>(bytes)? ^ (1 << 31);
    Ok((column_id as i32).into())
}

pub fn serialize_pk_and_column_id(pk_buf: &[u8], col_id: &ColumnId) -> Result<Vec<u8>> {
    Ok([pk_buf, serialize_column_id(col_id).as_slice()].concat())
}

pub fn parse_raw_key_to_vnode_and_key(raw_key: &[u8]) -> (VirtualNode, &[u8]) {
    let (vnode_bytes, key_bytes) = raw_key.split_at(VIRTUAL_NODE_SIZE);
    let vnode = VirtualNode::from_be_bytes(vnode_bytes.try_into().unwrap());
    (vnode, key_bytes)
}