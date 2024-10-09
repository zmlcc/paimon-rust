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

use crate::Result;
use arrow_array::cast::AsArray;

pub trait FormatReaderFactory {
    fn create_reader(path: &str) -> Result<impl RecordReader>;
}

pub trait RecordReader {
    fn read_batch(&mut self) -> Result<Option<RecordBatch>>;
}

pub struct RecordBatch {
    batch: arrow_array::RecordBatch,
}

impl RecordBatch {
    pub fn new(batch: arrow_array::RecordBatch) -> Self {
        Self { batch }
    }

    pub fn row_iter(&self) -> ColumnarRowIter {
        ColumnarRowIter {
            batch: &self.batch,
            pos: 0,
        }
    }
}

struct ColumnarRowIter<'a> {
    batch: &'a arrow_array::RecordBatch,
    pos: usize,
}

impl<'a> Iterator for ColumnarRowIter<'a> {
    type Item = ColumnarRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.batch.num_rows() {
            let v = ColumnarRow {
                record_batch: &self.batch,
                pos: self.pos,
            };
            self.pos += 1;
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct ColumnarRow<'a> {
    record_batch: &'a arrow_array::RecordBatch,
    pos: usize,
}

impl<'a> ColumnarRow<'a> {
    fn get_int(&self, i: usize) -> Result<i32> {
        let array = self
            .record_batch
            .column(i)
            .as_primitive_opt::<arrow::datatypes::Int32Type>()
            .unwrap();
        return Ok(array.value(self.pos));
    }

    fn get_int64(&self, i: usize) -> Result<i64> {
        let array = self
            .record_batch
            .column(i)
            .as_primitive_opt::<arrow::datatypes::Int64Type>()
            .unwrap();
        return Ok(array.value(self.pos));
    }
    fn get_string(&self, i: usize) -> Result<String> {
        return Ok("ok".into());
    }
}
