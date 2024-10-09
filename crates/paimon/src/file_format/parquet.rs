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

use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::types::*;
use crate::error::*;
use crate::Result;
use snafu::ResultExt;
use std::fs::File;

pub struct ParquetReaderFactory {}

impl FormatReaderFactory for ParquetReaderFactory {
    fn create_reader(path: &str) -> Result<impl RecordReader> {
        // let file_io = FileIO::from_url(path)?.build()?;
        // let input = file_io.new_input(path)?;
        //TODO: use fileio
        let file = File::open(path).context(IoSnafu {})?;
        // TODO: more builder options
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).context(ParquetSnafu {})?;
        let reader = builder.build().context(ParquetSnafu {})?;
        Ok(ParquetReader { reader })
    }
}

pub struct ParquetReader {
    reader: ParquetRecordBatchReader,
}

impl RecordReader for ParquetReader {
    fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let record = self.reader.next().transpose().context(ArrowSnafu {})?;
        Ok(record.map(|batch| RecordBatch::new(batch)))
    }
}
