use std::{fmt::Debug, fs::File};
use  parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow;

fn main() {
    let file = File::open("/workspace/test/example.parquet").unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());
    
    let mut reader = builder.with_batch_size(3).build().unwrap();
    
    let record_batch = reader.next().unwrap().unwrap();
    
    println!("Read {} records.", record_batch.num_rows());

    let aaa = ColumnarRow{record_batch};
    let mut bbb = ColumnarRowIter{columar_row:&aaa, pos:0};
    bbb.next();
    let ccc = bbb.next();

    println!("WHAT {:?}", ccc);
    let ddd = ccc.unwrap();
    println!("WHAT {:?}", ddd.get_int64(0));

    // while let Some(r) = reader.next() {
    //     println!("Read {} records.", r.unwrap().num_rows());
    // }

    // let a= [1,2,3];
    
}


struct ColumnarRow {
    record_batch:  RecordBatch,
}

impl ColumnarRow {
    fn new(record_batch: RecordBatch) -> Self{
        return ColumnarRow {
            record_batch,
        }
    }

    fn num_rows(&self) -> usize {
        self.record_batch.num_rows()
    }

}

struct ColumnarRowIter<'a> {
    columar_row: &'a ColumnarRow,
    pos: usize,
}

impl<'a> Iterator for ColumnarRowIter<'a> {
    type Item = Box<dyn RowAccess + 'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.columar_row.num_rows() {
            let v =Box::new(Row{
                record_batch: &self.columar_row.record_batch,
                pos:self.pos
            });
            self.pos += 1;
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct Row<'a> {
    record_batch: &'a RecordBatch,
    pos: usize,
}

use anyhow::Result;
trait RowAccess: Debug {
    fn get_int(&self, i: usize) -> Result<i32>;
    fn get_int64(&self, i: usize) -> Result<i64>;
    fn get_string(&self, i: usize) -> Result<String>;
}

impl<'a> RowAccess for Row<'a>  {
    fn get_int(&self, i: usize) -> Result<i32> {
        let array = self.record_batch.column(i).as_primitive_opt::<arrow
        ::
        datatypes::Int32Type>().unwrap();
        return Ok(array.value(self.pos))
    }

    fn get_int64(&self, i: usize) -> Result<i64> {
        let array = self.record_batch.column(i).as_primitive_opt::<arrow
        ::
        datatypes::Int64Type>().unwrap();
        return Ok(array.value(self.pos))
    }
    fn get_string(&self, i: usize) -> Result<String>{
        return Ok("ok".into())
    }
}