use beanstalkd_rs::{Connection, PutResult, ReserveResult};
use smol::block_on;

fn main() -> beanstalkd_rs::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;

        let result = conn.put(0, 0, 60, b"Hello, Beanstalkd!").await?;
        match result {
            PutResult::Inserted(id) => println!("Job inserted with ID: {}", id),
            PutResult::Buried(id) => println!("Job buried with ID: {}", id),
        }

        match conn.reserve().await? {
            ReserveResult::Reserved(job) => {
                println!("Got job {}: {:?}", job.id, job.body);
                conn.delete(job.id).await?;
            }
            ReserveResult::TimedOut => println!("No jobs available"),
            ReserveResult::DeadlineSoon => println!("Worker deadline approaching"),
        }
        Ok(())
    })
}
