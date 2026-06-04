# Minimal Beanstalkd client on Rust
[![CI](https://github.com/ArtemIsmagilov/beanstalkd-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/beanstalkd-rs/actions/workflows/ci.yaml)
[![Crates.io](https://img.shields.io/crates/v/beanstalkd-rs.svg)](https://crates.io/crates/beanstalkd-rs)
[![Docs.rs](https://docs.rs/beanstalkd-rs/badge.svg)](https://docs.rs/beanstalkd-rs)
[![MIT license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
## Beanstalkd

Beanstalkd is a simple, fast work queue on C. 

## Usage

### Producer

```rust
use beanstalkd_rs::{Connection, PutResult};
use smol::block_on;

fn main() -> beanstalkd_rs::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        let result = conn.put(0, 0, 60, b"Hello, Beanstalkd!").await?;
        match result {
            PutResult::Inserted(id) => println!("Job inserted with ID: {}", id),
            PutResult::Buried(id) => println!("Job buried with ID: {}", id),
        }
        Ok(())
    })
}
```

### Consumer

```rust
use beanstalkd_rs::{Connection, ReserveResult};
use smol::block_on;

fn main() -> beanstalkd_rs::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
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
```

## Features

- All commands are implemented
- Typed results
- Asynchronous interface
- Tcp/Unix sockets
- Errors Beanstalkd mapping with messages
- Has documentation
- Coverage Integration and unit tests

## Supported commands

| Category | Commands |
|----------|----------|
| Producing | `put` |
| Consuming | `reserve`, `reserve_with_timeout`, `reserve_job` |
| Job management | `delete`, `release`, `bury`, `touch`, `kick`, `kick_job` |
| Inspection | `peek`, `peek_ready`, `peek_delayed`, `peek_buried` |
| Tube management | `use_tube`, `watch`, `ignore`, `pause_tube` |
| Information | `list_tubes`, `list_tube_used`, `list_tubes_watched` |
| Statistics | `stats`, `stats_tube`, `stats_job` |
| Connection | `quit` |

## Testing

```bash
docker compose up
bash chmod_unix.bash
cargo test
```

## Mutation testing

```bash
cargo mutants
```

## Coverage testing

```bash
cargo llvm-cov
```

## Quality code

```bash
debtmap analyze .
```

## Security scanning

```bash
opengrep scan --config auto
```

## Formatting doc tests

```
cargo fmt -- --config format_code_in_doc_comments=true
```

## Links

- [Beanstalkd](https://beanstalkd.github.io/)
