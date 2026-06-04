use std::collections::HashMap;

use smol::io::{self, BufReader};
use smol::net::{TcpStream, unix::UnixStream};
use smol::prelude::*;

#[derive(Debug, PartialEq)]
pub struct Job {
    pub id: u64,
    pub body: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum IgnoreResult {
    Watching(u64),
    NotIgnored,
}

#[derive(Debug, PartialEq)]
pub enum ReleaseResult {
    Released,
    Buried,
    NotFound,
}

#[derive(Debug, PartialEq)]
pub enum ReserveResult {
    DeadlineSoon,
    TimedOut,
    Reserved(Job),
}

#[derive(Debug, PartialEq)]
pub enum PutResult {
    Inserted(u64),
    Buried(u64),
}
impl PutResult {
    pub fn id(&self) -> u64 {
        match self {
            Self::Inserted(id) => *id,
            Self::Buried(id) => *id,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Network I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("The job body must be followed by a CR-LF pair (\\r\\n)")]
    ExpectedCrlf,

    #[error("The job body exceeds max-job-size bytes allowed by the server")]
    JobTooBig,

    #[error("Server is out of memory")]
    OutOfMemory,

    #[error("Server encountered an internal error")]
    InternalError,

    #[error("Client sent a bad command format")]
    BadFormat,

    #[error("Client sent an unknown command")]
    UnknownCommand,

    #[error("Server is in drain mode and stops accepting puts")]
    Draining,

    #[error("Unexpected server response: {0}")]
    UnexpectedResponse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn check_global_error(line: &str) -> Option<Error> {
    match line {
        "OUT_OF_MEMORY\r\n" => Some(Error::OutOfMemory),
        "INTERNAL_ERROR\r\n" => Some(Error::InternalError),
        "BAD_FORMAT\r\n" => Some(Error::BadFormat),
        "UNKNOWN_COMMAND\r\n" => Some(Error::UnknownCommand),
        _ => None,
    }
}

fn build_quit() -> &'static [u8] {
    b"quit\r\n"
}

fn build_pause_tube(tube_name: &str, delay: u32) -> Vec<u8> {
    format!("pause-tube {tube_name} {delay}\r\n").into_bytes()
}

fn build_list_tubes_watched() -> &'static [u8] {
    b"list-tubes-watched\r\n"
}

fn build_list_tube_used() -> &'static [u8] {
    b"list-tube-used\r\n"
}

fn build_list_tubes() -> &'static [u8] {
    b"list-tubes\r\n"
}

fn build_stats() -> &'static [u8] {
    b"stats\r\n"
}

fn build_stats_tube(tube: &str) -> Vec<u8> {
    format!("stats-tube {tube}\r\n").into_bytes()
}

fn build_stats_job(id: u64) -> Vec<u8> {
    format!("stats-job {id}\r\n").into_bytes()
}

fn build_kick_job(id: u64) -> Vec<u8> {
    format!("kick-job {id}\r\n").into_bytes()
}

fn build_kick(bound: u32) -> Vec<u8> {
    format!("kick {bound}\r\n").into_bytes()
}

fn build_peek(id: u64) -> Vec<u8> {
    format!("peek {id}\r\n").into_bytes()
}

fn build_peek_ready() -> &'static [u8] {
    b"peek-ready\r\n"
}

fn build_peek_delayed() -> &'static [u8] {
    b"peek-delayed\r\n"
}

fn build_peek_buried() -> &'static [u8] {
    b"peek-buried\r\n"
}

fn build_ignore(tube: &str) -> Vec<u8> {
    format!("ignore {tube}\r\n").into_bytes()
}

fn build_watch(tube: &str) -> Vec<u8> {
    format!("watch {tube}\r\n").into_bytes()
}

fn build_touch(id: u64) -> Vec<u8> {
    format!("touch {id}\r\n").into_bytes()
}

fn build_bury(id: u64, pri: u32) -> Vec<u8> {
    format!("bury {id} {pri}\r\n").into_bytes()
}

fn build_release(id: u64, pri: u32, delay: u32) -> Vec<u8> {
    format!("release {id} {pri} {delay}\r\n").into_bytes()
}

fn build_delete(id: u64) -> Vec<u8> {
    format!("delete {id}\r\n").into_bytes()
}

fn build_reserve_job(id: u64) -> Vec<u8> {
    format!("reserve-job {id}\r\n").into_bytes()
}

fn build_reserve_with_timeout(seconds: u32) -> Vec<u8> {
    format!("reserve-with-timeout {seconds}\r\n").into_bytes()
}

fn build_reserve() -> &'static [u8] {
    b"reserve\r\n"
}

fn build_use_tube(tube: &str) -> Vec<u8> {
    format!("use {tube}\r\n").into_bytes()
}

fn build_put(pri: u32, delay: u32, ttr: u32, data: &[u8]) -> Vec<u8> {
    let mut w = format!("put {pri} {delay} {ttr} {}\r\n", data.len()).into_bytes();
    w.extend(data);
    w.extend(b"\r\n");
    w
}

async fn parse_pause_tube<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<bool> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    match buf.as_str() {
        "PAUSED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(Error::UnexpectedResponse(buf)),
    }
}

async fn parse_tubes<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Vec<String>> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("OK") {
        let bytes = buf.trim_end().strip_prefix("OK ").unwrap().parse().unwrap();
        let mut buf = vec![0; bytes + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(bytes);
        let buf = str::from_utf8(&buf).unwrap();
        return Ok(buf
            .lines()
            .skip(1)
            .map(|x| String::from(x.strip_prefix("- ").unwrap()))
            .collect());
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_tube<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<String> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("USING") {
        return Ok(buf.trim_end().strip_prefix("USING ").unwrap().to_string());
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_stats<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<HashMap<String, String>> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("OK") {
        let bytes = buf.trim_end().strip_prefix("OK ").unwrap().parse().unwrap();
        let mut buf = vec![0; bytes + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(bytes);
        let buf = str::from_utf8(&buf).unwrap();
        return Ok(buf
            .lines()
            .skip(1)
            .map(|x| x.split_once(": ").unwrap())
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect());
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_stats_tube_or_job<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<Option<HashMap<String, String>>> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf == "NOT_FOUND\r\n" {
        return Ok(None);
    }
    if buf.starts_with("OK") {
        let bytes = buf.trim_end().strip_prefix("OK ").unwrap().parse().unwrap();
        let mut buf = vec![0; bytes + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(bytes);
        let buf = str::from_utf8(&buf).unwrap();
        return Ok(Some(
            buf.lines()
                .skip(1)
                .map(|x| x.split_once(": ").unwrap())
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ));
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_kick_job<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<bool> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    match buf.as_str() {
        "KICKED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(Error::UnexpectedResponse(buf)),
    }
}

async fn parse_kick<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<u64> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("KICKED") {
        return Ok(buf
            .trim_ascii_end()
            .strip_prefix("KICKED ")
            .unwrap()
            .parse()
            .unwrap());
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_peek<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Option<Job>> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf == "NOT_FOUND\r\n" {
        return Ok(None);
    }
    if buf.starts_with("FOUND") {
        let mut split = buf
            .trim_ascii_end()
            .strip_prefix("FOUND ")
            .unwrap()
            .split(' ');
        let id = split.next().unwrap().parse().unwrap();
        let bytes = split.next().unwrap().parse().unwrap();
        let mut body = vec![0; bytes + 2];
        s.read_exact(&mut body).await?;
        body.truncate(bytes);
        return Ok(Some(Job { id, body }));
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_ignore<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<IgnoreResult> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf == "NOT_IGNORED\r\n" {
        return Ok(IgnoreResult::NotIgnored);
    }
    if buf.starts_with("WATCHING") {
        return Ok(IgnoreResult::Watching(
            buf.trim_ascii_end()
                .strip_prefix("WATCHING ")
                .unwrap()
                .parse()
                .unwrap(),
        ));
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_watch<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<u64> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("WATCHING") {
        return Ok(buf
            .trim_ascii_end()
            .strip_prefix("WATCHING ")
            .unwrap()
            .parse()
            .unwrap());
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_touch<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<bool> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    match buf.as_str() {
        "TOUCHED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(Error::UnexpectedResponse(buf)),
    }
}

async fn parse_bury<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<bool> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    match buf.as_str() {
        "BURIED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(Error::UnexpectedResponse(buf)),
    }
}

async fn parse_release<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<ReleaseResult> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    match buf.as_str() {
        "RELEASED\r\n" => Ok(ReleaseResult::Released),
        "BURIED\r\n" => Ok(ReleaseResult::Buried),
        "NOT_FOUND\r\n" => Ok(ReleaseResult::NotFound),
        _ => Err(Error::UnexpectedResponse(buf)),
    }
}

async fn parse_delete<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<bool> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    match buf.as_str() {
        "DELETED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(Error::UnexpectedResponse(buf)),
    }
}

async fn parse_reserve_job<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Option<Job>> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf == "NOT_FOUND\r\n" {
        return Ok(None);
    }
    if buf.starts_with("RESERVED") {
        let mut split = buf
            .trim_ascii_end()
            .strip_prefix("RESERVED ")
            .unwrap()
            .split(' ');
        let id = split.next().unwrap().parse().unwrap();
        let bytes = split.next().unwrap().parse().unwrap();
        let mut body = vec![0; bytes + 2];
        s.read_exact(&mut body).await?;
        body.truncate(bytes);
        return Ok(Some(Job { id, body }));
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_reserve<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<ReserveResult> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf == "TIMED_OUT\r\n" {
        return Ok(ReserveResult::TimedOut);
    }
    if buf == "DEADLINE_SOON\r\n" {
        return Ok(ReserveResult::DeadlineSoon);
    }
    if buf.starts_with("RESERVED") {
        let mut split = buf
            .trim_ascii_end()
            .strip_prefix("RESERVED ")
            .unwrap()
            .split(' ');
        let id = split.next().unwrap().parse().unwrap();
        let bytes = split.next().unwrap().parse().unwrap();
        let mut body = vec![0; bytes + 2];
        s.read_exact(&mut body).await?;
        body.truncate(bytes);
        return Ok(ReserveResult::Reserved(Job { id, body }));
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_use_tube<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<String> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("USING") {
        return Ok(buf
            .trim_ascii_end()
            .strip_prefix("USING ")
            .unwrap()
            .to_string());
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn parse_put<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<PutResult> {
    let mut buf = String::new();
    s.read_line(&mut buf).await?;
    if let Some(err) = check_global_error(&buf) {
        return Err(err);
    }
    if buf.starts_with("INSERTED") {
        return Ok(PutResult::Inserted(
            buf.trim_ascii_end()
                .strip_prefix("INSERTED ")
                .unwrap()
                .parse()
                .unwrap(),
        ));
    }
    if buf.starts_with("BURIED") {
        return Ok(PutResult::Buried(
            buf.trim_ascii_end()
                .strip_prefix("BURIED ")
                .unwrap()
                .parse()
                .unwrap(),
        ));
    }
    if buf == "EXPECTED_CRLF\r\n" {
        return Err(Error::ExpectedCrlf);
    }
    if buf == "JOB_TOO_BIG\r\n" {
        return Err(Error::JobTooBig);
    }
    if buf == "DRAINING\r\n" {
        return Err(Error::Draining);
    }
    Err(Error::UnexpectedResponse(buf))
}

async fn quit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(mut s: S) -> Result<()> {
    Ok(s.write_all(build_quit()).await?)
}

async fn pause_tube_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    tube_name: &str,
    delay: u32,
) -> Result<bool> {
    s.write_all(&build_pause_tube(tube_name, delay)).await?;
    parse_pause_tube(s).await
}

async fn list_tubes_watched_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<Vec<String>> {
    s.write_all(build_list_tubes_watched()).await?;
    parse_tubes(s).await
}

async fn list_tube_used_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<String> {
    s.write_all(build_list_tube_used()).await?;
    parse_tube(s).await
}

async fn list_tubes_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Vec<String>> {
    s.write_all(build_list_tubes()).await?;
    parse_tubes(s).await
}

async fn stats_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<HashMap<String, String>> {
    s.write_all(build_stats()).await?;
    parse_stats(s).await
}

async fn stats_tube_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    tube: &str,
) -> Result<Option<HashMap<String, String>>> {
    s.write_all(&build_stats_tube(tube)).await?;
    parse_stats_tube_or_job(s).await
}

async fn stats_job_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    id: u64,
) -> Result<Option<HashMap<String, String>>> {
    s.write_all(&build_stats_job(id)).await?;
    parse_stats_tube_or_job(s).await
}

async fn kick_job_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, id: u64) -> Result<bool> {
    s.write_all(&build_kick_job(id)).await?;
    parse_kick_job(s).await
}

async fn kick_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, bound: u32) -> Result<u64> {
    s.write_all(&build_kick(bound)).await?;
    parse_kick(s).await
}

async fn peek_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, id: u64) -> Result<Option<Job>> {
    s.write_all(&build_peek(id)).await?;
    parse_peek(s).await
}

async fn peek_ready_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Option<Job>> {
    s.write_all(build_peek_ready()).await?;
    parse_peek(s).await
}

async fn peek_delayed_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Option<Job>> {
    s.write_all(build_peek_delayed()).await?;
    parse_peek(s).await
}

async fn peek_buried_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Option<Job>> {
    s.write_all(build_peek_buried()).await?;
    parse_peek(s).await
}

async fn ignore_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    tube: &str,
) -> Result<IgnoreResult> {
    s.write_all(&build_ignore(tube)).await?;
    parse_ignore(s).await
}

async fn watch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, tube: &str) -> Result<u64> {
    s.write_all(&build_watch(tube)).await?;
    parse_watch(s).await
}

async fn touch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, id: u64) -> Result<bool> {
    s.write_all(&build_touch(id)).await?;
    parse_touch(s).await
}

async fn bury_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    id: u64,
    pri: u32,
) -> Result<bool> {
    s.write_all(&build_bury(id, pri)).await?;
    parse_bury(s).await
}

async fn release_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    id: u64,
    pri: u32,
    delay: u32,
) -> Result<ReleaseResult> {
    s.write_all(&build_release(id, pri, delay)).await?;
    parse_release(s).await
}

async fn delete_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, id: u64) -> Result<bool> {
    s.write_all(&build_delete(id)).await?;
    parse_delete(s).await
}

async fn reserve_job_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    id: u64,
) -> Result<Option<Job>> {
    s.write_all(&build_reserve_job(id)).await?;
    parse_reserve_job(s).await
}

async fn reserve_with_timeout_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    seconds: u32,
) -> Result<ReserveResult> {
    s.write_all(&build_reserve_with_timeout(seconds)).await?;
    parse_reserve(s).await
}

async fn reserve_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<ReserveResult> {
    s.write_all(build_reserve()).await?;
    parse_reserve(s).await
}

async fn use_tube_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    tube: &str,
) -> Result<String> {
    s.write_all(&build_use_tube(tube)).await?;
    parse_use_tube(s).await
}

async fn put_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    pri: u32,
    delay: u32,
    ttr: u32,
    data: &[u8],
) -> Result<PutResult> {
    s.write_all(&build_put(pri, delay, ttr, data)).await?;
    parse_put(s).await
}

pub enum Connection {
    Tcp(BufReader<TcpStream>),
    Unix(BufReader<UnixStream>),
}

impl Connection {
    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// let mut c = Connection::default().await?;
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn default() -> Result<Self> {
        Ok(Self::Tcp(BufReader::new(
            TcpStream::connect("127.0.0.1:11300").await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// let mut c = Connection::tcp_connect("127.0.0.1:11300").await?;
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn tcp_connect(addr: &str) -> Result<Self> {
        Ok(Self::Tcp(BufReader::new(TcpStream::connect(addr).await?)))
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// let mut c = Connection::unix_connect("/tmp/beanstalkd.sock").await?;
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn unix_connect(path: &str) -> Result<Self> {
        Ok(Self::Unix(BufReader::new(UnixStream::connect(path).await?)))
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.quit().await?
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn quit(self) -> Result<()> {
        match self {
            Self::Tcp(c) => quit_cmd(c).await,
            Self::Unix(c) => quit_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert!(c.pause_tube("default", 0).await?)
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn pause_tube(&mut self, tube_name: &str, delay: u32) -> Result<bool> {
        match self {
            Self::Tcp(c) => pause_tube_cmd(c, tube_name, delay).await,
            Self::Unix(c) => pause_tube_cmd(c, tube_name, delay).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.list_tubes_watched().await?, vec!["default"])
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn list_tubes_watched(&mut self) -> Result<Vec<String>> {
        match self {
            Self::Tcp(c) => list_tubes_watched_cmd(c).await,
            Self::Unix(c) => list_tubes_watched_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.list_tube_used().await?, "default")
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn list_tube_used(&mut self) -> Result<String> {
        match self {
            Self::Tcp(c) => list_tube_used_cmd(c).await,
            Self::Unix(c) => list_tube_used_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.list_tubes().await?, vec!["default"]);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn list_tubes(&mut self) -> Result<Vec<String>> {
        match self {
            Self::Tcp(c) => list_tubes_cmd(c).await,
            Self::Unix(c) => list_tubes_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.stats().await?.get("pid").unwrap(), "1");
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn stats(&mut self) -> Result<HashMap<String, String>> {
        match self {
            Self::Tcp(c) => stats_cmd(c).await,
            Self::Unix(c) => stats_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.stats_tube("default").await?.unwrap().get("name").unwrap(),
    ///         "\"default\""
    ///     );
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn stats_tube(&mut self, tube: &str) -> Result<Option<HashMap<String, String>>> {
        match self {
            Self::Tcp(c) => stats_tube_cmd(c, tube).await,
            Self::Unix(c) => stats_tube_cmd(c, tube).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     let r = c.put(0, 0, 0, b"a").await?;
    ///     assert_eq!(
    ///         c.stats_job(r.id()).await?.unwrap().get("tube").unwrap(),
    ///         "\"default\""
    ///     );
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn stats_job(&mut self, id: u64) -> Result<Option<HashMap<String, String>>> {
        match self {
            Self::Tcp(c) => stats_job_cmd(c, id).await,
            Self::Unix(c) => stats_job_cmd(c, id).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     let r = c.put(0, 0, 0, b"a").await?;
    ///     assert!(!c.kick_job(r.id()).await?);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn kick_job(&mut self, id: u64) -> Result<bool> {
        match self {
            Self::Tcp(c) => kick_job_cmd(c, id).await,
            Self::Unix(c) => kick_job_cmd(c, id).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     let r = c.put(0, 0, 0, b"a").await?;
    ///     assert_eq!(c.kick(10).await?, 0);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn kick(&mut self, bound: u32) -> Result<u64> {
        match self {
            Self::Tcp(c) => kick_cmd(c, bound).await,
            Self::Unix(c) => kick_cmd(c, bound).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error, Job};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     let r = c.put(0, 0, 0, b"a").await?;
    ///     assert_eq!(
    ///         c.peek(r.id()).await?,
    ///         Some(Job {
    ///             id: r.id(),
    ///             body: b"a".to_vec()
    ///         })
    ///     );
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn peek(&mut self, id: u64) -> Result<Option<Job>> {
        match self {
            Self::Tcp(c) => peek_cmd(c, id).await,
            Self::Unix(c) => peek_cmd(c, id).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.peek_ready().await?.unwrap().id;
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn peek_ready(&mut self) -> Result<Option<Job>> {
        match self {
            Self::Tcp(c) => peek_ready_cmd(c).await,
            Self::Unix(c) => peek_ready_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.peek_delayed().await?.is_none();
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn peek_delayed(&mut self) -> Result<Option<Job>> {
        match self {
            Self::Tcp(c) => peek_delayed_cmd(c).await,
            Self::Unix(c) => peek_delayed_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.peek_buried().await?.is_none();
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn peek_buried(&mut self) -> Result<Option<Job>> {
        match self {
            Self::Tcp(c) => peek_buried_cmd(c).await,
            Self::Unix(c) => peek_buried_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error, IgnoreResult};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.ignore("default").await?, IgnoreResult::NotIgnored);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn ignore(&mut self, tube: &str) -> Result<IgnoreResult> {
        match self {
            Self::Tcp(c) => ignore_cmd(c, tube).await,
            Self::Unix(c) => ignore_cmd(c, tube).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.watch("default").await?, 1);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn watch(&mut self, tube: &str) -> Result<u64> {
        match self {
            Self::Tcp(c) => watch_cmd(c, tube).await,
            Self::Unix(c) => watch_cmd(c, tube).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert!(!c.touch(1).await?);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn touch(&mut self, id: u64) -> Result<bool> {
        match self {
            Self::Tcp(c) => touch_cmd(c, id).await,
            Self::Unix(c) => touch_cmd(c, id).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert!(!c.bury(1, 0).await?);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn bury(&mut self, id: u64, pri: u32) -> Result<bool> {
        match self {
            Self::Tcp(c) => bury_cmd(c, id, pri).await,
            Self::Unix(c) => bury_cmd(c, id, pri).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error, ReleaseResult};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.release(1, 0, 0).await?, ReleaseResult::NotFound);
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn release(&mut self, id: u64, pri: u32, delay: u32) -> Result<ReleaseResult> {
        match self {
            Self::Tcp(c) => release_cmd(c, id, pri, delay).await,
            Self::Unix(c) => release_cmd(c, id, pri, delay).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     let r = c.put(0, 0, 0, b"a").await?;
    ///     assert!(c.delete(r.id()).await?)
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn delete(&mut self, id: u64) -> Result<bool> {
        match self {
            Self::Tcp(c) => delete_cmd(c, id).await,
            Self::Unix(c) => delete_cmd(c, id).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert!(c.reserve_job(1).await?.is_none())
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn reserve_job(&mut self, id: u64) -> Result<Option<Job>> {
        match self {
            Self::Tcp(c) => reserve_job_cmd(c, id).await,
            Self::Unix(c) => reserve_job_cmd(c, id).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error, ReserveResult};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.reserve_with_timeout(0).await?;
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn reserve_with_timeout(&mut self, seconds: u32) -> Result<ReserveResult> {
        match self {
            Self::Tcp(c) => reserve_with_timeout_cmd(c, seconds).await,
            Self::Unix(c) => reserve_with_timeout_cmd(c, seconds).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error, ReserveResult};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.reserve().await?;
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn reserve(&mut self) -> Result<ReserveResult> {
        match self {
            Self::Tcp(c) => reserve_cmd(c).await,
            Self::Unix(c) => reserve_cmd(c).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     assert_eq!(c.use_tube("default").await?, "default")
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn use_tube(&mut self, tube: &str) -> Result<String> {
        match self {
            Self::Tcp(c) => use_tube_cmd(c, tube).await,
            Self::Unix(c) => use_tube_cmd(c, tube).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use beanstalkd_rs::{Connection, Error, PutResult};
    /// # use smol::block_on;
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/beanstalkd.sock").await?,
    /// ] {
    ///     c.put(0, 0, 0, b"a").await?;
    /// }
    /// # Ok::<(), Error>(())
    /// # }).unwrap();
    /// ```
    pub async fn put(
        &mut self,
        pri: u32,
        delay: u32,
        ttr: u32,
        data: impl AsRef<[u8]>,
    ) -> Result<PutResult> {
        match self {
            Self::Tcp(c) => put_cmd(c, pri, delay, ttr, data.as_ref()).await,
            Self::Unix(c) => put_cmd(c, pri, delay, ttr, data.as_ref()).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol::block_on;
    use smol::io::Cursor;

    #[test]
    fn test_quit() {
        block_on(async {
            let c = Cursor::new(b"quit\r\n".to_vec());
            quit_cmd(c).await.unwrap();
        })
    }

    #[test]
    fn test_pause_tube() {
        block_on(async {
            let mut c = Cursor::new(b"pause-tube default 5\r\nPAUSED\r\n".to_vec());
            assert!(pause_tube_cmd(&mut c, "default", 5).await.unwrap());

            let mut c = Cursor::new(b"pause-tube default 5\r\nNOT_FOUND\r\n".to_vec());
            assert!(!pause_tube_cmd(&mut c, "default", 5).await.unwrap());

            let mut c = Cursor::new(b"pause-tube default 5\r\nERROR\r\n".to_vec());
            assert!(pause_tube_cmd(&mut c, "default", 5).await.is_err());

            let mut c = Cursor::new(b"pause-tube default 5\r\nBAD_FORMAT\r\n".to_vec());
            assert!(pause_tube_cmd(&mut c, "default", 5).await.is_err());
        })
    }

    #[test]
    fn test_list_tubes_watched() {
        block_on(async {
            let mut c =
                Cursor::new(b"list-tubes-watched\r\nOK 18\r\n---\n- default\n- a\n\r\n".to_vec());
            assert_eq!(
                list_tubes_watched_cmd(&mut c).await.unwrap(),
                vec!["default", "a"]
            );

            let mut c = Cursor::new(b"list-tubes-watched\r\nERROR\r\n".to_vec());
            assert!(list_tubes_watched_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"list-tubes-watched\r\nBAD_FORMAT\r\n".to_vec());
            assert!(list_tubes_watched_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_list_tube_used() {
        block_on(async {
            let mut c = Cursor::new(b"list-tube-used\r\nUSING default\r\n".to_vec());
            assert_eq!(list_tube_used_cmd(&mut c).await.unwrap(), "default");

            let mut c = Cursor::new(b"list-tube-used\r\nERROR\r\n".to_vec());
            assert!(list_tube_used_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"list-tube-used\r\nBAD_FORMAT\r\n".to_vec());
            assert!(list_tube_used_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_list_tubes() {
        block_on(async {
            let mut c = Cursor::new(b"list-tubes\r\nOK 18\r\n---\n- default\n- a\n\r\n".to_vec());
            assert_eq!(list_tubes_cmd(&mut c).await.unwrap(), vec!["default", "a"]);

            let mut c = Cursor::new(b"list-tubes\r\nERROR\r\n".to_vec());
            assert!(list_tubes_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"list-tubes\r\nBAD_FORMAT\r\n".to_vec());
            assert!(list_tubes_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_stats() {
        block_on(async {
            let mut c =
                Cursor::new(b"stats\r\nOK 27\r\n---\npid: 1\nversion: \"1.13\"\n\r\n".to_vec());
            assert_eq!(
                stats_cmd(&mut c).await.unwrap(),
                HashMap::from([
                    ("pid".to_string(), "1".to_string()),
                    ("version".to_string(), "\"1.13\"".to_string())
                ])
            );

            let mut c = Cursor::new(b"stats\r\nERROR\r\n".to_vec());
            assert!(stats_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"stats\r\nBAD_FORMAT\r\n".to_vec());
            assert!(stats_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_stats_tube() {
        block_on(async {
            let mut c = Cursor::new(
                b"stats-tube default\r\nOK 34\r\n---\nname: \"default\"\ntotal-jobs: 0\n\r\n"
                    .to_vec(),
            );
            assert_eq!(
                stats_tube_cmd(&mut c, "default").await.unwrap(),
                Some(HashMap::from([
                    ("name".to_string(), "\"default\"".to_string()),
                    ("total-jobs".to_string(), "0".to_string())
                ]))
            );

            let mut c = Cursor::new(b"stats-tube default\r\nERROR\r\n".to_vec());
            assert!(stats_tube_cmd(&mut c, "default").await.is_err());

            let mut c = Cursor::new(b"stats-tube default\r\nBAD_FORMAT\r\n".to_vec());
            assert!(stats_tube_cmd(&mut c, "default").await.is_err());
        })
    }

    #[test]
    fn test_stats_job() {
        block_on(async {
            let mut c = Cursor::new(
                b"stats-job 1\r\nOK 25\r\n---\nid: 1\ntube: \"default\"\n\r\n".to_vec(),
            );
            assert_eq!(
                stats_job_cmd(&mut c, 1).await.unwrap(),
                Some(HashMap::from([
                    ("id".to_string(), "1".to_string()),
                    ("tube".to_string(), "\"default\"".to_string())
                ]))
            );

            let mut c = Cursor::new(b"stats-job 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(stats_job_cmd(&mut c, 1).await.unwrap().is_none());

            let mut c = Cursor::new(b"stats-job 1\r\nERROR\r\n".to_vec());
            assert!(stats_job_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"stats-job 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(stats_job_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_kick_job() {
        block_on(async {
            let mut c = Cursor::new(b"kick-job 1\r\nKICKED\r\n".to_vec());
            assert!(kick_job_cmd(&mut c, 1).await.unwrap());

            let mut c = Cursor::new(b"kick-job 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(!kick_job_cmd(&mut c, 1).await.unwrap());

            let mut c = Cursor::new(b"kick-job 1\r\nERROR\r\n".to_vec());
            assert!(kick_job_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"kick-job 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(kick_job_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_kick() {
        block_on(async {
            let mut c = Cursor::new(b"kick 10\r\nKICKED 0\r\n".to_vec());
            assert_eq!(kick_cmd(&mut c, 10).await.unwrap(), 0);

            let mut c = Cursor::new(b"kick 10\r\nERROR\r\n".to_vec());
            assert!(kick_cmd(&mut c, 10).await.is_err());

            let mut c = Cursor::new(b"kick 10\r\nBAD_FORMAT\r\n".to_vec());
            assert!(kick_cmd(&mut c, 10).await.is_err());
        })
    }

    #[test]
    fn test_peek() {
        block_on(async {
            let mut c = Cursor::new(b"peek 1\r\nFOUND 1 1\r\na\r\n".to_vec());
            assert_eq!(
                peek_cmd(&mut c, 1).await.unwrap(),
                Some(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"peek 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(peek_cmd(&mut c, 1).await.unwrap().is_none());

            let mut c = Cursor::new(b"peek 1\r\nERROR\r\n".to_vec());
            assert!(peek_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"peek 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(peek_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_peek_ready() {
        block_on(async {
            let mut c = Cursor::new(b"peek-ready\r\nFOUND 1 1\r\na\r\n".to_vec());
            assert_eq!(
                peek_ready_cmd(&mut c).await.unwrap(),
                Some(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"peek-ready\r\nNOT_FOUND\r\n".to_vec());
            assert!(peek_ready_cmd(&mut c).await.unwrap().is_none());

            let mut c = Cursor::new(b"peek-ready\r\nERROR\r\n".to_vec());
            assert!(peek_ready_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"peek-ready\r\nBAD_FORMAT\r\n".to_vec());
            assert!(peek_ready_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_peek_delayed() {
        block_on(async {
            let mut c = Cursor::new(b"peek-delayed\r\nFOUND 1 1\r\na\r\n".to_vec());
            assert_eq!(
                peek_delayed_cmd(&mut c).await.unwrap(),
                Some(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"peek-delayed\r\nNOT_FOUND\r\n".to_vec());
            assert!(peek_delayed_cmd(&mut c).await.unwrap().is_none());

            let mut c = Cursor::new(b"peek-delayed\r\nERROR\r\n".to_vec());
            assert!(peek_delayed_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"peek-delayed\r\nBAD_FORMAT\r\n".to_vec());
            assert!(peek_delayed_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_peek_buried() {
        block_on(async {
            let mut c = Cursor::new(b"peek-buried\r\nFOUND 1 1\r\na\r\n".to_vec());
            assert_eq!(
                peek_buried_cmd(&mut c).await.unwrap(),
                Some(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"peek-buried\r\nNOT_FOUND\r\n".to_vec());
            assert!(peek_buried_cmd(&mut c).await.unwrap().is_none());

            let mut c = Cursor::new(b"peek-buried\r\nERROR\r\n".to_vec());
            assert!(peek_buried_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"peek-buried\r\nBAD_FORMAT\r\n".to_vec());
            assert!(peek_buried_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_ignore() {
        block_on(async {
            let mut c = Cursor::new(b"ignore default\r\nWATCHING 1\r\n".to_vec());
            assert_eq!(
                ignore_cmd(&mut c, "default").await.unwrap(),
                IgnoreResult::Watching(1)
            );

            let mut c = Cursor::new(b"ignore default\r\nNOT_IGNORED\r\n".to_vec());
            assert_eq!(
                ignore_cmd(&mut c, "default").await.unwrap(),
                IgnoreResult::NotIgnored
            );

            let mut c = Cursor::new(b"ignore default\r\nERROR\r\n".to_vec());
            assert!(ignore_cmd(&mut c, "default").await.is_err());

            let mut c = Cursor::new(b"ignore default\r\nBAD_FORMAT\r\n".to_vec());
            assert!(ignore_cmd(&mut c, "default").await.is_err());
        })
    }

    #[test]
    fn test_watch() {
        block_on(async {
            let mut c = Cursor::new(b"watch default\r\nWATCHING 1\r\n".to_vec());
            assert_eq!(watch_cmd(&mut c, "default").await.unwrap(), 1);

            let mut c = Cursor::new(b"watch default\r\nERROR\r\n".to_vec());
            assert!(watch_cmd(&mut c, "default").await.is_err());

            let mut c = Cursor::new(b"watch default\r\nBAD_FORMAT\r\n".to_vec());
            assert!(watch_cmd(&mut c, "default").await.is_err());
        })
    }

    #[test]
    fn test_touch() {
        block_on(async {
            let mut c = Cursor::new(b"touch 1\r\nTOUCHED\r\n".to_vec());
            assert!(touch_cmd(&mut c, 1).await.unwrap());

            let mut c = Cursor::new(b"touch 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(!touch_cmd(&mut c, 1).await.unwrap());

            let mut c = Cursor::new(b"touch 1\r\nERROR\r\n".to_vec());
            assert!(touch_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"touch 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(touch_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_bury() {
        block_on(async {
            let mut c = Cursor::new(b"bury 1 0\r\nBURIED\r\n".to_vec());
            assert!(bury_cmd(&mut c, 1, 0).await.unwrap());

            let mut c = Cursor::new(b"bury 1 0\r\nNOT_FOUND\r\n".to_vec());
            assert!(!bury_cmd(&mut c, 1, 0).await.unwrap());

            let mut c = Cursor::new(b"bury 1 0\r\nERROR\r\n".to_vec());
            assert!(bury_cmd(&mut c, 1, 0).await.is_err());

            let mut c = Cursor::new(b"bury 1 0\r\nBAD_FORMAT\r\n".to_vec());
            assert!(bury_cmd(&mut c, 1, 0).await.is_err());
        })
    }

    #[test]
    fn test_release() {
        block_on(async {
            let mut c = Cursor::new(b"release 1 0 0\r\nRELEASED\r\n".to_vec());
            assert_eq!(
                release_cmd(&mut c, 1, 0, 0).await.unwrap(),
                ReleaseResult::Released
            );

            let mut c = Cursor::new(b"release 1 0 0\r\nBURIED\r\n".to_vec());
            assert_eq!(
                release_cmd(&mut c, 1, 0, 0).await.unwrap(),
                ReleaseResult::Buried
            );

            let mut c = Cursor::new(b"release 1 0 0\r\nNOT_FOUND\r\n".to_vec());
            assert_eq!(
                release_cmd(&mut c, 1, 0, 0).await.unwrap(),
                ReleaseResult::NotFound
            );

            let mut c = Cursor::new(b"release 1 0 0\r\nERROR\r\n".to_vec());
            assert!(release_cmd(&mut c, 1, 0, 0).await.is_err());

            let mut c = Cursor::new(b"release 1 0 0\r\nBAD_FORMAT\r\n".to_vec());
            assert!(release_cmd(&mut c, 1, 0, 0).await.is_err());
        })
    }

    #[test]
    fn test_delete() {
        block_on(async {
            let mut c = Cursor::new(b"delete 1\r\nDELETED\r\n".to_vec());
            assert!(delete_cmd(&mut c, 1).await.unwrap());

            let mut c = Cursor::new(b"delete 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(!delete_cmd(&mut c, 1).await.unwrap());

            let mut c = Cursor::new(b"delete 1\r\nERROR\r\n".to_vec());
            assert!(delete_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"delete 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(delete_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_reserve_job() {
        block_on(async {
            let mut c = Cursor::new(b"reserve-job 1\r\nRESERVED 1 1\r\na\r\n".to_vec());
            assert_eq!(
                reserve_job_cmd(&mut c, 1).await.unwrap(),
                Some(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"reserve-job 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(reserve_job_cmd(&mut c, 1).await.unwrap().is_none());

            let mut c = Cursor::new(b"reserve-job 1\r\nERROR\r\n".to_vec());
            assert!(reserve_job_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"reserve-job 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(reserve_job_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_reserve_with_timeout() {
        block_on(async {
            let mut c = Cursor::new(b"reserve-with-timeout 1\r\nRESERVED 1 1\r\na\r\n".to_vec());
            assert_eq!(
                reserve_with_timeout_cmd(&mut c, 1).await.unwrap(),
                ReserveResult::Reserved(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"reserve-with-timeout 1\r\nDEADLINE_SOON\r\n".to_vec());
            assert_eq!(
                reserve_with_timeout_cmd(&mut c, 1).await.unwrap(),
                ReserveResult::DeadlineSoon
            );

            let mut c = Cursor::new(b"reserve-with-timeout 1\r\nTIMED_OUT\r\n".to_vec());
            assert_eq!(
                reserve_with_timeout_cmd(&mut c, 1).await.unwrap(),
                ReserveResult::TimedOut
            );

            let mut c = Cursor::new(b"reserve-with-timeout 1\r\nERROR\r\n".to_vec());
            assert!(reserve_with_timeout_cmd(&mut c, 1).await.is_err());

            let mut c = Cursor::new(b"reserve-with-timeout 1\r\nBAD_FORMAT\r\n".to_vec());
            assert!(reserve_with_timeout_cmd(&mut c, 1).await.is_err());
        })
    }

    #[test]
    fn test_reserve() {
        block_on(async {
            let mut c = Cursor::new(b"reserve\r\nRESERVED 1 1\r\na\r\n".to_vec());
            assert_eq!(
                reserve_cmd(&mut c).await.unwrap(),
                ReserveResult::Reserved(Job {
                    id: 1,
                    body: b"a".to_vec()
                })
            );

            let mut c = Cursor::new(b"reserve\r\nDEADLINE_SOON\r\n".to_vec());
            assert_eq!(
                reserve_cmd(&mut c).await.unwrap(),
                ReserveResult::DeadlineSoon
            );

            let mut c = Cursor::new(b"reserve\r\nTIMED_OUT\r\n".to_vec());
            assert_eq!(reserve_cmd(&mut c).await.unwrap(), ReserveResult::TimedOut);

            let mut c = Cursor::new(b"reserve\r\nERROR\r\n".to_vec());
            assert!(reserve_cmd(&mut c).await.is_err());

            let mut c = Cursor::new(b"reserve\r\nBAD_FORMAT\r\n".to_vec());
            assert!(reserve_cmd(&mut c).await.is_err());
        })
    }

    #[test]
    fn test_use_tube() {
        block_on(async {
            let mut c = Cursor::new(b"use default\r\nUSING default\r\n".to_vec());
            assert_eq!(use_tube_cmd(&mut c, "default").await.unwrap(), "default");

            let mut c = Cursor::new(b"use default\r\nERROR\r\n".to_vec());
            assert!(use_tube_cmd(&mut c, "default").await.is_err());

            let mut c = Cursor::new(b"use default\r\nBAD_FORMAT\r\n".to_vec());
            assert!(use_tube_cmd(&mut c, "default").await.is_err());
        })
    }

    #[test]
    fn test_put() {
        block_on(async {
            let mut c = Cursor::new(b"put 0 0 0 1\r\na\r\nINSERTED 1\r\n".to_vec());
            assert_eq!(
                put_cmd(&mut c, 1, 0, 0, b"a").await.unwrap(),
                PutResult::Inserted(1)
            );

            let mut c = Cursor::new(b"put 0 0 0 1\r\na\r\nBURIED 1\r\n".to_vec());
            assert_eq!(
                put_cmd(&mut c, 1, 0, 0, b"a").await.unwrap(),
                PutResult::Buried(1)
            );

            let mut c = Cursor::new(b"put 0 0 0 1\r\na\r\nEXPECTED_CRLF\r\n".to_vec());
            assert!(put_cmd(&mut c, 1, 0, 0, b"a").await.is_err());

            let mut c = Cursor::new(b"put 0 0 0 1\r\na\r\nJOB_TOO_BIG\r\n".to_vec());
            assert!(put_cmd(&mut c, 1, 0, 0, b"a").await.is_err());

            let mut c = Cursor::new(b"put 0 0 0 1\r\na\r\nDRAINING\r\n".to_vec());
            assert!(put_cmd(&mut c, 1, 0, 0, b"a").await.is_err());

            let mut c = Cursor::new(b"put 0 0 0 1\r\na\r\nERROR\r\n".to_vec());
            assert!(put_cmd(&mut c, 1, 0, 0, b"a").await.is_err());

            let mut c = Cursor::new(b"put 0 0 0\r\na\r\nBAD_FORMAT\r\n".to_vec());
            assert!(put_cmd(&mut c, 1, 0, 0, b"a").await.is_err());
        })
    }
}
