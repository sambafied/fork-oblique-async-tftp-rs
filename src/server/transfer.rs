//! RRQ transfer seam for applications that own admission, sockets and tasks.
//!
//! No executor or worker is spawned here. Dropping `ReadTransfer` or its `run`
//! future closes its socket and drops its reader (including any custody pin).
//! Readers use futures IO; Tokio callers must use an explicit compatibility
//! adapter. The legacy `TftpServer::serve` API remains separate.
use super::{read_req::ReadRequest, ServerConfig};
use crate::{packet::Packet, Error, Result};
use async_io::Timer;
use futures_lite::{future, AsyncRead};
use std::{
    io,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

pub use crate::packet::{Mode, Opts as RequestedOptions, RwReq as Request};

/// Decode a bounded RRQ, exposing mode before the application opens content.
/// WRQ, malformed/duplicate recognized options and nondecimal values fail closed.
/// Unknown extension options are ignored as required by RFC 2347.
pub fn decode_rrq(bytes: &[u8]) -> Result<Request> {
    if bytes.len() > 512 || !bytes.ends_with(&[0]) {
        return Err(Error::InvalidPacket);
    }
    let fields: Vec<_> = bytes
        .get(2..)
        .ok_or(Error::InvalidPacket)?
        .split(|b| *b == 0)
        .collect();
    if fields.len() < 3 || fields[0].is_empty() || fields.len() % 2 != 1 {
        return Err(Error::InvalidPacket);
    }
    let mut seen = [false; 4];
    for pair in fields[2..fields.len() - 1].chunks_exact(2) {
        let key =
            std::str::from_utf8(pair[0]).map_err(|_| Error::InvalidPacket)?;
        let index = ["blksize", "timeout", "tsize", "windowsize"]
            .iter()
            .position(|name| key.eq_ignore_ascii_case(name));
        if key.is_empty() || pair[1].is_empty() {
            return Err(Error::InvalidPacket);
        }
        if let Some(i) = index {
            if seen[i] || !pair[1].iter().all(u8::is_ascii_digit) {
                return Err(Error::InvalidPacket);
            }
            seen[i] = true;
            let value = std::str::from_utf8(pair[1])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(Error::InvalidPacket)?;
            let valid = match i {
                0 => (8..=65464).contains(&value),
                1 => (1..=255).contains(&value),
                2 => value == 0,
                _ => (1..=65535).contains(&value),
            };
            if !valid {
                return Err(Error::InvalidPacket);
            }
        }
    }
    match Packet::decode(bytes)? {
        Packet::Rrq(request) => Ok(request),
        _ => Err(Error::InvalidPacket),
    }
}

/// Immutable settings captured at admission. Window negotiation is disabled.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    /// Omit *all* options up front, including timeout and tsize.
    pub omit_options: bool,
    /// Negotiated block-size ceiling (8..=1428); default DATA size remains 512.
    pub block_size_limit: u16,
    /// Default ACK timeout when the client did not request an exact RFC timeout.
    pub timeout: Duration,
    /// Retransmissions after the first send (0..=8).
    pub retries: u32,
    /// Absolute duration including reads, sends, OACK and ACK waits.
    pub deadline: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            omit_options: false,
            block_size_limit: 1428,
            timeout: Duration::from_secs(3),
            retries: 8,
            deadline: Duration::from_secs(300),
        }
    }
}

/// Owns exactly one supplied socket and one reader. No listening or file opens.
pub struct ReadTransfer<R: AsyncRead + Send + Unpin> {
    inner: ReadRequest<LengthReader<R>>,
    deadline: Duration,
}

impl<R: AsyncRead + Send + Unpin> ReadTransfer<R> {
    /// Validate mode, options and no-wrap length before any content read or IO.
    /// `socket` must already be bound to the application's chosen local address.
    pub fn new(
        socket: UdpSocket,
        peer: SocketAddr,
        mut request: Request,
        reader: R,
        length: u64,
        settings: Settings,
    ) -> Result<Self> {
        let invalid = || {
            Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid RRQ transfer settings",
            ))
        };
        if request.mode != Mode::Octet
            || !(8..=1428).contains(&settings.block_size_limit)
            || settings.timeout.is_zero()
            || settings.timeout > Duration::from_secs(255)
            || settings.retries > 8
            || settings.deadline.is_zero()
            || settings.deadline > Duration::from_secs(3600)
            || request
                .opts
                .block_size
                .is_some_and(|n| !(8..=65464).contains(&n))
            || request.opts.timeout == Some(0)
            || request.opts.window_size == Some(0)
            || request.opts.transfer_size.is_some_and(|n| n != 0)
            || socket.local_addr()?.is_ipv4() != peer.is_ipv4()
        {
            return Err(invalid());
        }
        // Never downgrade after an OACK timeout: lost ACK0 is ambiguous.
        if settings.omit_options {
            request.opts = RequestedOptions::default();
        }
        request.opts.window_size = None;
        let block = request
            .opts
            .block_size
            .map(|n| n.min(settings.block_size_limit))
            .unwrap_or(512);
        // Includes the mandatory zero terminal block for exact multiples.
        if length / u64::from(block) >= 65535 {
            return Err(invalid());
        }
        let config = ServerConfig {
            timeout: settings.timeout,
            block_size_limit: Some(settings.block_size_limit),
            window_size_limit: Some(1),
            max_send_retries: settings.retries,
            ignore_client_timeout: false,
            ignore_client_block_size: false,
            ignore_client_window_size: true,
        };
        let inner = ReadRequest::with_socket(
            LengthReader {
                reader,
                remaining: length,
            },
            Some(length),
            peer,
            &request,
            config,
            socket,
        )?;
        Ok(Self {
            inner,
            deadline: settings.deadline,
        })
    }

    /// Run until completion/error or the absolute deadline. Cancellation is drop.
    /// Errors are returned to the caller, which owns rate-limited ERROR responses.
    pub async fn run(mut self) -> Result<()> {
        future::race(self.inner.try_handle(), async {
            Timer::after(self.deadline).await;
            Err(Error::Io(io::ErrorKind::TimedOut.into()))
        })
        .await
    }
}

// Enforce the declared immutable length even for a faulty/custom reader, so a
// stream can neither silently truncate nor evade the preflight no-wrap guard.
struct LengthReader<R> {
    reader: R,
    remaining: u64,
}
impl<R: AsyncRead + Unpin> AsyncRead for LengthReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if out.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.remaining == 0 {
            let mut extra = [0];
            return match Pin::new(&mut this.reader).poll_read(cx, &mut extra) {
                Poll::Ready(Ok(0)) => Poll::Ready(Ok(0)),
                Poll::Ready(Ok(_)) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "reader exceeds declared length",
                ))),
                other => other,
            };
        }
        let max =
            out.len().min(this.remaining.try_into().unwrap_or(usize::MAX));
        match Pin::new(&mut this.reader).poll_read(cx, &mut out[..max]) {
            Poll::Ready(Ok(0)) => {
                Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()))
            }
            Poll::Ready(Ok(n)) => {
                this.remaining -= n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
}
