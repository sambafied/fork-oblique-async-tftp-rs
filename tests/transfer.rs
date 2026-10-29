use async_tftp::server::transfer::{decode_rrq, Mode, ReadTransfer, Settings};
use futures_lite::{future, io::Cursor, AsyncRead};
use std::{
    io,
    net::UdpSocket,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

fn socket() -> UdpSocket {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s
}
fn req(options: &[u8]) -> Vec<u8> {
    let mut r = b"\0\x01boot\0octet\0".to_vec();
    r.extend_from_slice(options);
    r
}
fn ack(s: &UdpSocket, peer: std::net::SocketAddr, n: u16) {
    let [a, b] = n.to_be_bytes();
    s.send_to(&[0, 4, a, b], peer).unwrap();
}
fn recv(s: &UdpSocket) -> (Vec<u8>, std::net::SocketAddr) {
    let mut b = [0; 2048];
    let (n, p) = s.recv_from(&mut b).unwrap();
    (b[..n].to_vec(), p)
}
fn settings() -> Settings {
    Settings {
        timeout: Duration::from_millis(30),
        retries: 1,
        deadline: Duration::from_secs(1),
        ..Settings::default()
    }
}

#[test]
fn metadata_and_strict_admission() {
    assert_eq!(
        decode_rrq(&req(
            b"blksize\01428\0timeout\03\0tsize\00\0windowsize\04\0"
        ))
        .unwrap()
        .opts
        .block_size,
        Some(1428)
    );
    assert_eq!(
        decode_rrq(b"\0\x01boot\0netascii\0").unwrap().mode,
        Mode::Netascii
    );
    for r in [
        b"\0\x02boot\0octet\0".to_vec(),
        req(b"blksize\0512\0BLKSIZE\01024\0"),
        req(b"timeout\00\0"),
        req(b"blksize\0+512\0"),
        req(b"tsize\01\0"),
        req(b"tsize\018446744073709551616\0"),
        req(b"x\0"),
        vec![0; 513],
    ] {
        assert!(decode_rrq(&r).is_err(), "accepted {r:?}");
    }
}

#[test]
fn conservative_omits_every_option_and_exact_multiple_has_terminal_zero() {
    let server = socket();
    let client = socket();
    let addr = server.local_addr().unwrap();
    let transfer = ReadTransfer::new(
        server,
        client.local_addr().unwrap(),
        decode_rrq(&req(
            b"blksize\01024\0timeout\01\0tsize\00\0windowsize\08\0",
        ))
        .unwrap(),
        Cursor::new(vec![42; 512]),
        512,
        Settings {
            omit_options: true,
            ..settings()
        },
    )
    .unwrap();
    let worker = std::thread::spawn(move || future::block_on(transfer.run()));
    let (data, peer) = recv(&client);
    assert_eq!(peer, addr);
    assert_eq!(&data[..4], &[0, 3, 0, 1]);
    assert_eq!(data.len(), 516);
    // A different peer cannot acknowledge the real transfer.
    ack(&socket(), peer, 1);
    // Wrong-peer ACK must leave DATA1 unacknowledged (next packet is a retry).
    assert_eq!(recv(&client).0, data);
    ack(&client, peer, 1);
    let (end, _) = recv(&client);
    assert_eq!(end, [0, 3, 0, 2]);
    ack(&client, peer, 2);
    worker.join().unwrap().unwrap();
}

#[test]
fn negotiated_oack_echo_and_ack_zero_gate() {
    let server = socket();
    let client = socket();
    let transfer = ReadTransfer::new(
        server,
        client.local_addr().unwrap(),
        decode_rrq(&req(
            b"blksize\02048\0timeout\01\0tsize\00\0windowsize\08\0",
        ))
        .unwrap(),
        Cursor::new(vec![7; 1428]),
        1428,
        settings(),
    )
    .unwrap();
    let worker = std::thread::spawn(move || future::block_on(transfer.run()));
    let (oack, peer) = recv(&client);
    assert_eq!(oack, b"\0\x06blksize\01428\0timeout\01\0tsize\01428\0");
    ack(&client, peer, 0);
    let (data, _) = recv(&client);
    assert_eq!(data.len(), 1432);
    assert_eq!(&data[..4], &[0, 3, 0, 1]);
    ack(&client, peer, 1);
    assert_eq!(recv(&client).0, [0, 3, 0, 2]);
    ack(&client, peer, 2);
    worker.join().unwrap().unwrap();
}

#[test]
fn oack_timeout_never_falls_back_and_retry_count_is_bounded() {
    let server = socket();
    let client = socket();
    let transfer = ReadTransfer::new(
        server,
        client.local_addr().unwrap(),
        decode_rrq(&req(b"blksize\01024\0")).unwrap(),
        Cursor::new(vec![0; 10]),
        10,
        settings(),
    )
    .unwrap();
    let worker = std::thread::spawn(move || future::block_on(transfer.run()));
    let first = recv(&client).0;
    assert_eq!(&first[..2], &[0, 6]);
    assert_eq!(recv(&client).0, first);
    assert!(worker.join().unwrap().is_err());
    client.set_read_timeout(Some(Duration::from_millis(80))).unwrap();
    let mut b = [0; 2048];
    assert!(client.recv_from(&mut b).is_err());
}

struct PinReader {
    dropped: Arc<AtomicUsize>,
    pending: bool,
}
impl Drop for PinReader {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl AsyncRead for PinReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending {
            Poll::Pending
        } else {
            out[0] = 1;
            Poll::Ready(Ok(1))
        }
    }
}
#[test]
fn cancellation_releases_owned_pins_and_sockets_without_hidden_tasks() {
    future::block_on(async {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        let mut addresses = Vec::new();
        // The application owns the exact bounded set of transfer futures.
        for _ in 0..4 {
            let s = socket();
            addresses.push(s.local_addr().unwrap());
            let r = PinReader {
                dropped: dropped.clone(),
                pending: true,
            };
            let t = ReadTransfer::new(
                s,
                "127.0.0.1:44444".parse().unwrap(),
                decode_rrq(&req(b"")).unwrap(),
                r,
                1,
                settings(),
            )
            .unwrap();
            workers.push(Box::pin(t.run()));
        }
        for worker in &mut workers {
            assert!(future::poll_once(worker.as_mut()).await.is_none());
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(workers);
        assert_eq!(dropped.load(Ordering::SeqCst), 4);
        for addr in addresses {
            UdpSocket::bind(addr).unwrap();
        }
    });
}
#[test]
fn absolute_deadline_includes_a_stalled_reader() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let t = ReadTransfer::new(
        socket(),
        "127.0.0.1:44444".parse().unwrap(),
        decode_rrq(&req(b"")).unwrap(),
        PinReader {
            dropped: dropped.clone(),
            pending: true,
        },
        1,
        Settings {
            deadline: Duration::from_millis(20),
            ..settings()
        },
    )
    .unwrap();
    assert!(future::block_on(t.run()).is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
#[test]
fn rejects_mode_wrap_and_incorrect_reader_length() {
    for (r, length) in [
        (decode_rrq(b"\0\x01boot\0netascii\0").unwrap(), 0),
        (decode_rrq(&req(b"")).unwrap(), 65535 * 512),
    ] {
        assert!(ReadTransfer::new(
            socket(),
            "127.0.0.1:44444".parse().unwrap(),
            r,
            Cursor::new(vec![]),
            length,
            settings()
        )
        .is_err());
    }
    // Last valid no-wrap length is accepted, including its terminal packet.
    assert!(ReadTransfer::new(
        socket(),
        "127.0.0.1:44444".parse().unwrap(),
        decode_rrq(&req(b"")).unwrap(),
        Cursor::new(vec![]),
        65535 * 512 - 1,
        settings()
    )
    .is_ok());
    for (data, length) in [(vec![1], 2), (vec![1, 2], 1)] {
        let t = ReadTransfer::new(
            socket(),
            "127.0.0.1:44444".parse().unwrap(),
            decode_rrq(&req(b"")).unwrap(),
            Cursor::new(data),
            length,
            settings(),
        )
        .unwrap();
        assert!(future::block_on(t.run()).is_err());
    }
}

#[test]
fn cancel_while_waiting_for_ack_closes_socket() {
    future::block_on(async {
        let server = socket();
        let addr = server.local_addr().unwrap();
        let client = socket();
        let transfer = ReadTransfer::new(
            server,
            client.local_addr().unwrap(),
            decode_rrq(&req(b"")).unwrap(),
            Cursor::new(vec![5]),
            1,
            settings(),
        )
        .unwrap();
        let mut worker = Box::pin(transfer.run());
        assert!(future::poll_once(worker.as_mut()).await.is_none());
        assert_eq!(recv(&client).0, [0, 3, 0, 1, 5]);
        drop(worker);
        UdpSocket::bind(addr).unwrap();
    });
}

#[test]
fn data_retry_is_identical_and_stale_ack_does_not_advance() {
    let server = socket();
    let client = socket();
    let transfer = ReadTransfer::new(
        server,
        client.local_addr().unwrap(),
        decode_rrq(&req(b"")).unwrap(),
        Cursor::new(vec![8; 512]),
        512,
        settings(),
    )
    .unwrap();
    let worker = std::thread::spawn(move || future::block_on(transfer.run()));
    let (first, peer) = recv(&client);
    ack(&client, peer, 0);
    assert_eq!(recv(&client).0, first);
    ack(&client, peer, 1);
    assert_eq!(recv(&client).0, [0, 3, 0, 2]);
    ack(&client, peer, 2);
    worker.join().unwrap().unwrap();
}

#[test]
fn legacy_server_api_still_serves_reads() {
    use async_tftp::server::TftpServerBuilder;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("boot"), [12, 34]).unwrap();
    let server = future::block_on(
        TftpServerBuilder::with_dir_ro(root.path())
            .unwrap()
            .bind("127.0.0.1:0".parse().unwrap())
            .build(),
    )
    .unwrap();
    let addr = server.listen_addr().unwrap();
    let (tx, rx) = async_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        future::block_on(future::race(server.serve(), async {
            rx.recv().await.unwrap();
            Ok(())
        }))
    });
    let client = socket();
    client.send_to(&req(b""), addr).unwrap();
    let (data, peer) = recv(&client);
    assert_eq!(data, [0, 3, 0, 1, 12, 34]);
    ack(&client, peer, 1);
    tx.send_blocking(()).unwrap();
    worker.join().unwrap().unwrap();
}
