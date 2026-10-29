# Owned RRQ seam — DEL-2.0 / sambafied-pxe issue #7

2026-10-29. Base: namespaced integration `4afd573e8aa9e72eaf23b6331ae2837282275597`, preserving upstream baseline `6cb2ca259ce353175f0f191672fdcf5b69df1d0f` and existing plan PR #1. Upstream master is unchanged.

`server::transfer` exposes bounded RRQ decoding with mode/options, immutable Settings and ReadTransfer. The application supplies a bound standard UDP socket and owned futures AsyncRead; it owns listening, quota/admission, tasks and rate-limited ERROR reporting. No executor/tasks are created by the seam. Dropping transfer/run releases reader pins and socket. A reader-length guard rejects truncated/growing streams and enforces the preflight no-wrap bound. All-option omission is selected before OACK; window options are always omitted. A single absolute timer includes reader stalls, sends and ACK waits. Existing server API remains available through the same RRQ implementation.

The shared RRQ buffer now uses initialized storage instead of constructing a mutable byte slice over uninitialized memory. Original upstream notice/license files are unchanged. The example-only async-tar dependency moves from 0.5.1 to 0.6.1: 0.5.x's xattr 0.2 fails against current libc's removed ENOATTR alias on Linux. Existing examples compile after that correction; production library dependencies are unchanged.

Direct evidence:

- `cargo test --locked --lib --test transfer`: 11 original codec tests + 10 integration tests pass. Tests cover strict RRQ/options metadata; conservative DATA1/no OACK and terminal zero; exact negotiated timeout/tsize/block cap and ACK0; OACK retry exhaustion without downgrade; wrong-peer and stale ACK isolation; identical DATA retransmission; dropping four owned pending transfers releases all four reader pins and sockets; cancellation during ACK wait; stalled-reader deadline; mode/no-wrap/length rejection; and the original server public API with a temporary disk fixture.
- `cargo check --locked --examples`: pass, including existing Tokio and tar examples.
- `cargo fmt --all -- --check`, `git diff --check`: pass.
- Standard Clippy completes with 16 inherited formatting-borrow warnings in legacy read/server/write paths. Optional `-D warnings` fails on those warnings; no warning-free claim.
- Cargo.lock is upstream-ignored. The first offline attempt could not fetch uncached example dependencies; it is not counted as a passed test. No upstream external-client feature suite was run. All executed network tests bind high-port IPv4 loopback only.

Product acceptance remains open until the dedicated product compiles against the exact reviewed merged Git pin with its explicit Tokio compatibility adapter and owned-task probe. This fork evidence is not boot qualification, live network readiness, release or deployment evidence. The caller must implement pathname/CAS policy before opening a reader; this seam never opens a file or authorizes a path.
