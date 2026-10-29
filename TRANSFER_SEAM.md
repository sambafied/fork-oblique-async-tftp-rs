# Transfer seam plan (sambafied-pxe ADR-016 / DEL-2.0)

Pin baseline: `6cb2ca259ce353175f0f191672fdcf5b69df1d0f`.

## Required public API (bounded fork)

1. Decoded RRQ metadata visible to the product (path, mode, requested options).
2. A transfer future the product can `select!` / cancel; workers must not detach.
3. Immutable transfer settings: retries (~8 product default), timeout, ignore windowsize, omit-all-options conservative mode.
4. Product owns UDP listen, peer quotas, and JoinHandles; do not call library `serve()` as the product loop.
5. Reader + length supplied by product (CAS Handler); no DirHandler as security boundary.

## Probe checklist

- [ ] Compile product against public API only
- [x] Cancel stops IO and drops pin within deadline
- [x] Conservative path omits options up front
- [x] Bounded concurrent transfers (caller-owned futures; no hidden workers)

MIT license preserved; attribute upstream oblique/async-tftp-rs.

Implementation and executed fork evidence: [TRANSFER_SEAM_EVIDENCE.md](TRANSFER_SEAM_EVIDENCE.md). Product compile/adapter acceptance remains a separate immutable-pin probe.
