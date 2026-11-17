# fpa-gateway

Rust 1.99.1 tonic native HTTP/2 gRPC gateway for the independent FPA results stack.
Matches prociv-gateway: separate binary/library crates, clap/env/dotenv settings,
reflection, standard health service, gzip and graceful SIGTERM. Dependencies use
full public Git revisions, with no sibling checkout or protoc requirement.

The gateway has no upstream HTTP access. Moka caches include the RPC method and
encoded request, coalesce public misses and never cache errors. Catalog TTLs are
3600/600/300 seconds; phase snapshots 15 seconds; match sheets 10 seconds.
Retrieval timestamps survive cache hits unchanged. Credential/tenant/binary
metadata bypass shared caching; transport metadata is stripped, other metadata
forwarded. HTTPS uses native roots, with bounded deadlines and concurrency.


```sh
rtk cargo run --locked
rtk just verify
```

Default port: 50202. See `.env.example` and `--help`.
The complete flow is fpa-app → fpa-gateway → fpa-proxy → FPA. Packaging,
Compose, Cloud Run and Vercel releases belong to fpa-infra. No background archive,
push scoring stream or invented match clock is provided. The app polls snapshots;
in-play freshness depends on FPA's publication behavior.
