# Cross-Cluster Replication

## What it shows

Active-active cross-cluster replication. This sample stands up **two independent
in-process Orleans clusters** (`site-a` and `site-b`), wires them together with
Orleans.Lattice replication over the canonical gRPC push transport, then writes a
key on `site-a` **only** and watches it converge onto `site-b` - with no direct
write to `site-b`. The `orders` tree is opted into replication as a
last-writer-wins register; each site ships its mutations to the other.

This is the one sample that hosts two clusters and the only one that runs on an
ASP.NET Core / Kestrel pipeline, because the gRPC replication receiver is served
over HTTP/2 (here plaintext h2c on loopback).

## Run it

```
dotnet run --project samples/CrossClusterReplication
```

## Expected output

```
Starting two independent Orleans clusters (site-a, site-b)...
Both clusters ready and peered over gRPC.

== Before ==
  site-b sees 'order/1001' = (absent)

== Writing on site-a only ==
  site-a wrote 'order/1001' = CONFIRMED

== Waiting for convergence on site-b (no direct write) ==
  .  converged.
  site-b now sees 'order/1001' = CONFIRMED

[OK] the write made on site-a converged onto site-b across clusters.
```

(The number of `.` poll dots before convergence varies run to run with shipping
latency; the before/after values and the converged result are stable.)

## When to use

- Multi-region / multi-datacenter deployments that want each region to serve
  reads and writes locally while staying eventually consistent with the others.
- Active-active topologies where any cluster may write any replicated tree and
  conflicting updates must converge deterministically by CRDT merge mode.

## When not to use

- Single-cluster deployments - replication adds a shipping pipeline you do not
  need. Use plain `AddLattice` there.
- Strong/linearizable cross-region consistency. Replication is convergent and
  causally consistent, not a global consensus protocol.

## Notes on this sample

- Uses plaintext HTTP/2 (h2c) on loopback with `AllowPlaintextEndpoints = true`.
  Each site binds Kestrel to HTTP/2 with no certificate and grpc-dotnet speaks
  h2c by prior knowledge over an `http://` address, so no process-wide switch is
  involved. Production deployments should use `https://` peer endpoints and leave
  `AllowPlaintextEndpoints` at its secure default.
- Receiver-side shared-secret authentication is turned off
  (`RequireAuthentication = false`) because this is a loopback demo with no
  secret material. Production must supply a secret and leave authentication on.
- The gRPC ports default to `17001` / `17002`; change them in `Program.cs` if
  those are taken on your machine.

## Feature docs

[docs/lattice.replication/README.md](../../docs/lattice.replication/README.md)
