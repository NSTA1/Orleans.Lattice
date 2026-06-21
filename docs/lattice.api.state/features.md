# Orleans.Lattice.Api.State Feature Index

Feature planning for the `Orleans.Lattice.Api.State` package - a read-only cluster state-query / observe / subscribe API layered on top of `Orleans.Lattice` - is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files. See the [package overview](./README.md) for the user-facing description. This page is a grouped, human-readable index that links each tracked item to its issue. Keep it in sync whenever an issue is opened, closed, or retitled.

- **Browse all api.state issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=Orleans.Lattice.Api.State

## Package boundary

Everything tracked here ships in the `Orleans.Lattice.Api.State` assembly (the transport-agnostic facade and model) and its `Orleans.Lattice.Api.State.Grpc` companion (the code-first gRPC binding and public client). Public API lives under `Orleans.Lattice.Api.State` and `Orleans.Lattice.Api.State.Grpc`; the facade interfaces, the gRPC service, and the marshallers are internal. The package depends only on `Orleans.Lattice`.

The surface is **strictly read-only**: it observes trees, structure, entries, change feeds, and metrics. It exposes no mutation verb. One core-library change supports it - a push-up structural topology digest so a tree's shape can be read without walking every node.

**Non-goals:** any write / delete / reconfigure verb, a bundled dashboard UI, and the MCP bridge itself (the facade is built to be reused by one, but the bridge ships separately).

## Epic

- [F-110](https://github.com/NSTA1/Orleans.Lattice/issues/836) - Orleans.Lattice.Api.State: cluster state query / observe / subscribe API (epic)

## Features

### Shipped

- [F-111](https://github.com/NSTA1/Orleans.Lattice/issues/825) - Project & package scaffolding
- [F-112](https://github.com/NSTA1/Orleans.Lattice/issues/826) - Transport-agnostic state-query model & read facade
- [F-113](https://github.com/NSTA1/Orleans.Lattice/issues/827) - Tree & view discovery / catalog endpoint
- [F-114](https://github.com/NSTA1/Orleans.Lattice/issues/828) - Push-up structural tree metadata (topology digest)
- [F-115](https://github.com/NSTA1/Orleans.Lattice/issues/829) - Tree-structure query endpoint
- [F-116](https://github.com/NSTA1/Orleans.Lattice/issues/830) - Entry / key-range inspection endpoint
- [F-117](https://github.com/NSTA1/Orleans.Lattice/issues/831) - gRPC contract & service host
- [F-118](https://github.com/NSTA1/Orleans.Lattice/issues/832) - Change observation (subscribe / server-stream)
- [F-119](https://github.com/NSTA1/Orleans.Lattice/issues/833) - Live metadata / metrics observation
- [F-120](https://github.com/NSTA1/Orleans.Lattice/issues/834) - Efficiency & overhead guardrails
- [F-121](https://github.com/NSTA1/Orleans.Lattice/issues/835) - Docs, sample explorer & end-to-end tests
