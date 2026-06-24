namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire response for the entry-scan cancel RPC. A best-effort acknowledgement
/// that the server processed the cursor-release request. It carries no payload:
/// cancel is idempotent, so an unknown, already-drained, or freshly-closed
/// cursor and a live one that was just closed are indistinguishable to the
/// caller, and both are success.
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.EntryScanCancelResponse)]
[Immutable]
public sealed record EntryScanCancelResponse;

