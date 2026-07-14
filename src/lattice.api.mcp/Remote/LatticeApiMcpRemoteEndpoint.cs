using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The per-group remote endpoint configuration for one facade group under the
/// <c>Orleans.Lattice.Api.Mcp</c> remote-host topology. It names the served
/// <see cref="Endpoint"/> the group is reached at - surfaced verbatim in the
/// <c>lattice_capabilities</c> report - and optionally supplies a pre-built
/// <see cref="CallInvoker"/> so a host that already owns a tuned gRPC channel
/// (custom TLS, retries, deadlines) can reuse it instead of the address-derived
/// default.
/// </summary>
/// <remarks>
/// When <see cref="CallInvoker"/> is <see langword="null"/> the remote binding
/// builds a channel from <see cref="Endpoint"/> with
/// <c>GrpcChannel.ForAddress</c>; when it is supplied the binding uses it as-is
/// and treats <see cref="Endpoint"/> as advertisement metadata only. Either way
/// the caller-credential-forwarding interceptor is layered on top so the ambient
/// caller credential flows to the remote cluster.
/// </remarks>
public sealed class LatticeApiMcpRemoteEndpoint
{
    /// <summary>
    /// The served endpoint the group is reached at (for example
    /// <c>https://cluster-a.internal:5001</c>). Required. Reported verbatim in the
    /// group's <see cref="LatticeApiMcpGroupCapability.Endpoint"/> slot, and used
    /// to build the gRPC channel when <see cref="CallInvoker"/> is not supplied.
    /// </summary>
    public required string Endpoint { get; init; }

    /// <summary>
    /// An optional pre-built gRPC call invoker for the group. When supplied it is
    /// used directly (letting a host reuse a channel it configured with bespoke
    /// transport policy); when <see langword="null"/> a channel is built from
    /// <see cref="Endpoint"/>.
    /// </summary>
    public CallInvoker? CallInvoker { get; init; }
}
