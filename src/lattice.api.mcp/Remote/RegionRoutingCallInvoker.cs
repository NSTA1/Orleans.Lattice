using System.Collections.Frozen;
using Grpc.Core;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="CallInvoker"/> that dispatches each outbound gRPC call for one
/// facade group to the target region's channel, selected from the ambient
/// <see cref="LatticeApiMcpRegionScope"/>. Built once per group at startup over a
/// frozen region-to-invoker map so per-call routing is a single dictionary lookup
/// with no allocation; the default-region path (no region selected) returns the
/// cached default invoker field directly, adding zero work versus a non-routed
/// binding.
/// </summary>
/// <remarks>
/// The credential-forwarding interceptor is layered on top of this invoker, so
/// the caller's identity flows to whichever region this invoker dispatches to and
/// the target region authorizes independently, fail-closed. The map only ever
/// contains regions the router validated as serving this group, so a selection is
/// always resolvable; a defensive miss falls back to the default region.
/// </remarks>
internal sealed class RegionRoutingCallInvoker : CallInvoker
{
    private readonly CallInvoker _default;
    private readonly FrozenDictionary<string, CallInvoker> _byRegion;

    /// <summary>
    /// Builds the routing invoker over the default region's invoker and the frozen
    /// per-region map (which includes the default region so explicit targeting of
    /// it routes identically).
    /// </summary>
    /// <param name="defaultInvoker">The invoker for the default (current) region.</param>
    /// <param name="byRegion">The frozen region-id to invoker map for this group.</param>
    public RegionRoutingCallInvoker(
        CallInvoker defaultInvoker,
        FrozenDictionary<string, CallInvoker> byRegion)
    {
        _default = defaultInvoker ?? throw new ArgumentNullException(nameof(defaultInvoker));
        _byRegion = byRegion ?? throw new ArgumentNullException(nameof(byRegion));
    }

    private CallInvoker Selected()
    {
        var region = LatticeApiMcpRegionScope.Current;
        if (region is null)
        {
            return _default;
        }

        return _byRegion.TryGetValue(region, out var invoker) ? invoker : _default;
    }

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        => Selected().BlockingUnaryCall(method, host, options, request);

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        => Selected().AsyncUnaryCall(method, host, options, request);

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        => Selected().AsyncServerStreamingCall(method, host, options, request);

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options)
        => Selected().AsyncClientStreamingCall(method, host, options);

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options)
        => Selected().AsyncDuplexStreamingCall(method, host, options);
}
