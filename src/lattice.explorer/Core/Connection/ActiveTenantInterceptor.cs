using Grpc.Core;
using Grpc.Core.Interceptors;
using GrpcMetadata = Grpc.Core.Metadata;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// Asserts the circuit's active tenant on every outgoing call through the
/// <see cref="LatticeActiveTenantAssertion.DefaultHeaderName"/> metadata header,
/// reading it from an <see cref="ILatticeActiveTenantProvider"/> at the moment
/// the call starts.
/// </summary>
/// <remarks>
/// <para>
/// <b>Read per call, never captured.</b> The tenant is asked for on each call, so
/// a tenant switch changes the very next call and one channel can never carry a
/// stale tenant. The only thing kept between calls is the last header entry
/// built, reused while the tenant is unchanged: an entry is immutable, so reusing
/// it cannot carry one call's tenant into another.
/// </para>
/// <para>
/// <b>It owns the header.</b> It runs innermost, after every other interceptor
/// has added its metadata, and removes any value for the header it finds before
/// adding its own, so a statically configured value can never stand in for the
/// circuit's tenant. When the provider asserts none the header is removed and
/// nothing is added, so the call reaches the cluster as the reserved default
/// tenant's would.
/// </para>
/// <para>
/// <b>Allocation.</b> A call that asserts no tenant and carries no metadata
/// allocates nothing here. A call that asserts one adds the cached entry to the
/// call's metadata in place, as gRPC's own metadata interceptor does, and
/// allocates a metadata collection only when the call carried none.
/// </para>
/// </remarks>
/// <param name="provider">The circuit's live tenant source.</param>
internal sealed class ActiveTenantInterceptor(ILatticeActiveTenantProvider provider) : Interceptor
{
    /// <summary>The metadata key the tenant is asserted under: the cluster's default header name.</summary>
    internal const string HeaderName = LatticeActiveTenantAssertion.DefaultHeaderName;

    private readonly ILatticeActiveTenantProvider _provider = provider ?? throw new ArgumentNullException(nameof(provider));
    private GrpcMetadata.Entry? _entry;

    /// <inheritdoc />
    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, Assert(context));

    /// <inheritdoc />
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, Assert(context));

    /// <inheritdoc />
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(request, Assert(context));

    /// <inheritdoc />
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(Assert(context));

    /// <inheritdoc />
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(Assert(context));

    private ClientInterceptorContext<TRequest, TResponse> Assert<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var tenant = _provider.AssertedTenant;
        var headers = context.Options.Headers;
        if (headers is not null)
        {
            RemoveHeader(headers);
        }

        if (string.IsNullOrEmpty(tenant))
        {
            return context;
        }

        var entry = EntryFor(tenant);
        if (headers is not null)
        {
            headers.Add(entry);
            return context;
        }

        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            context.Options.WithHeaders(new GrpcMetadata { entry }));
    }

    private GrpcMetadata.Entry EntryFor(string tenant)
    {
        var cached = Volatile.Read(ref _entry);
        if (cached is not null && string.Equals(cached.Value, tenant, StringComparison.Ordinal))
        {
            return cached;
        }

        cached = new GrpcMetadata.Entry(HeaderName, tenant);
        Volatile.Write(ref _entry, cached);
        return cached;
    }

    private static void RemoveHeader(GrpcMetadata headers)
    {
        for (var i = headers.Count - 1; i >= 0; i--)
        {
            if (string.Equals(headers[i].Key, HeaderName, StringComparison.OrdinalIgnoreCase))
            {
                headers.RemoveAt(i);
            }
        }
    }
}
