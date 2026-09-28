using System.Collections.Immutable;
using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>
/// Strongly typed client for the administrative app catalogue. The supplied invoker owns routing, credentials,
/// TLS and deadlines.
/// </summary>
public sealed class LatticeAppCatalogApiGrpcClient : ILatticeAppCatalog
{
    private static readonly AppsEmptyRequest EmptyRequest = new();
    private readonly CallInvoker _invoker;
    private readonly LatticeAppCatalogGrpcMethods _methods;

    private LatticeAppCatalogApiGrpcClient(CallInvoker invoker, LatticeAppCatalogGrpcMethods methods)
    {
        _invoker = invoker;
        _methods = methods;
    }

    /// <summary>Creates a client using Orleans serializers from the supplied provider; does not own either argument.</summary>
    /// <param name="callInvoker">The call invoker the client sends through.</param>
    /// <param name="serializerProvider">The provider supplying Orleans serializers for the app messages.</param>
    /// <returns>The client.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static LatticeAppCatalogApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);
        return new(callInvoker, new LatticeAppCatalogGrpcMethods(serializerProvider));
    }

    /// <inheritdoc />
    public async Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default)
        => (await UnaryAsync(_methods.ListSources, EmptyRequest, cancellationToken).ConfigureAwait(false)).Sources;

    /// <inheritdoc />
    public Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        return UnaryAsync(_methods.ListAvailable, query, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<AppDescriptor?> DescribeFromSourceAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return (await UnaryAsync(_methods.DescribeFromSource,
            new AppsSourceAppRequest { SourceKey = sourceKey, Slug = appSlug, Version = version },
            cancellationToken).ConfigureAwait(false)).Descriptor;
    }

    /// <inheritdoc />
    public async Task<AppIconAsset?> GetIconAsync(
        string sourceKey,
        string appSlug,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return (await UnaryAsync(_methods.GetIcon,
            new AppsSourceAppRequest { SourceKey = sourceKey, Slug = appSlug, Version = version },
            cancellationToken).ConfigureAwait(false)).Icon;
    }

    /// <inheritdoc />
    public Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetCapabilities, EmptyRequest, cancellationToken);

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CancellationToken cancellationToken)
        where TRequest : class where TResponse : class
    {
        using var call = _invoker.AsyncUnaryCall(method, null, new CallOptions(cancellationToken: cancellationToken), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
