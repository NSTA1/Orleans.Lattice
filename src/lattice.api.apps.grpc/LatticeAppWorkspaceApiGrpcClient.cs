using System.Collections.Immutable;
using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>
/// Strongly typed client for the per-user app workspace. The supplied invoker owns routing, credentials, TLS
/// and deadlines; the caller's credential is what the workspace evaluates.
/// </summary>
public sealed class LatticeAppWorkspaceApiGrpcClient : ILatticeAppWorkspace
{
    private static readonly AppsEmptyRequest EmptyRequest = new();
    private readonly CallInvoker _invoker;
    private readonly LatticeAppWorkspaceGrpcMethods _methods;

    private LatticeAppWorkspaceApiGrpcClient(CallInvoker invoker, LatticeAppWorkspaceGrpcMethods methods)
    {
        _invoker = invoker;
        _methods = methods;
    }

    /// <summary>Creates a client using Orleans serializers from the supplied provider; does not own either argument.</summary>
    /// <param name="callInvoker">The call invoker the client sends through.</param>
    /// <param name="serializerProvider">The provider supplying Orleans serializers for the app messages.</param>
    /// <returns>The client.</returns>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static LatticeAppWorkspaceApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);
        return new(callInvoker, new LatticeAppWorkspaceGrpcMethods(serializerProvider));
    }

    /// <inheritdoc />
    public async Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)
        => (await UnaryAsync(_methods.ListMyApps, EmptyRequest, cancellationToken).ConfigureAwait(false)).Apps;

    /// <inheritdoc />
    public async Task<WorkspaceAppDescriptor?> DescribeMyAppAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return (await UnaryAsync(_methods.DescribeMyApp,
            new AppsSlugRequest { Slug = appSlug }, cancellationToken).ConfigureAwait(false)).Descriptor;
    }

    /// <inheritdoc />
    public async Task<AppIconAsset?> GetIconAsync(string appSlug, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        return (await UnaryAsync(_methods.GetIcon,
            new AppsSlugRequest { Slug = appSlug }, cancellationToken).ConfigureAwait(false)).Icon;
    }

    /// <inheritdoc />
    public async Task<AppUiAsset?> GetUiAssetAsync(string appSlug, string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(appSlug);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return (await UnaryAsync(_methods.GetUiAsset,
            new AppsUiAssetRequest { Slug = appSlug, Path = path }, cancellationToken).ConfigureAwait(false)).Asset;
    }

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CancellationToken cancellationToken)
        where TRequest : class where TResponse : class
    {
        using var call = _invoker.AsyncUnaryCall(method, null, new CallOptions(cancellationToken: cancellationToken), request);
        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
