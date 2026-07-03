using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The production <see cref="IExplorerAuthSchemeProbe"/>: opens a short-lived
/// gRPC channel to the endpoint and calls the unauthenticated
/// <c>GetAuthScheme</c> RPC, mapping the wire advertisement onto the explorer's
/// <see cref="ExplorerAuthSchemeAdvertisement"/>. An endpoint that does not
/// implement the RPC (an older server) or is unreachable yields
/// <see cref="ExplorerAuthSchemeAdvertisement.Empty"/> so discovery degrades to
/// manual selection instead of failing.
/// </summary>
public sealed class GrpcExplorerAuthSchemeProbe : IExplorerAuthSchemeProbe, IDisposable
{
    private readonly ServiceProvider _serializerProvider;

    /// <summary>Creates the probe, building its own Orleans serializer provider.</summary>
    public GrpcExplorerAuthSchemeProbe()
    {
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
    }

    /// <inheritdoc />
    public async Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
        string address,
        bool allowUnencryptedHttp2 = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);

        if (allowUnencryptedHttp2)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        using var channel = GrpcChannel.ForAddress(address);
        try
        {
            var client = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), _serializerProvider);
            var advertisement = await client
                .GetAuthSchemeAsync(new AuthSchemeAdvertisementRequest(), cancellationToken)
                .ConfigureAwait(false);

            return Map(advertisement);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RpcException)
        {
            // Endpoint does not advertise (Unimplemented), is unreachable, or
            // rejected the probe: fall back to manual/Basic selection.
            return ExplorerAuthSchemeAdvertisement.Empty;
        }
    }

    private static ExplorerAuthSchemeAdvertisement Map(AuthSchemeAdvertisement advertisement)
    {
        if (advertisement.Schemes.Count == 0)
        {
            return ExplorerAuthSchemeAdvertisement.Empty;
        }

        var schemes = new ExplorerAuthSchemeDescriptor[advertisement.Schemes.Count];
        for (var i = 0; i < schemes.Length; i++)
        {
            var descriptor = advertisement.Schemes[i];
            schemes[i] = new ExplorerAuthSchemeDescriptor
            {
                SchemeId = descriptor.SchemeId,
                DisplayName = descriptor.DisplayName,
                Parameters = descriptor.Parameters,
            };
        }

        return new ExplorerAuthSchemeAdvertisement { Schemes = schemes };
    }

    /// <inheritdoc />
    public void Dispose() => _serializerProvider.Dispose();
}
