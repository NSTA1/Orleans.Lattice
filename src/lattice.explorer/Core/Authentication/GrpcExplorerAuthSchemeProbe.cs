using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;
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
    public Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
        string address,
        bool allowUnencryptedHttp2 = false,
        CancellationToken cancellationToken = default)
        => ProbeAsync(address, allowUnencryptedHttp2, transportHeaders: null, cancellationToken);

    /// <inheritdoc />
    public async Task<ExplorerAuthSchemeAdvertisement> ProbeAsync(
        string address,
        bool allowUnencryptedHttp2,
        IReadOnlyDictionary<string, string>? transportHeaders,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);

        // The probe's channel is built by the shared factory like every other
        // Explorer channel, so its transport posture is scoped to this one
        // short-lived channel rather than written into process-global state. It
        // carries no credential, so the insecure-channel safeguard never comes
        // into play here.
        using var channel = LatticeGrpcChannelFactory.CreateChannel(
            new LatticeConnectionSettings
            {
                Address = address,
                AllowUnencryptedHttp2 = allowUnencryptedHttp2,
            });

        try
        {
            // Transport headers gate the unauthenticated probe the same way they
            // gate every other call (for example an origin-routing header a
            // fronting proxy requires); without them a proxy-guarded endpoint
            // rejects the probe and discovery wrongly falls back to manual/Basic.
            var invoker = LatticeGrpcChannelFactory.ApplyTransportHeaders(
                channel.CreateCallInvoker(),
                transportHeaders);
            var client = LatticeStateApiGrpcClient.Create(invoker, _serializerProvider);
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
