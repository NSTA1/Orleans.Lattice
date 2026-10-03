using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// A real state-API gRPC server on a Kestrel endpoint bound HTTP/2-only on a
/// plain <c>http://</c> loopback address and an ephemeral port, over scripted
/// facades.
/// </summary>
/// <remarks>
/// <para>
/// Several types in Core build their own <c>GrpcChannel</c> through
/// <see cref="Orleans.Lattice.Explorer.Core.Connection.LatticeGrpcChannelFactory"/>
/// and so cannot have an in-memory test handler swapped into them. A real
/// listening endpoint is the only way to drive those end to end, and h2c is what
/// a channel built for an endpoint that opted into unencrypted transport speaks:
/// there is no TLS and therefore no ALPN, so the endpoint is reachable solely by
/// HTTP/2 prior knowledge.
/// </para>
/// <para>
/// Authorization is off by default, which is the posture these fixtures want -
/// the Explorer's own credential pipeline is covered by the channel-factory
/// fixtures. Pass <c>requireAuthorization: true</c> to leave the binding's
/// default-deny authorizer in force, which is how a caller sees an endpoint that
/// demands a sign-in.
/// </para>
/// </remarks>
internal sealed class StateApiH2cServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private StateApiH2cServer(WebApplication app, string address)
    {
        _app = app;
        Address = address;
    }

    /// <summary>The <c>http://</c> address the server is listening on.</summary>
    public string Address { get; }

    /// <summary>An address nothing listens on, for a probe that must find the endpoint down.</summary>
    public const string UnreachableAddress = "http://127.0.0.1:1";

    /// <summary>Starts the server over the supplied facades.</summary>
    /// <param name="query">The read facade every unary RPC is served from.</param>
    /// <param name="observer">The change-stream facade; substituted when omitted.</param>
    /// <param name="metrics">The metrics facade; substituted when omitted.</param>
    /// <param name="requireAuthorization">Whether the binding's default-deny authorizer is enforced.</param>
    /// <returns>The started server.</returns>
    public static async Task<StateApiH2cServer> StartAsync(
        ILatticeStateQuery query,
        ILatticeStateObserver? observer = null,
        ILatticeStateMetricsObserver? metrics = null,
        bool requireAuthorization = false)
    {
        ArgumentNullException.ThrowIfNull(query);

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();

        // Loopback rather than ListenLocalhost, which refuses an ephemeral port;
        // HTTP/2 only, so the endpoint is reachable solely by h2c prior knowledge.
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));

        builder.Services.AddSerializer();
        builder.Services.AddSingleton(query);
        builder.Services.AddSingleton(observer ?? Substitute.For<ILatticeStateObserver>());
        builder.Services.AddSingleton(metrics ?? Substitute.For<ILatticeStateMetricsObserver>());
        builder.Services.AddLatticeStateApiGrpc(options => options.RequireAuthorization = requireAuthorization);

        var app = builder.Build();
        app.MapLatticeStateApiGrpc();
        await app.StartAsync().ConfigureAwait(false);

        var address = app.Services
            .GetRequiredService<IServer>()
            .Features
            .Get<IServerAddressesFeature>()!
            .Addresses
            .First();

        return new StateApiH2cServer(app, address);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
    }
}
