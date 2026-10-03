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
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// Regression for issue #4337: a caller-cancelled auth-scheme probe against a
/// live, advertising endpoint must surface as cancellation rather than as an
/// empty advertisement, which would push the sign-in onto the Basic fallback.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class GrpcExplorerAuthSchemeProbeCancellationTests
{
    [Test]
    public async Task A_cancelled_probe_throws_rather_than_reporting_an_empty_advertisement()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddSerializer();
        builder.Services.AddSingleton(Substitute.For<ILatticeStateQuery>());
        builder.Services.AddSingleton(Substitute.For<ILatticeStateObserver>());
        builder.Services.AddSingleton(Substitute.For<ILatticeStateMetricsObserver>());
        builder.Services.AddLatticeStateApiGrpc(options =>
            options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor
            {
                SchemeId = ExplorerAuthSchemes.Entra,
                DisplayName = "Microsoft Entra ID",
            }));

        var app = builder.Build();
        app.MapLatticeStateApiGrpc();
        await app.StartAsync();

        try
        {
            var address = app.Services
                .GetRequiredService<IServer>()
                .Features
                .Get<IServerAddressesFeature>()!
                .Addresses
                .First();

            using var probe = new GrpcExplorerAuthSchemeProbe();
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();

            Assert.That(
                async () => await probe.ProbeAsync(address, allowUnencryptedHttp2: true, cancelled.Token),
                Throws.InstanceOf<OperationCanceledException>(),
                "a caller who gave up must not be told the endpoint advertises nothing");
        }
        finally
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }
}
