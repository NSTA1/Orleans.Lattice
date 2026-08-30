using System.Net;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// End-to-end proof that a channel built by
/// <see cref="LatticeGrpcChannelFactory"/> for an endpoint which opted into
/// unencrypted transport still speaks HTTP/2 over cleartext (h2c), now that the
/// process-global <c>Http2UnencryptedSupport</c> app switch is gone (issue
/// #1784).
/// </summary>
/// <remarks>
/// <para>
/// The server is a Kestrel endpoint bound <c>Http2</c>-only on a plain
/// <c>http://</c> address, so it accepts a connection only by h2c prior
/// knowledge - there is no TLS and therefore no ALPN to negotiate with, and an
/// HTTP/1.1 client is refused outright.
/// </para>
/// <para>
/// The discriminator is the gRPC status. A call that never reached the server
/// fails <see cref="StatusCode.Unavailable"/> or
/// <see cref="StatusCode.Internal"/> (a transport or protocol fault); a call
/// that did reach it and got an HTTP 404 for an unmapped path fails
/// <see cref="StatusCode.Unimplemented"/>. Only the latter is possible over a
/// connection that actually established, which is what makes it proof rather
/// than a smoke test - pointing the same test at an HTTP/1.1-only Kestrel
/// endpoint yields <see cref="StatusCode.Internal"/> instead.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeGrpcChannelFactoryH2cTests
{
    private const string Http2UnencryptedSwitch =
        "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport";

    /// <summary>
    /// An arbitrary unary method. The server maps nothing at its path, so the
    /// call is answered rather than served - which is exactly what proves the
    /// transport came up.
    /// </summary>
    private static readonly Method<byte[], byte[]> ProbeMethod = new(
        MethodType.Unary,
        "lattice.explorer.test.H2cProbe",
        "Probe",
        Marshallers.Create(static value => value, static bytes => bytes),
        Marshallers.Create(static value => value, static bytes => bytes));

    [Test]
    public async Task A_channel_for_an_opted_in_endpoint_reaches_a_plaintext_http2_server()
    {
        await using var server = await H2cServer.StartAsync().ConfigureAwait(false);

        var settings = new LatticeConnectionSettings
        {
            Address = server.Address,
            AllowUnencryptedHttp2 = true,
        };

        using var channel = LatticeGrpcChannelFactory.CreateChannel(settings);
        var invoker = LatticeGrpcChannelFactory.CreateCallInvoker(channel, settings);

        var fault = Assert.ThrowsAsync<RpcException>(async () =>
            await invoker
                .AsyncUnaryCall(ProbeMethod, host: null, new CallOptions(), [])
                .ResponseAsync
                .ConfigureAwait(false));

        Assert.Multiple(() =>
        {
            Assert.That(
                fault!.StatusCode,
                Is.EqualTo(StatusCode.Unimplemented),
                "the server answered, so the h2c connection established; Unavailable would mean it never did");

            // The whole point of the fix: the opted-in channel worked without
            // writing anything another circuit in this process could observe.
            Assert.That(
                AppContext.TryGetSwitch(Http2UnencryptedSwitch, out _),
                Is.False,
                "the per-channel opt-in must leave no process-global trace");
        });
    }

    [Test]
    public async Task A_channel_that_did_not_opt_in_keeps_the_insecure_credential_safeguard()
    {
        await using var server = await H2cServer.StartAsync().ConfigureAwait(false);

        // The enforced half of the opt-in, and the one a sibling circuit could
        // never influence even before this change: credentials are withheld from
        // a channel gRPC cannot confirm is secure unless this endpoint itself
        // opted in. Building an opted-in channel first proves it is unaffected
        // by the sibling.
        var optedIn = new LatticeConnectionSettings
        {
            Address = server.Address,
            AllowUnencryptedHttp2 = true,
            Authentication = LatticeCallAuthentication.Bearer(new FakeCredentialProvider()),
        };
        using var optedInChannel = LatticeGrpcChannelFactory.CreateChannel(optedIn);

        var notOptedIn = optedIn with { AllowUnencryptedHttp2 = false };

        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeGrpcChannelFactory.BuildChannelOptions(optedIn).UnsafeUseInsecureChannelCallCredentials,
                Is.True);
            Assert.That(
                LatticeGrpcChannelFactory.BuildChannelOptions(notOptedIn).UnsafeUseInsecureChannelCallCredentials,
                Is.False,
                "a sibling channel's opt-in must not lift this channel's safeguard");
        });
    }

    /// <summary>
    /// A Kestrel endpoint bound HTTP/2-only on a plain <c>http://</c> address and
    /// an ephemeral port, so a client reaches it only over h2c.
    /// </summary>
    private sealed class H2cServer : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private H2cServer(WebApplication app, string address)
        {
            _app = app;
            Address = address;
        }

        /// <summary>The <c>http://</c> address the server is listening on.</summary>
        public string Address { get; }

        public static async Task<H2cServer> StartAsync()
        {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();

            // Loopback rather than ListenLocalhost, which refuses an ephemeral
            // port; HTTP/2 only, so the endpoint is reachable solely by h2c
            // prior knowledge.
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));

            var app = builder.Build();
            await app.StartAsync().ConfigureAwait(false);

            var address = app.Services
                .GetRequiredService<IServer>()
                .Features
                .Get<IServerAddressesFeature>()!
                .Addresses
                .First();

            return new H2cServer(app, address);
        }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync().ConfigureAwait(false);
            await _app.DisposeAsync().ConfigureAwait(false);
        }
    }
}
