using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// Unit coverage for <see cref="LatticeGrpcChannelFactory"/>, the single seam
/// every Explorer gRPC client builds its channel and call invoker through.
/// </summary>
/// <remarks>
/// The insecure-channel safeguard the factory resolves has its own fixture,
/// <see cref="GrpcInsecureChannelSafeguardTests"/>; this one covers the argument
/// contract, the transport-header decoration, and the invariant that motivated
/// the seam - that building a channel for an endpoint which opted into
/// unencrypted transport writes nothing to process-global state.
/// </remarks>
[TestFixture]
public sealed class LatticeGrpcChannelFactoryTests
{
    private const string Http2UnencryptedSwitch =
        "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport";

    private static LatticeConnectionSettings Settings(
        string address = "http://lattice.example:5199",
        bool allowUnencrypted = true,
        IReadOnlyDictionary<string, string>? transportHeaders = null) =>
        new()
        {
            Address = address,
            AllowUnencryptedHttp2 = allowUnencrypted,
            TransportHeaders = transportHeaders,
        };

    [Test]
    public void CreateChannel_for_an_unencrypted_endpoint_writes_no_process_global_switch()
    {
        // The defect this seam exists to fix: the opt-in belongs to one channel,
        // so nothing about it may be observable process-wide. A circuit that
        // never opted in must not inherit another circuit's choice.
        using var channel = LatticeGrpcChannelFactory.CreateChannel(Settings());

        Assert.That(
            AppContext.TryGetSwitch(Http2UnencryptedSwitch, out _),
            Is.False,
            "building a channel for an opted-in endpoint must not set the process-wide h2c switch");
    }

    [Test]
    public void BuildChannelOptions_scopes_the_transport_handler_to_one_channel()
    {
        using var first = LatticeGrpcChannelFactory.BuildChannelOptions(Settings()).HttpHandler;
        using var second = LatticeGrpcChannelFactory.BuildChannelOptions(Settings()).HttpHandler;

        Assert.Multiple(() =>
        {
            // The whole shape of the fix: a handler per channel, not a switch per
            // process. Two channels never share the transport decision.
            Assert.That(first, Is.InstanceOf<SocketsHttpHandler>());
            Assert.That(second, Is.InstanceOf<SocketsHttpHandler>());
            Assert.That(first, Is.Not.SameAs(second));
        });
    }

    [Test]
    public void BuildChannelOptions_matches_the_handler_grpc_would_have_built_itself()
    {
        var options = LatticeGrpcChannelFactory.BuildChannelOptions(Settings());
        using var handler = options.HttpHandler;

        // Grpc.Net.Client's own primary handler is a SocketsHttpHandler with
        // EnableMultipleHttp2Connections set, so supplying one changes nothing
        // but its lifetime and its scope.
        Assert.That(
            ((SocketsHttpHandler)handler!).EnableMultipleHttp2Connections,
            Is.True);
    }

    [Test]
    public void BuildChannelOptions_gives_the_channel_ownership_of_the_handler()
    {
        var options = LatticeGrpcChannelFactory.BuildChannelOptions(Settings());
        using var handler = options.HttpHandler;

        // Supplying a handler suppresses gRPC's default-disposal rule, so without
        // this flag every rebuild on an endpoint or sign-in change would leak the
        // handler's connection pool.
        Assert.That(options.DisposeHttpClient, Is.True);
    }

    [Test]
    public void CreateChannel_binds_the_configured_address()
    {
        using var channel = LatticeGrpcChannelFactory.CreateChannel(Settings("http://lattice.example:5199"));

        Assert.That(channel.Target, Is.EqualTo("lattice.example:5199"));
    }

    [Test]
    public void CreateChannel_accepts_an_https_endpoint()
    {
        using var channel = LatticeGrpcChannelFactory.CreateChannel(
            Settings("https://lattice.example:443", allowUnencrypted: false));

        // The target is the URI authority, which elides the scheme's default
        // port.
        Assert.That(channel.Target, Is.EqualTo("lattice.example"));
    }

    [Test]
    public void CreateChannel_rejects_null_settings() =>
        Assert.That(
            () => LatticeGrpcChannelFactory.CreateChannel(null!),
            Throws.TypeOf<ArgumentNullException>());

    [Test]
    public void BuildChannelOptions_rejects_null_settings() =>
        Assert.That(
            () => LatticeGrpcChannelFactory.BuildChannelOptions(null!),
            Throws.TypeOf<ArgumentNullException>());

    [Test]
    public void CreateCallInvoker_rejects_a_null_channel() =>
        Assert.That(
            () => LatticeGrpcChannelFactory.CreateCallInvoker(null!, Settings()),
            Throws.TypeOf<ArgumentNullException>());

    [Test]
    public void CreateCallInvoker_rejects_null_settings()
    {
        using var channel = LatticeGrpcChannelFactory.CreateChannel(Settings());

        Assert.That(
            () => LatticeGrpcChannelFactory.CreateCallInvoker(channel, null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void CreateCallInvoker_returns_an_invoker_for_an_anonymous_connection()
    {
        using var channel = LatticeGrpcChannelFactory.CreateChannel(Settings());

        var invoker = LatticeGrpcChannelFactory.CreateCallInvoker(channel, Settings());

        Assert.That(invoker, Is.Not.Null);
    }

    [Test]
    public void CreateCallInvoker_decorates_for_a_credential_provider()
    {
        var settings = Settings() with
        {
            Authentication = LatticeCallAuthentication.Bearer(new FakeCredentialProvider()),
        };
        using var channel = LatticeGrpcChannelFactory.CreateChannel(settings);

        var invoker = LatticeGrpcChannelFactory.CreateCallInvoker(channel, settings);

        // A decorated invoker is an interceptor wrapper, so it is a different
        // type from the channel's own invoker rather than a different instance
        // of it (the channel hands out a fresh instance on every call).
        Assert.That(invoker, Is.Not.TypeOf(channel.CreateCallInvoker().GetType()));
    }

    [Test]
    public void ApplyTransportHeaders_returns_the_invoker_unchanged_when_there_are_none()
    {
        using var channel = LatticeGrpcChannelFactory.CreateChannel(Settings());
        CallInvoker invoker = channel.CreateCallInvoker();

        Assert.Multiple(() =>
        {
            Assert.That(LatticeGrpcChannelFactory.ApplyTransportHeaders(invoker, null), Is.SameAs(invoker));
            Assert.That(
                LatticeGrpcChannelFactory.ApplyTransportHeaders(
                    invoker,
                    new Dictionary<string, string>()),
                Is.SameAs(invoker));
        });
    }

    [Test]
    public void ApplyTransportHeaders_decorates_when_headers_are_supplied()
    {
        using var channel = LatticeGrpcChannelFactory.CreateChannel(Settings());
        CallInvoker invoker = channel.CreateCallInvoker();

        var decorated = LatticeGrpcChannelFactory.ApplyTransportHeaders(
            invoker,
            new Dictionary<string, string> { ["x-azure-fdid"] = "origin" });

        Assert.That(decorated, Is.Not.TypeOf(invoker.GetType()));
    }

    [Test]
    public void ApplyTransportHeaders_rejects_a_null_invoker() =>
        Assert.That(
            () => LatticeGrpcChannelFactory.ApplyTransportHeaders(null!, null),
            Throws.TypeOf<ArgumentNullException>());
}
