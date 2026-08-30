using NSubstitute;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Unit tests for the production <see cref="GrpcTelemetryQueryClient"/>. They
/// exercise the constructor guards, the argument guards, the unconfigured and
/// disposed error paths, and the channel build (including every auth-attaching
/// branch) by issuing each call with an already-cancelled token so the transport
/// fails fast without a server. No cluster is stood up, so these remain fast,
/// deterministic unit tests.
/// </summary>
[TestFixture]
public class GrpcTelemetryQueryClientTests
{
    private static GrpcTelemetryQueryClient Create(
        ExplorerConfiguration? config,
        LatticeCallAuthentication? auth = null) =>
        new(ExplorerControlClientHarness.Session(config), ExplorerControlClientHarness.Auth(auth));

    private static TelemetryQueryRequest Request() => new() { QueryId = SampleTelemetry.RangeQueryId };

    [Test]
    public void Constructor_null_session_throws() =>
        Assert.That(
            () => new GrpcTelemetryQueryClient(null!, ExplorerControlClientHarness.Auth(null)),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_null_auth_throws() =>
        Assert.That(
            () => new GrpcTelemetryQueryClient(ExplorerControlClientHarness.Session(null), null!),
            Throws.ArgumentNullException);

    [Test]
    public void Query_rejects_a_null_request()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.QueryAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void A_call_without_an_endpoint_throws_invalid_operation()
    {
        using var client = Create(config: null);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await client.GetCatalogAsync(), Throws.InstanceOf<InvalidOperationException>());
            Assert.That(
                async () => await client.QueryAsync(Request()),
                Throws.InstanceOf<InvalidOperationException>());
        });
    }

    [Test]
    public void A_call_after_dispose_throws_object_disposed()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());
        client.Dispose();

        Assert.That(async () => await client.GetCatalogAsync(), Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());

        client.Dispose();

        Assert.That(() => client.Dispose(), Throws.Nothing);
    }

    [Test]
    public void Every_call_builds_the_channel_and_leaves_a_non_denial_fault_untranslated()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var calls = new Func<Task>[]
        {
            () => client.GetCatalogAsync(ct),
            () => client.QueryAsync(Request(), ct),
        };

        Assert.That(calls, Has.Length.EqualTo(2), "every facade operation must be covered here");

        foreach (var call in calls)
        {
            var ex = Assert.CatchAsync(async () => await call());
            Assert.That(ex, Is.Not.Null);
            Assert.Multiple(() =>
            {
                Assert.That(
                    ex,
                    Is.Not.InstanceOf<LatticeAuthorizationDeniedException>(),
                    "a cancelled (non permission-denied) transport fault must not be translated to a denial");
                Assert.That(
                    ex,
                    Is.Not.InstanceOf<TelemetryUnavailableException>(),
                    "a cancelled (non unimplemented) transport fault must not be read as an absent capability");
            });
        }
    }

    [Test]
    public void A_call_with_static_header_auth_builds_the_channel()
    {
        var auth = LatticeCallAuthentication.Basic("operator", "secret");
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.GetCatalogAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void A_call_with_a_credential_provider_over_h2c_builds_insecure_call_credentials()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        using var client = Create(
            ExplorerControlClientHarness.H2cConfig(),
            LatticeCallAuthentication.Bearer(provider));

        var ex = Assert.CatchAsync(
            async () => await client.GetCatalogAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void A_call_with_a_credential_provider_over_tls_builds_the_channel()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        using var client = Create(
            ExplorerControlClientHarness.TlsConfig(),
            LatticeCallAuthentication.Bearer(provider));

        var ex = Assert.CatchAsync(
            async () => await client.GetCatalogAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void A_call_with_transport_headers_builds_the_channel()
    {
        var config = ExplorerControlClientHarness.H2cConfig(
            transportHeaders: new Dictionary<string, string> { ["x-azure-fdid"] = "origin-1" });
        using var client = Create(config);

        var ex = Assert.CatchAsync(
            async () => await client.GetCatalogAsync(ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Repeated_calls_reuse_the_one_channel()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var first = Assert.CatchAsync(async () => await client.GetCatalogAsync(ct));
        var second = Assert.CatchAsync(async () => await client.QueryAsync(Request(), ct));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
        });
    }
}
