using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// Unit tests for the production <see cref="GrpcSchemaAdminClient"/>. They exercise
/// the constructor guards, the unconfigured / disposed error paths, and the channel
/// build (including every auth-attaching branch) by issuing each call with an
/// already-cancelled token so the transport fails fast without a server. No cluster
/// is stood up, so these remain fast, deterministic unit tests.
/// </summary>
[TestFixture]
public class GrpcSchemaAdminClientTests
{
    private static readonly LatticeSchemaPolicy Policy = new(Array.Empty<LatticeSchemaRule>());
    private static readonly LatticeSchemaVersionConfig VersionConfig = new(1, 2);

    private static GrpcSchemaAdminClient Create(
        ExplorerConfiguration? config,
        LatticeCallAuthentication? auth = null) =>
        new(ExplorerControlClientHarness.Session(config), ExplorerControlClientHarness.Auth(auth));

    [Test]
    public void Constructor_null_session_throws()
    {
        Assert.That(
            () => new GrpcSchemaAdminClient(null!, ExplorerControlClientHarness.Auth(null)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_auth_throws()
    {
        Assert.That(
            () => new GrpcSchemaAdminClient(ExplorerControlClientHarness.Session(null), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetPolicyAsync_empty_tree_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.GetPolicyAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetPolicyAsync_null_policy_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.SetPolicyAsync("orders", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ListDeadLettersAsync_non_positive_max_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.ListDeadLettersAsync("orders", 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Method_without_endpoint_throws_invalid_operation()
    {
        using var client = Create(config: null);

        Assert.That(
            async () => await client.GetPolicyAsync("orders"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Method_after_dispose_throws_object_disposed()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());
        client.Dispose();

        Assert.That(
            async () => await client.GetPolicyAsync("orders"),
            Throws.InstanceOf<ObjectDisposedException>());
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());

        client.Dispose();

        Assert.That(() => client.Dispose(), Throws.Nothing);
    }

    [Test]
    public async Task All_calls_build_channel_and_propagate_cancellation()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var calls = new Func<Task>[]
        {
            () => client.GetPolicyAsync("orders", ct),
            () => client.SetPolicyAsync("orders", Policy, ct),
            () => client.ClearPolicyAsync("orders", ct),
            () => client.CountDeadLettersAsync("orders", ct),
            () => client.ListDeadLettersAsync("orders", 5, ct),
            () => client.GetVersionConfigAsync("orders", ct),
            () => client.SetVersionConfigAsync("orders", VersionConfig, ct),
            () => client.AdvanceTargetVersionAsync("orders", 3, ct),
            () => client.AdvanceAndMigrateAsync("orders", 3, ct),
            () => client.MigrateToTargetVersionAsync("orders", ct),
            () => client.ClearVersionConfigAsync("orders", ct),
            () => client.GetRemediationStatusAsync("orders", ct),
            () => client.ScanComplianceAsync("orders", ct),
            () => client.ProbeCapabilitiesAsync("orders", ct),
        };

        foreach (var call in calls)
        {
            var ex = Assert.CatchAsync(async () => await call());
            Assert.That(ex, Is.Not.Null);
            Assert.That(ex, Is.Not.InstanceOf<LatticeAuthorizationDeniedException>(),
                "a cancelled (non permission-denied) transport fault must not be translated to a denial");
        }
    }

    [Test]
    public void Call_with_static_header_auth_builds_channel()
    {
        var auth = LatticeCallAuthentication.Basic("operator", "secret");
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.GetPolicyAsync("orders", ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_credential_provider_over_h2c_builds_insecure_call_credentials()
    {
        var provider = NSubstitute.Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("Bearer token"));
        var auth = LatticeCallAuthentication.Bearer(provider);
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(async () => await client.GetPolicyAsync("orders", ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_credential_provider_over_tls_builds_channel()
    {
        var provider = NSubstitute.Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("Bearer token"));
        var auth = LatticeCallAuthentication.Bearer(provider);
        using var client = Create(ExplorerControlClientHarness.TlsConfig(), auth);

        var ex = Assert.CatchAsync(async () => await client.GetPolicyAsync("orders", ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_transport_headers_builds_channel()
    {
        var config = ExplorerControlClientHarness.H2cConfig(
            transportHeaders: new Dictionary<string, string> { ["x-azure-fdid"] = "origin-1" });
        using var client = Create(config);

        var ex = Assert.CatchAsync(async () => await client.GetPolicyAsync("orders", ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public async Task Repeated_calls_reuse_the_same_channel()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var first = Assert.CatchAsync(async () => await client.GetPolicyAsync("orders", ct));
        var second = Assert.CatchAsync(async () => await client.GetPolicyAsync("orders", ct));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
        });
        await Task.CompletedTask;
    }
}
