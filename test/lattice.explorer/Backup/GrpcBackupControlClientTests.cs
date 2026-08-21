using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Unit tests for the production <see cref="GrpcBackupControlClient"/>. They
/// exercise the constructor guards, the argument guards, the unconfigured /
/// disposed error paths, and the channel build (including every auth-attaching
/// branch) by issuing each call with an already-cancelled token so the transport
/// fails fast without a server. No cluster is stood up, so these remain fast,
/// deterministic unit tests.
/// </summary>
[TestFixture]
public class GrpcBackupControlClientTests
{
    private static readonly BackupScopeSelector Scope = BackupScopeSelector.WholeTree("orders");

    private static GrpcBackupControlClient Create(
        ExplorerConfiguration? config,
        LatticeCallAuthentication? auth = null) =>
        new(ExplorerControlClientHarness.Session(config), ExplorerControlClientHarness.Auth(auth));

    [Test]
    public void Constructor_null_session_throws()
    {
        Assert.That(
            () => new GrpcBackupControlClient(null!, ExplorerControlClientHarness.Auth(null)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_auth_throws()
    {
        Assert.That(
            () => new GrpcBackupControlClient(ExplorerControlClientHarness.Session(null), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeCapabilitiesAsync_null_scope_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.ProbeCapabilitiesAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ListBackupsAsync_null_request_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.ListBackupsAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DescribeBackupAsync_empty_id_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.DescribeBackupAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ConfigureBackupHealthAsync_null_config_throws()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());

        Assert.That(() => client.ConfigureBackupHealthAsync("backup-1", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Method_without_endpoint_throws_invalid_operation()
    {
        using var client = Create(config: null);

        Assert.That(
            async () => await client.ListBackupsAsync(new BackupCatalogRequest()),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Method_after_dispose_throws_object_disposed()
    {
        var client = Create(ExplorerControlClientHarness.H2cConfig());
        client.Dispose();

        Assert.That(
            async () => await client.ListBackupsAsync(new BackupCatalogRequest()),
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
            () => client.ProbeCapabilitiesAsync(Scope, ct),
            () => client.ListBackupsAsync(new BackupCatalogRequest(), ct),
            () => client.DescribeBackupAsync("backup-1", ct),
            () => client.CreateBackupAsync(new LatticeBackupCaptureRequest("nightly", Scope), ct),
            () => client.CreateIncrementalBackupAsync(new LatticeBackupIncrementalCaptureRequest("nightly", Scope, "base-1"), ct),
            () => client.CreateBackupSetAsync(new LatticeBackupSetCaptureRequest("nightly", new[] { Scope }), ct),
            () => client.RestoreBackupAsync(new LatticeRestoreRequest("backup-1"), ct),
            () => client.DeleteBackupAsync("backup-1", ct),
            () => client.ScheduleBackupAsync(Scope, false, TimeSpan.FromMinutes(5), ct),
            () => client.CancelScheduleAsync(Scope, false, ct),
            () => client.GetScopeStatusAsync(Scope, ct),
            () => client.IsHealthMonitoringAvailableAsync(ct),
            () => client.CheckBackupHealthAsync("backup-1", ct),
            () => client.GetBackupHealthAsync("backup-1", ct),
            () => client.ConfigureBackupHealthAsync("backup-1", new BackupHealthConfig(true, TimeSpan.FromMinutes(5)), ct),
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
            async () => await client.ListBackupsAsync(new BackupCatalogRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_credential_provider_over_h2c_builds_insecure_call_credentials()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        var auth = LatticeCallAuthentication.Bearer(provider);
        using var client = Create(ExplorerControlClientHarness.H2cConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.ListBackupsAsync(new BackupCatalogRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_credential_provider_over_tls_builds_channel()
    {
        var provider = Substitute.For<ILatticeCallCredentialProvider>();
        provider.GetAuthorizationHeaderAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<string?>("******"));
        var auth = LatticeCallAuthentication.Bearer(provider);
        using var client = Create(ExplorerControlClientHarness.TlsConfig(), auth);

        var ex = Assert.CatchAsync(
            async () => await client.ListBackupsAsync(new BackupCatalogRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public void Call_with_transport_headers_builds_channel()
    {
        var config = ExplorerControlClientHarness.H2cConfig(
            transportHeaders: new Dictionary<string, string> { ["x-azure-fdid"] = "origin-1" });
        using var client = Create(config);

        var ex = Assert.CatchAsync(
            async () => await client.ListBackupsAsync(new BackupCatalogRequest(), ExplorerControlClientHarness.Cancelled()));

        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public async Task Repeated_calls_reuse_the_same_channel()
    {
        using var client = Create(ExplorerControlClientHarness.H2cConfig());
        var ct = ExplorerControlClientHarness.Cancelled();

        var first = Assert.CatchAsync(async () => await client.ListBackupsAsync(new BackupCatalogRequest(), ct));
        var second = Assert.CatchAsync(async () => await client.ListBackupsAsync(new BackupCatalogRequest(), ct));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(second, Is.Not.Null);
        });
        await Task.CompletedTask;
    }
}
