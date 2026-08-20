using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Backup;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeBackupApiGrpcClient"/> that drives each
/// unary RPC over a <see cref="UnaryResponseCallInvoker"/> - a synchronous
/// in-process call invoker returning a canned response - so the client's
/// request-shaping and response-unwrapping logic is exercised deterministically
/// without a live gRPC server. The transport-level round trips are covered
/// separately by the integration E2E suite.
/// </summary>
[TestFixture]
public sealed class LatticeBackupApiGrpcClientUnitTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeBackupApiGrpcClient ClientReturning(object response) =>
        LatticeBackupApiGrpcClient.Create(new UnaryResponseCallInvoker(response), _services);

    [Test]
    public async Task IsHealthMonitoringAvailableAsync_returns_the_availability_flag_from_the_response()
    {
        var client = ClientReturning(new BackupHealthAvailabilityResponse { Available = true });

        var available = await client.IsHealthMonitoringAvailableAsync();

        Assert.That(available, Is.True);
    }

    [Test]
    public async Task CheckBackupHealthAsync_returns_the_report_from_the_response()
    {
        var report = new BackupHealthReport(
            "b1",
            BackupHealthStatus.Healthy,
            manifestPresent: true,
            Array.Empty<string>(),
            Array.Empty<string>(),
            DateTimeOffset.UtcNow,
            "ok");
        var client = ClientReturning(new BackupHealthReportResponse { Found = true, Report = report });

        var result = await client.CheckBackupHealthAsync("b1");

        Assert.That(result, Is.SameAs(report));
    }

    [Test]
    public void CheckBackupHealthAsync_empty_backupId_throws()
    {
        var client = ClientReturning(new BackupHealthReportResponse());

        Assert.That(async () => await client.CheckBackupHealthAsync(string.Empty), Throws.ArgumentException);
    }

    [Test]
    public async Task GetBackupHealthAsync_returns_the_report_when_found()
    {
        var report = new BackupHealthReport(
            "b2",
            BackupHealthStatus.Warning,
            manifestPresent: true,
            new[] { "art-1" },
            Array.Empty<string>(),
            DateTimeOffset.UtcNow,
            "one artifact missing");
        var client = ClientReturning(new BackupHealthReportResponse { Found = true, Report = report });

        var result = await client.GetBackupHealthAsync("b2");

        Assert.That(result, Is.SameAs(report));
    }

    [Test]
    public async Task GetBackupHealthAsync_returns_null_when_not_found()
    {
        var client = ClientReturning(new BackupHealthReportResponse { Found = false });

        var result = await client.GetBackupHealthAsync("missing");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task ConfigureBackupHealthAsync_sends_the_config_and_completes()
    {
        var invoker = new UnaryResponseCallInvoker(new BackupHealthConfigureResponse());
        var client = LatticeBackupApiGrpcClient.Create(invoker, _services);

        await client.ConfigureBackupHealthAsync("b3", new BackupHealthConfig(true, TimeSpan.FromHours(6)));

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<BackupHealthConfigureRequestMessage>());
            var message = (BackupHealthConfigureRequestMessage)invoker.LastRequest!;
            Assert.That(message.BackupId, Is.EqualTo("b3"));
            Assert.That(message.MonitoringEnabled, Is.True);
            Assert.That(message.IntervalTicks, Is.EqualTo(TimeSpan.FromHours(6).Ticks));
        });
    }

    [Test]
    public void ConfigureBackupHealthAsync_null_config_throws()
    {
        var client = ClientReturning(new BackupHealthConfigureResponse());

        Assert.That(
            async () => await client.ConfigureBackupHealthAsync("b3", null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CancelScheduleAsync_sends_the_scope_and_completes()
    {
        var invoker = new UnaryResponseCallInvoker(new BackupCancelScheduleResponse());
        var client = LatticeBackupApiGrpcClient.Create(invoker, _services);
        var scope = BackupScopeSelector.WholeTree("orders");

        await client.CancelScheduleAsync(scope, incremental: true);

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.InstanceOf<BackupCancelScheduleRequestMessage>());
            var message = (BackupCancelScheduleRequestMessage)invoker.LastRequest!;
            Assert.That(message.Scope, Is.EqualTo(scope));
            Assert.That(message.Incremental, Is.True);
        });
    }

    [Test]
    public void CancelScheduleAsync_null_scope_throws()
    {
        var client = ClientReturning(new BackupCancelScheduleResponse());

        Assert.That(
            async () => await client.CancelScheduleAsync(null!, incremental: false),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetAuthSchemeAsync_returns_the_advertisement_from_the_response()
    {
        var advertisement = new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "bearer" } },
        };
        var client = ClientReturning(advertisement);

        var result = await client.GetAuthSchemeAsync(new AuthSchemeAdvertisementRequest());

        Assert.That(result.Schemes, Has.Count.EqualTo(1));
        Assert.That(result.Schemes[0].SchemeId, Is.EqualTo("bearer"));
    }

    [Test]
    public async Task GetScopeStatusAsync_returns_null_when_the_scope_is_unknown()
    {
        var client = ClientReturning(new BackupScopeStatusResponse { Found = false });

        var status = await client.GetScopeStatusAsync(BackupScopeSelector.WholeTree("orders"));

        Assert.That(status, Is.Null);
    }
}
