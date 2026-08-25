using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantResidencyWarmupHostedService"/>, the start-up
/// background service that warms the residency snapshot once. The happy path (the
/// registry is reachable immediately) completes on the first
/// <see cref="TenantResidencySnapshotMaintainer.EnsureWarmAsync"/> and needs no
/// delay, so it is driven with no timing dependency; the retry cadence itself is
/// not exercised by wall-clock here.
/// </summary>
[TestFixture]
public sealed class TenantResidencyWarmupHostedServiceTests
{
    private static async IAsyncEnumerable<TenantRecord> Empty(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    private static TenantResidencySnapshotMaintainer Maintainer(ITenantRegistry registry) =>
        new(
            registry,
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            Array.Empty<ITenantRegionStatusChangeListener>(),
            NullLogger<TenantResidencySnapshotMaintainer>.Instance);

    private static TenantResidencyWarmupHostedService Service(TenantResidencySnapshotMaintainer maintainer) =>
        new(maintainer, TimeProvider.System, NullLogger<TenantResidencyWarmupHostedService>.Instance);

    [Test]
    public void Ctor_null_maintainer_throws() =>
        Assert.That(
            () => new TenantResidencyWarmupHostedService(
                null!, TimeProvider.System, NullLogger<TenantResidencyWarmupHostedService>.Instance),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_time_provider_throws()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());

        Assert.That(
            () => new TenantResidencyWarmupHostedService(
                maintainer, null!, NullLogger<TenantResidencyWarmupHostedService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_logger_throws()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());

        Assert.That(
            () => new TenantResidencyWarmupHostedService(maintainer, TimeProvider.System, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task StartAsync_warms_the_maintainer_once()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Empty());
        var maintainer = Maintainer(registry);
        var service = Service(maintainer);

        await service.StartAsync(CancellationToken.None);
        // Await the fire-and-forget warm loop to completion deterministically.
        await service.StopAsync(CancellationToken.None);

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public async Task StopAsync_before_start_is_a_no_op()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());
        var service = Service(maintainer);

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing);
        await Task.CompletedTask;
    }
}
