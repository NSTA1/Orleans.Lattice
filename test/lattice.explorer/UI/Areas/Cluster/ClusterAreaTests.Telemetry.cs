using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// #5602: a caller the capability probe reports as holding cluster telemetry is
/// admitted without the storage summary, which on a large estate outlasts the
/// directory's availability budget; the summary is read for Home afterwards.
/// </summary>
public sealed partial class ClusterAreaTests
{
    [Test]
    public async Task A_caller_the_probe_reports_holding_cluster_telemetry_is_admitted_without_reading_the_estate()
    {
        _admin.ProbeCapabilitiesAsync(ClusterArea.ProbeTreeId, Arg.Any<CancellationToken>())
            .Returns(ClusterTreeAccess.None(ClusterArea.ProbeTreeId) with { CanViewClusterTelemetry = true });
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary { TreeCount = 2, TotalBytes = 2048 });
        UseTrees("orders", "invoices");
        var area = CreateArea();

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var readsBeforeHome = StorageReads();
        var badgeBeforeHome = await area.GetDirectoryBadgeAsync(CancellationToken.None);
        var status = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(readsBeforeHome, Is.Zero, "the availability check never reads the estate");
            Assert.That(badgeBeforeHome, Is.Null, "nothing is counted before the summary is read");
            Assert.That(status, Is.EqualTo("2 trees, 2.0 KiB stored."));
            Assert.That(badge, Is.EqualTo("2"));
            Assert.That(StorageReads(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_probe_that_does_not_report_telemetry_or_faults_falls_back_to_the_storage_summary()
    {
        _admin.ProbeCapabilitiesAsync(ClusterArea.ProbeTreeId, Arg.Any<CancellationToken>())
            .Returns(
                _ => Task.FromResult(ClusterTreeAccess.None(ClusterArea.ProbeTreeId)),
                _ => throw new NotSupportedException("an older cluster"));
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("denied"));

        var withoutTelemetry = await CreateArea().GetAvailabilityAsync(CancellationToken.None);
        var faulted = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(withoutTelemetry, Is.EqualTo(AreaAvailability.Hidden), "fails closed: the summary is refused");
            Assert.That(faulted, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(StorageReads(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_summary_Home_cannot_read_leaves_the_status_empty_and_the_area_visible()
    {
        _admin.ProbeCapabilitiesAsync(ClusterArea.ProbeTreeId, Arg.Any<CancellationToken>())
            .Returns(ClusterTreeAccess.None(ClusterArea.ProbeTreeId) with { CanViewClusterTelemetry = true });
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        var area = CreateArea();

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var status = await area.GetHomeStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(status, Is.Null);
        });
    }

    private int StorageReads() =>
        _admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetStorageUsageAsync));
}
