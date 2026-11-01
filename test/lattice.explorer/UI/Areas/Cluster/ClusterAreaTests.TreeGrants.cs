using Grpc.Core;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// #5581: a caller refused the cluster-wide storage summary is admitted to the area
/// only by a tree-lifecycle or tree-admin grant on a tree the catalogue lists, never
/// by a read, diagnostics or bulk-load grant.
/// </summary>
public sealed partial class ClusterAreaTests
{
    [Test]
    public async Task A_caller_refused_the_summary_who_may_manage_a_trees_lifecycle_sees_it()
    {
        RefuseSummary();
        UseTrees("factory-orders", "factory-orders-archive-2026-10");
        Grant("factory-orders-archive-2026-10", caps => caps with { CanManageTreeLifecycle = true });
        var area = CreateArea();

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var status = await area.GetHomeStatusAsync(CancellationToken.None);
        var badge = await area.GetDirectoryBadgeAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(status, Is.Null, "the estate-wide figures stay out of reach");
            Assert.That(badge, Is.Null, "and are not counted");
        });
    }

    [Test]
    public async Task A_caller_refused_the_summary_who_may_administer_a_tree_sees_it()
    {
        RefuseSummary();
        UseTrees("orders");
        Grant("orders", caps => caps with { CanAdministerTree = true });
        var area = CreateArea();

        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
    }

    [Test]
    public async Task A_caller_refused_the_summary_who_may_only_read_view_diagnostics_or_bulk_load_does_not_see_it()
    {
        RefuseSummary();
        UseTrees("orders", "invoices");
        Grant("orders", caps => caps with { CanViewDiagnostics = true, CanBulkLoad = true, CanRestore = true });
        Grant("invoices", caps => caps with { CanViewDiagnostics = true });
        var area = CreateArea();

        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_refused_tree_probe_or_an_unreadable_catalogue_admits_nothing()
    {
        RefuseSummary();
        UseTrees("orders");
        _admin.ProbeCapabilitiesAsync("orders", Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("denied"));
        var refusedProbe = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        _session.Connection.ListTreesAsync(Arg.Any<Orleans.Lattice.Api.State.CatalogRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new UnauthorizedAccessException("denied"));
        var refusedCatalogue = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(refusedProbe, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(refusedCatalogue, Is.EqualTo(AreaAvailability.Hidden));
        });
    }

    [Test]
    public async Task A_tree_probe_the_cluster_did_not_answer_is_not_remembered()
    {
        RefuseSummary();
        UseTrees("orders");
        _admin.ProbeCapabilitiesAsync("orders", Arg.Any<CancellationToken>())
            .Returns(
                _ => throw new RpcException(new Status(StatusCode.Unavailable, "down")),
                _ => Task.FromResult(ClusterTreeAccess.None("orders") with { CanManageTreeLifecycle = true }));
        var area = CreateArea();

        var first = await area.GetAvailabilityAsync(CancellationToken.None);
        var second = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(second, Is.EqualTo(AreaAvailability.Visible));
        });
    }

    [Test]
    public async Task The_fallback_probes_at_most_its_bound_and_stops_at_the_first_grant()
    {
        RefuseSummary();
        var ids = Enumerable.Range(0, ClusterArea.MaximumGrantProbes + 10).Select(i => $"tree-{i:D3}").ToArray();
        UseTrees(ids);
        _admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(ClusterTreeAccess.None(call.Arg<string>())));
        Grant(ids[^1], caps => caps with { CanManageTreeLifecycle = true });
        var bounded = await CreateArea().GetAvailabilityAsync(CancellationToken.None);
        var boundedProbes = _admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.ProbeCapabilitiesAsync));

        _admin.ClearReceivedCalls();
        Grant(ids[1], caps => caps with { CanAdministerTree = true });
        var early = await CreateArea().GetAvailabilityAsync(CancellationToken.None);
        var earlyProbes = _admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.ProbeCapabilitiesAsync));

        Assert.Multiple(() =>
        {
            Assert.That(bounded, Is.EqualTo(AreaAvailability.Hidden), "a grant past the bound is never read");
            Assert.That(boundedProbes, Is.EqualTo(ClusterArea.MaximumGrantProbes));
            Assert.That(early, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(earlyProbes, Is.EqualTo(ClusterArea.GrantProbeConcurrency), "the first wave holds the grant, so no second wave is probed");
        });
    }

    [Test]
    public async Task The_fallback_probes_a_wave_of_trees_at_once()
    {
        // One probe after another could not answer inside the directory's
        // availability budget for a reader on a cluster of a few dozen trees (U4).
        RefuseSummary();
        var ids = Enumerable.Range(0, ClusterArea.GrantProbeConcurrency).Select(i => $"tree-{i:D3}").ToArray();
        UseTrees(ids);
        var started = 0;
        var allStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                if (Interlocked.Increment(ref started) == ids.Length)
                {
                    allStarted.SetResult();
                }

                await allStarted.Task;
                return ClusterTreeAccess.None(call.Arg<string>());
            });

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public void Only_a_lifecycle_or_admin_grant_admits_the_area()
    {
        var none = ClusterTreeAccess.None("orders");

        Assert.Multiple(() =>
        {
            Assert.That(ClusterArea.AdmitsArea(none), Is.False);
            Assert.That(ClusterArea.AdmitsArea(none with { CanViewDiagnostics = true, CanBulkLoad = true, CanRestore = true }), Is.False);
            Assert.That(ClusterArea.AdmitsArea(none with { CanManageTreeLifecycle = true }), Is.True);
            Assert.That(ClusterArea.AdmitsArea(none with { CanAdministerTree = true }), Is.True);
        });
    }

    private void RefuseSummary() =>
        _admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("denied"));

    private void Grant(string treeId, Func<LatticeTreeAdminCapabilities, LatticeTreeAdminCapabilities> grant) =>
        _admin.ProbeCapabilitiesAsync(treeId, Arg.Any<CancellationToken>()).Returns(Task.FromResult(grant(ClusterTreeAccess.None(treeId))));
}
