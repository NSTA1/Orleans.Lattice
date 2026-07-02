using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Runtime;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Integration coverage that the folded <see cref="ComplianceFoldProjection"/>
/// view actually serves <see cref="LatticeFactBackend.GetStateAsync"/> over the
/// default fact tree, and that its answer matches the inline
/// <see cref="ComplianceFold"/> for the reverse-arrival case. Uses a live
/// TestCluster with the view registered (see
/// <see cref="FederationTestClusterFixture"/>), so it is tagged
/// <c>Integration</c>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ComplianceViewIntegrationTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task View_serves_business_Hlc_ordered_state_for_reversed_arrival()
    {
        var backend = _fixture.NewLatticeBackendOverDefaultTree();
        var serial = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(80000, 89999)}");

        var first = Nc(serial, 10, "NC-A", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var second = Mrb(serial, 20, "NC-A", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);

        // Arrive reversed: the folded view must still order by business HLC.
        await backend.EmitAsync(second);
        await backend.EmitAsync(first);

        // Drive the maintainer until it has applied our writes, so the read hits
        // the materialised row rather than the scan+fold fallback.
        var view = await _fixture.ViewFactory.GetAsync(ComplianceFoldProjection.ViewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        var state = await backend.GetStateAsync(serial);

        Assert.That(state, Is.EqualTo(ComplianceState.Nominal));
    }

    [Test]
    public async Task View_state_matches_inline_fold_for_a_scrap_terminal_sequence()
    {
        var backend = _fixture.NewLatticeBackendOverDefaultTree();
        var serial = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(80000, 89999)}");

        await backend.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Critical, ProcessSite.ToulouseNdtLab));
        // Late UseAsIs must not revive a scrapped part.
        await backend.EmitAsync(Mrb(serial, 2, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb));

        var view = await _fixture.ViewFactory.GetAsync(ComplianceFoldProjection.ViewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        var viewState = await backend.GetStateAsync(serial);
        var inlineState = ComplianceFold.Fold(await backend.GetFactsAsync(serial));

        Assert.Multiple(() =>
        {
            Assert.That(viewState, Is.EqualTo(ComplianceState.Scrap));
            Assert.That(viewState, Is.EqualTo(inlineState));
        });
    }

    [Test]
    public async Task BuildAllParts_joins_view_lattice_state_with_baseline_and_surfaces_divergence()
    {
        // The dashboard snapshot reads the fact-derived half (lattice state,
        // latest stage, fact count) from the folded view over the default tree,
        // and joins BaselineState per part from the baseline backend. Seed a
        // Critical NC into the view's source tree only; the baseline never sees
        // it, so the two halves disagree - exactly the demo's divergence.
        var serial = new PartSerialNumber($"HPT-BLD-S1-2028-{Random.Shared.Next(83000, 84999)}");
        var defaultTree = _fixture.NewLatticeBackendOverDefaultTree();
        await defaultTree.EmitAsync(Nc(serial, tick: 1, "NC-1", NcSeverity.Critical, ProcessSite.ToulouseNdtLab), CancellationToken.None);

        var view = await _fixture.ViewFactory.GetAsync(ComplianceFoldProjection.ViewName);
        Assert.That(view, Is.Not.Null);
        await view!.WaitForSourceHeadAsync(TimeSpan.FromSeconds(30));

        var (router, _, _) = _fixture.NewRouter();
        var streamId = StreamId.Create(DashboardBroadcaster.StreamNamespace, $"broadcast-{Guid.NewGuid():N}");
        await using var broadcaster = new DashboardBroadcaster(
            router,
            _fixture.Cluster.Client,
            _fixture.NewPartCrdtStore(),
            _fixture.ViewFactory,
            NullLogger<DashboardBroadcaster>.Instance,
            streamId);
        await broadcaster.StartAsync(CancellationToken.None);

        var snapshot = await broadcaster.GetInitialPartsAsync();
        var row = snapshot.First(p => p.Serial == serial);

        Assert.Multiple(() =>
        {
            // Lattice half sourced from the folded view.
            Assert.That(row.LatticeState, Is.EqualTo(ComplianceState.Scrap));
            Assert.That(row.LatestStage, Is.EqualTo(ProcessStage.MRB));
            Assert.That(row.FactCount, Is.EqualTo(1));
            // Baseline half joined from the (empty) baseline backend.
            Assert.That(row.BaselineState, Is.EqualTo(ComplianceState.Nominal));
            // The two independently-maintained halves disagree - divergence.
            Assert.That(row.Diverges, Is.True);
        });
    }
}
