using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regressions for a cross-tree atomic batch whose registered slice goes stale
/// while the joint decision is pending. Registering stores this view's slice at
/// the coordinator, and the participant that registers last flips every stored
/// slice. When this view discards the staged batch (the staging backstop) and
/// rebuilds from source, or an ordinary write supersedes a key of the stored
/// slice, a later joint flip must not land the stale slice over the newer value.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ViewPerson(int Age, string Tag);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static LatticePredicateNode AdultFilter() =>
        LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18);

    private void CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        _ = factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private async Task<IViewMaintainerGrain> MaintainerAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        return maintainer;
    }

    private async Task<CrossTreeAtomicWriteOutcome> CrossTreeWriteAsync(
        string treeA, string keyA, byte[] valueA,
        string treeB, string keyB, byte[] valueB,
        string operationId)
        => await _fixture.Cluster.Client.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new KeyValuePair<string, byte[]>(keyA, valueA)]),
                new LatticeTreeBatch(treeB, [new KeyValuePair<string, byte[]>(keyB, valueB)]),
            ],
            operationId);

    [Test]
    public Task A_backstop_rebuild_is_not_overwritten_by_a_later_joint_flip_of_the_discarded_slice()
        => BackstopRebuildSurvivesLateJointFlipAsync(MaterialisedViewClusterFixture.CrossTreeBackstopViewName, "mv-xt-backstop-a-src");

    [Test]
    public Task An_in_place_backstop_rebuild_is_not_overwritten_by_a_later_joint_flip_of_the_discarded_slice()
        => BackstopRebuildSurvivesLateJointFlipAsync(MaterialisedViewClusterFixture.CrossTreeBackstopShipViewName, "mv-xt-backstop-ship-a-src");

    private async Task BackstopRebuildSurvivesLateJointFlipAsync(string viewA, string treeA)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeB = $"mv-xt-backstop-b-src-{suffix}";
        var viewB = $"mv-xt-backstop-b-view-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);

        // Seed each source before activation so a ShipView view designates itself producer.
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("seed", Person(5, "seed"));
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeB).SetAsync("seed", Person(5, "seed"));
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        // View A stages op1, registers its slice {ka = a1} and waits for view B.
        Assert.That(
            await CrossTreeWriteAsync(treeA, "ka", Person(30, "a1"), treeB, "kb", Person(40, "b1"), $"mv-xt-backstop-1-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();

        // An ordinary write supersedes ka, and a second cross-tree batch stages
        // beside op1 so the next drain trips the backstop (MaxStagedTransactions = 1),
        // which discards both and rebuilds view A from source (ka = a2).
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", Person(50, "a2"));
        Assert.That(
            await CrossTreeWriteAsync(treeA, "ka2", Person(31, "a3"), treeB, "kb2", Person(41, "b2"), $"mv-xt-backstop-2-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();

        var viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")), "The backstop rebuild reads current source state.");

        // View B now registers op1: the coordinator must not land view A's
        // discarded registration {ka = a1} over the rebuilt value.
        for (var i = 0; i < 3; i++)
        {
            await maintainerB.DrainAsync();
            await maintainerA.DrainAsync();
        }

        viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")),
                "A joint flip of the slice the backstop discarded must not overwrite the newer source value.");
            Assert.That(await viewTreeA.GetAsync("ka2"), Is.EqualTo(Person(31, "a3")));
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
        });
    }

    [Test]
    public Task A_failed_degrade_of_a_discarded_slice_is_retried_by_the_next_drain_before_any_rebuild_publishes()
        => FailedDegradeIsRetriedAsync(MaterialisedViewClusterFixture.CrossTreeBackstopFaultViewName, "mv-xt-backstop-fault-a-src", reactivate: false, rebuild: false);

    [Test]
    public Task A_failed_degrade_of_a_discarded_slice_is_retried_after_the_maintainer_reactivates()
        => FailedDegradeIsRetriedAsync(MaterialisedViewClusterFixture.CrossTreeBackstopReactivationViewName, "mv-xt-backstop-react-a-src", reactivate: true, rebuild: false);

    [Test]
    public Task A_rebuild_after_a_failed_degrade_degrades_the_discarded_slice_before_it_publishes()
        => FailedDegradeIsRetriedAsync(MaterialisedViewClusterFixture.CrossTreeBackstopFaultRebuildViewName, "mv-xt-backstop-fault-rb-a-src", reactivate: false, rebuild: true);

    [Test]
    public Task A_rebuild_after_a_failed_degrade_and_reactivation_degrades_the_discarded_slice_before_it_publishes()
        => FailedDegradeIsRetriedAsync(MaterialisedViewClusterFixture.CrossTreeBackstopReactivationRebuildViewName, "mv-xt-backstop-react-rb-a-src", reactivate: true, rebuild: true);

    // rebuild: a rebuild other than the backstop's own (an operator rebuild here; a
    // fall-off-log, lag-eviction or range-reconcile rebuild takes the same path)
    // runs before any drain re-trips the backstop.
    private async Task FailedDegradeIsRetriedAsync(string viewA, string treeA, bool reactivate, bool rebuild)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeB = $"mv-xt-backstop-fault-b-src-{suffix}";
        var viewB = $"mv-xt-backstop-fault-b-view-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("seed", Person(5, "seed"));
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeB).SetAsync("seed", Person(5, "seed"));
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        // View A stages op1 and registers its slice {ka = a1}; view B has not registered.
        Assert.That(
            await CrossTreeWriteAsync(treeA, "ka", Person(30, "a1"), treeB, "kb", Person(40, "b1"), $"mv-xt-fault-1-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();
        var treeBeforeBackstop = await maintainerA.GetActiveTreeIdAsync();

        // An ordinary write supersedes ka and a second cross-tree batch trips the
        // backstop, but the coordinator fails the degrade of op1's discarded slice.
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", Person(50, "a2"));
        Assert.That(
            await CrossTreeWriteAsync(treeA, "ka2", Person(31, "a3"), treeB, "kb2", Person(41, "b2"), $"mv-xt-fault-2-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        var callsBefore = CrossTreeDegradeFaultFilter.CallCount(viewA);
        CrossTreeDegradeFaultFilter.FailNext(viewA, 1);
        Assert.That(async () => await maintainerA.DrainAsync(), Throws.Exception, "The injected degrade failure surfaces from the backstop drain.");
        Assert.That(await maintainerA.GetActiveTreeIdAsync(), Is.EqualTo(treeBeforeBackstop),
            "No rebuild may publish before the discarded slice is terminally degraded.");

        if (reactivate)
        {
            await maintainerA.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
            await Task.Delay(500);
            maintainerA = await MaintainerAsync(viewA);
        }

        // The next drain (or rebuild) retries the degrade before it rebuilds.
        if (rebuild)
        {
            await maintainerA.RebuildAsync();
        }
        else
        {
            await maintainerA.DrainAsync();
        }

        var callsAfterRetry = CrossTreeDegradeFaultFilter.CallCount(viewA);

        // View B now registers op1: its joint flip must not land the discarded slice.
        for (var i = 0; i < 3; i++)
        {
            await maintainerB.DrainAsync();
            await maintainerA.DrainAsync();
        }

        var viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")),
                "A joint flip of the slice the backstop discarded must not overwrite the newer source value.");
            Assert.That(await viewTreeA.GetAsync("ka2"), Is.EqualTo(Person(31, "a3")));
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
            Assert.That(callsAfterRetry, Is.GreaterThanOrEqualTo(callsBefore + 2),
                "The failed degrade must be retried before the drain or rebuild publishes.");
        });
    }

    [Test]
    public async Task An_in_place_rebuild_after_reactivation_is_not_overwritten_by_a_joint_flip_of_a_slice_registered_by_the_previous_activation()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = MaterialisedViewClusterFixture.CrossTreePriorRegistrationSourceTreeId;
        var treeB = $"mv-xt-prior-reg-b-src-{suffix}";
        var viewA = MaterialisedViewClusterFixture.CrossTreePriorRegistrationViewName;
        var viewB = $"mv-xt-prior-reg-b-view-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);

        // Seed each source before activation so the ShipView view designates itself producer.
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("seed", Person(5, "seed"));
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeB).SetAsync("seed", Person(5, "seed"));
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        // View A stages op1 and registers its slice {ka = a1, kc0..kc8}; view B has
        // not registered. The ten source-A keys span the source's WAL partitions, so
        // at least one partition carries two of op1's prepares.
        CrossTreeRegisterReadyHookFilter.BeforeNextRegistration(viewA, _ => Task.CompletedTask);
        Assert.That(
            await _fixture.Cluster.Client.SetManyAtomicAsync(
                [
                    new LatticeTreeBatch(treeA, [new KeyValuePair<string, byte[]>("ka", Person(30, "a1")), .. Enumerable.Range(0, 9).Select(i => new KeyValuePair<string, byte[]>($"kc{i}", Person(31, "c1")))]),
                    new LatticeTreeBatch(treeB, [new KeyValuePair<string, byte[]>("kb", Person(40, "b1"))]),
                ],
                $"mv-xt-prior-reg-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        for (var i = 0; i < 6; i++)
        {
            await maintainerA.DrainAsync();
        }

        Assert.That(CrossTreeRegisterReadyHookFilter.IsArmed(viewA), Is.False, "View A registered op1 before reactivation.");

        // A later ordinary write is the source last-writer for ka. View A loses its
        // in-memory staging (reactivation) and its one-entry-per-partition drain
        // re-reads only part of op1 after the source WAL retention (2s) has elapsed:
        // the retention backstop abandons the incomplete op1 (whose operation id it
        // may not even have read) and rebuilds from source (ka = a2), retiring the
        // decided op1 without replaying it, so view A never re-registers. The slice
        // the previous activation registered is still stored at the coordinator.
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", Person(50, "a2"));
        await maintainerA.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        await Task.Delay(2500);
        maintainerA = await MaintainerAsync(viewA);
        for (var i = 0; i < 4; i++)
        {
            await maintainerA.DrainAsync();
        }

        var viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")), "The rebuild reads current source state.");

        // View B now registers op1: the coordinator must not land view A's
        // pre-rebuild registration {ka = a1} over the rebuilt value.
        for (var i = 0; i < 3; i++)
        {
            await maintainerB.DrainAsync();
            await maintainerA.DrainAsync();
        }

        viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        var viewTreeB = await _fixture.ActiveViewTreeAsync(viewB);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")),
                "A joint flip of a slice registered before the rebuild must not overwrite the rebuilt value.");
            Assert.That(await viewTreeB.GetAsync("kb"), Is.EqualTo(Person(40, "b1")));
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task A_joint_flip_of_a_slice_registered_before_an_ordinary_supersession_does_not_lose_the_ordinary_write()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-superseded-a-src-{suffix}";
        var treeB = $"mv-xt-superseded-b-src-{suffix}";
        var viewA = $"mv-xt-superseded-a-view-{suffix}";
        var viewB = $"mv-xt-superseded-b-view-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        // View A stages op1 and registers its slice {ka = a1}; view B has not registered.
        Assert.That(
            await CrossTreeWriteAsync(treeA, "ka", Person(30, "a1"), treeB, "kb", Person(40, "b1"), $"mv-xt-superseded-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();

        // A later ordinary write is the source last-writer for ka. View A's next
        // drain applies it and re-registers op1 without ka, but view B completes the
        // wait set first, so the coordinator flips the slice view A registered
        // before the supersession: {ka = a1}.
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", Person(50, "a2"));
        CrossTreeRegisterReadyHookFilter.BeforeNextRegistration(
            viewA,
            grains => grains.GetGrain<IViewMaintainerGrain>(viewB).DrainAsync());
        await maintainerA.DrainAsync();
        Assert.That(CrossTreeRegisterReadyHookFilter.IsArmed(viewA), Is.False, "View A re-registered op1 after applying the ordinary write.");

        for (var i = 0; i < 2; i++)
        {
            await maintainerB.DrainAsync();
            await maintainerA.DrainAsync();
        }

        var viewTreeA = await _fixture.ActiveViewTreeAsync(viewA);
        var viewTreeB = await _fixture.ActiveViewTreeAsync(viewB);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTreeA.GetAsync("ka"), Is.EqualTo(Person(50, "a2")),
                "The joint flip of the pre-supersession slice must not leave the older atomic value over the ordinary write.");
            Assert.That(await viewTreeB.GetAsync("kb"), Is.EqualTo(Person(40, "b1")));
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
        });
    }

}
