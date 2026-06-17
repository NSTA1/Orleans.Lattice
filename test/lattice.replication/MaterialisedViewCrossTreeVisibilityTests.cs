using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Phase 6 cross-tree atomic visibility for materialised views. Two source trees
/// are written through the real cross-tree atomic-write primitive
/// (<see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>), which
/// stamps the cross-tree coupling fields on each tree's WAL terminals. The two
/// views derived from those source trees are asserted to flip <b>jointly</b>:
/// <list type="bullet">
///   <item><description>one view's slice is never surfaced while the other view's slice is still pre-commit;</description></item>
///   <item><description>a committed cross-tree batch becomes visible in both views;</description></item>
///   <item><description>an aborted (guard-miss) cross-tree batch surfaces in neither view;</description></item>
///   <item><description>when a participant view never becomes ready, the present view degrades to per-tree-slice atomicity, emits the joint-atomicity-violation metric, and still converges;</description></item>
///   <item><description>redelivery / replay after the joint flip is idempotent (no double-apply).</description></item>
/// </list>
/// Convergence is driven through each maintainer's <c>DrainAsync</c> so assertions
/// are deterministic rather than timer-dependent.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewCrossTreeVisibilityTests
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

    private ILatticeView CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private async Task<IViewMaintainerGrain> MaintainerAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        return maintainer;
    }

    private Task<ILattice> ViewTreeAsync(string viewName) => _fixture.ActiveViewTreeAsync(viewName);

    private static async Task DrainAsync(IViewMaintainerGrain maintainer, int times)
    {
        for (var i = 0; i < times; i++)
        {
            await maintainer.DrainAsync();
        }
    }

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail("View did not catch up to the source head.");
    }

    /// <summary>Drives a two-tree cross-tree atomic write of one adult per tree.</summary>
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
    public async Task One_views_slice_is_not_surfaced_while_the_other_is_pre_commit()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-joint-a-src-{suffix}";
        var treeB = $"mv-xt-joint-b-src-{suffix}";
        var viewA = $"mv-xt-joint-a-view-{suffix}";
        var viewB = $"mv-xt-joint-b-view-{suffix}";
        _ = CreateAdultView(treeA, viewA);
        _ = CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        var outcome = await CrossTreeWriteAsync(
            treeA, "ka", Person(30, "a1"),
            treeB, "kb", Person(40, "b1"),
            $"mv-xt-joint-{suffix}");
        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));

        // Drain only view A's maintainer: it stages, registers its readiness with
        // the coordinator and waits. The coordinator gates on view B too, so view
        // A must NOT surface its slice yet (joint all-or-nothing).
        await DrainAsync(maintainerA, times: 3);

        var treeViewA = await ViewTreeAsync(viewA);
        var treeViewB = await ViewTreeAsync(viewB);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await treeViewA.GetAsync("ka"), Is.Null,
                "View A must not flip its slice while view B is still pre-commit.");
            Assert.That(await treeViewB.GetAsync("kb"), Is.Null,
                "View B has not drained, so its slice is pre-commit.");
            Assert.That(await maintainerA.GetLagAsync(), Is.GreaterThan(0),
                "View A holds the checkpoint back while the joint decision is pending.");
        });

        // Now drain view B: the wait set completes and the coordinator flips both
        // participant view trees jointly.
        await DrainAsync(maintainerB, times: 3);

        treeViewA = await ViewTreeAsync(viewA);
        treeViewB = await ViewTreeAsync(viewB);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await treeViewA.GetAsync("ka"), Is.EqualTo(Person(30, "a1")),
                "Once the wait set completes, view A's slice flips jointly.");
            Assert.That(await treeViewB.GetAsync("kb"), Is.EqualTo(Person(40, "b1")),
                "View B's slice flips together with view A's.");
        });
    }

    [Test]
    public async Task Committed_cross_tree_batch_becomes_visible_in_both_views()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-commit-a-src-{suffix}";
        var treeB = $"mv-xt-commit-b-src-{suffix}";
        var viewA = $"mv-xt-commit-a-view-{suffix}";
        var viewB = $"mv-xt-commit-b-view-{suffix}";
        _ = CreateAdultView(treeA, viewA);
        _ = CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        await CrossTreeWriteAsync(
            treeA, "ca", Person(25, "a1"),
            treeB, "cb", Person(55, "b1"),
            $"mv-xt-commit-{suffix}");

        // Drain both maintainers to convergence: the coordinator flips both view
        // trees jointly and both maintainers then advance their checkpoints.
        for (var round = 0; round < 10; round++)
        {
            await maintainerA.DrainAsync();
            await maintainerB.DrainAsync();
            if (await maintainerA.GetLagAsync() == 0 && await maintainerB.GetLagAsync() == 0)
            {
                break;
            }

            await Task.Delay(20);
        }

        var treeViewA = await ViewTreeAsync(viewA);
        var treeViewB = await ViewTreeAsync(viewB);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await treeViewA.GetAsync("ca"), Is.EqualTo(Person(25, "a1")));
            Assert.That(await treeViewB.GetAsync("cb"), Is.EqualTo(Person(55, "b1")));
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
            Assert.That(await maintainerB.GetLagAsync(), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Aborted_cross_tree_batch_surfaces_in_neither_view()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-abort-a-src-{suffix}";
        var treeB = $"mv-xt-abort-b-src-{suffix}";
        var viewA = $"mv-xt-abort-a-view-{suffix}";
        var viewB = $"mv-xt-abort-b-view-{suffix}";
        _ = CreateAdultView(treeA, viewA);
        _ = CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        // Seed tree B's guarded key with a value that fails the guard, so the
        // cross-tree write aborts and commits nothing in either tree.
        var treeBGrain = _fixture.Cluster.Client.GetGrain<ILattice>(treeB);
        await treeBGrain.SetAsync("guarded", Person(70, "seed"));

        var outcome = await _fixture.Cluster.Client.BeginAtomicWrite($"mv-xt-abort-{suffix}")
            .ForTree(treeA).Set("aa", Person(30, "a1"))
            .ForTree(treeB).SetWhere("guarded", new ViewPerson(80, "new"), p => p.Age >= 200)
            .CommitAsync();
        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.PreconditionFailed));

        await DrainToZeroAsync(maintainerA);
        await DrainToZeroAsync(maintainerB);

        var treeViewA = await ViewTreeAsync(viewA);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await treeViewA.GetAsync("aa"), Is.Null,
                "An aborted cross-tree batch must never surface in view A.");
        });
    }

    [Test]
    public async Task Participant_unavailable_degrades_to_per_tree_slice_and_emits_the_metric()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-degrade-a-src-{suffix}";
        var treeB = $"mv-xt-degrade-b-src-{suffix}";
        var viewA = MaterialisedViewClusterFixture.CrossTreeDegradeViewName;
        var viewB = $"mv-xt-degrade-b-view-{suffix}";

        // Both views are registered (so the wait set is {A, B}), but view B's
        // maintainer is never drained - it models a permanently unavailable
        // participant. View A has a tiny readiness timeout.
        _ = CreateAdultView(treeA, viewA);
        _ = CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);

        using var degradeMetric = new MeterCollector<long>(
            LatticeMetrics.MeterName, "orleans.lattice.view.cross_tree_joint_violation");

        await CrossTreeWriteAsync(
            treeA, "da", Person(30, "a1"),
            treeB, "db", Person(40, "b1"),
            $"mv-xt-degrade-{suffix}");

        // First drain registers readiness and records the deadline; subsequent
        // drains (past the 1ms timeout) degrade to a per-tree-slice flip.
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainerA.DrainAsync();
            if (await maintainerA.GetLagAsync() == 0)
            {
                break;
            }

            await Task.Delay(20);
        }

        var treeViewA = await ViewTreeAsync(viewA);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await treeViewA.GetAsync("da"), Is.EqualTo(Person(30, "a1")),
                "After degrading, view A flips its own slice atomically.");
            Assert.That(degradeMetric.Measurements.Count, Is.GreaterThanOrEqualTo(1),
                "Degrading to per-tree-slice atomicity must emit the joint-atomicity-violation metric.");
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0),
                "After degrading, view A advances its checkpoint to the head.");
        });
    }

    [Test]
    public async Task Joint_flip_is_idempotent_under_redelivery()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-xt-idem-a-src-{suffix}";
        var treeB = $"mv-xt-idem-b-src-{suffix}";
        var viewA = $"mv-xt-idem-a-view-{suffix}";
        var viewB = $"mv-xt-idem-b-view-{suffix}";
        _ = CreateAdultView(treeA, viewA);
        _ = CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);

        await CrossTreeWriteAsync(
            treeA, "ia", Person(33, "a1"),
            treeB, "ib", Person(44, "b1"),
            $"mv-xt-idem-{suffix}");

        for (var round = 0; round < 10; round++)
        {
            await maintainerA.DrainAsync();
            await maintainerB.DrainAsync();
            if (await maintainerA.GetLagAsync() == 0 && await maintainerB.GetLagAsync() == 0)
            {
                break;
            }

            await Task.Delay(20);
        }

        // Re-drain both maintainers many more times: the deterministic joint
        // operation id and the coordinator's memoized decision make every replay a
        // no-op - the values must remain exactly the committed batch.
        await DrainAsync(maintainerA, times: 5);
        await DrainAsync(maintainerB, times: 5);

        var treeViewA = await ViewTreeAsync(viewA);
        var treeViewB = await ViewTreeAsync(viewB);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await treeViewA.GetAsync("ia"), Is.EqualTo(Person(33, "a1")));
            Assert.That(await treeViewB.GetAsync("ib"), Is.EqualTo(Person(44, "b1")));
            Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
            Assert.That(await maintainerB.GetLagAsync(), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Coordinator_terminal_degrade_makes_late_registrants_flip_locally_not_jointly()
    {
        // A participant times out and degrades before the wait set completes. The
        // coordinator must terminally degrade so a *late* registration of the
        // missing participant flips locally (Degraded) rather than triggering a
        // joint flip that could clobber the already-degraded participant's local
        // flip.
        var op = $"mvxtdegradeterminal{Guid.NewGuid():N}";
        var viewA = $"view-a-{op}";
        var viewB = $"view-b-{op}";
        var waitSet = new List<string> { viewA, viewB };
        waitSet.Sort(StringComparer.Ordinal);
        var coordinator = _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(op);

        var first = await coordinator.RegisterReadyAsync(Readiness(op, viewA, waitSet, $"vt-a-{op}"));
        var degraded = await coordinator.RegisterDegradedAsync(viewA);
        var late = await coordinator.RegisterReadyAsync(Readiness(op, viewB, waitSet, $"vt-b-{op}"));

        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.False, "the wait set is incomplete on the first registration");
            Assert.That(first.Degraded, Is.False);
            Assert.That(degraded.Degraded, Is.True, "the timed-out participant terminally degrades the operation");
            Assert.That(degraded.Applied, Is.False);
            Assert.That(late.Degraded, Is.True, "a late registrant on a degraded operation flips locally, never jointly");
            Assert.That(late.Applied, Is.False, "no joint flip is ever issued once the operation has degraded");
        });
    }

    [Test]
    public async Task Coordinator_degrade_after_joint_flip_committed_returns_committed()
    {
        // The joint flip commits, then a participant degrades after the fact (it
        // lost the race to its own timeout). The coordinator must report the
        // committed decision so the late-degrading maintainer applies the joint
        // result instead of double-writing its slice locally.
        var op = $"mvxtdegradelate{Guid.NewGuid():N}";
        var viewA = $"view-a-{op}";
        var viewB = $"view-b-{op}";
        var treeViewA = $"vt-a-{op}";
        var treeViewB = $"vt-b-{op}";
        var waitSet = new List<string> { viewA, viewB };
        waitSet.Sort(StringComparer.Ordinal);
        var coordinator = _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(op);

        await coordinator.RegisterReadyAsync(Readiness(op, viewA, waitSet, treeViewA, ("ka", Person(20, "a"))));
        var completed = await coordinator.RegisterReadyAsync(Readiness(op, viewB, waitSet, treeViewB, ("kb", Person(21, "b"))));
        var lateDegrade = await coordinator.RegisterDegradedAsync(viewA);

        Assert.Multiple(() =>
        {
            Assert.That(completed.Applied, Is.True, "the second registration completes the wait set and applies the joint flip");
            Assert.That(lateDegrade.Applied, Is.True, "a degrade after the joint flip committed sees the committed decision");
            Assert.That(lateDegrade.Degraded, Is.False, "an already-committed operation is never degraded");
        });

        // The joint flip durably wrote each participant view tree's slice.
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await _fixture.Cluster.Client.GetGrain<ILattice>(treeViewA).GetAsync("ka"), Is.EqualTo(Person(20, "a")));
            Assert.That(await _fixture.Cluster.Client.GetGrain<ILattice>(treeViewB).GetAsync("kb"), Is.EqualTo(Person(21, "b")));
        });
    }

    private static ViewCrossTreeReadiness Readiness(
        string op, string viewName, List<string> waitSet, string viewTreeId, params (string Key, byte[] Value)[] upserts)
        => new()
        {
            OperationId = op,
            ViewName = viewName,
            WaitSet = waitSet,
            ViewTreeId = viewTreeId,
            Upserts = [.. upserts.Select(u => new KeyValuePair<string, byte[]>(u.Key, u.Value))],
        };
}
