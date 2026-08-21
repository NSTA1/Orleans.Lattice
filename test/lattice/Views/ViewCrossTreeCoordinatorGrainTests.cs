using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// In-process, cluster-free coverage for <see cref="ViewCrossTreeCoordinatorGrain"/>.
/// The grain is constructed directly with substituted collaborators and a
/// <see cref="FakePersistentState{T}"/>, so the wait-set rendezvous, the guard
/// clauses, the memoized applied/degraded returns, and the joint-flip fan-out
/// (single-tree atomic write, all-empty no-op, and the two-tree cross-tree
/// commit) are all asserted deterministically without an Orleans TestCluster.
/// </summary>
[TestFixture]
public sealed class ViewCrossTreeCoordinatorGrainTests
{
    private const string OperationId = "op-xt-1";

    private sealed class Harness
    {
        public required ViewCrossTreeCoordinatorGrain Grain { get; init; }
        public required IGrainFactory GrainFactory { get; init; }
        public required FakePersistentState<ViewCrossTreeCoordinatorState> State { get; init; }
    }

    private static Harness CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("view-cross-tree-coord", OperationId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var state = new FakePersistentState<ViewCrossTreeCoordinatorState>();

        var grain = new ViewCrossTreeCoordinatorGrain(
            context,
            grainFactory,
            reminderRegistry,
            optionsMonitor,
            NullLogger<ViewCrossTreeCoordinatorGrain>.Instance,
            state);

        return new Harness { Grain = grain, GrainFactory = grainFactory, State = state };
    }

    private static ViewCrossTreeReadiness Readiness(
        string viewName,
        string viewTreeId,
        IReadOnlyList<string> waitSet,
        List<KeyValuePair<string, byte[]>>? upserts = null,
        IReadOnlyList<string>? deletes = null) => new()
        {
            OperationId = OperationId,
            ViewName = viewName,
            ViewTreeId = viewTreeId,
            WaitSet = waitSet,
            Upserts = upserts ?? [],
            Deletes = deletes ?? [],
        };

    private static List<KeyValuePair<string, byte[]>> Upsert(string key, params byte[] value) =>
        [new(key, value)];

    // ------------------------------------------------------------------
    // Guard clauses.
    // ------------------------------------------------------------------

    [Test]
    public void RegisterReadyAsync_null_readiness_throws()
    {
        var h = CreateGrain();
        Assert.That(async () => await h.Grain.RegisterReadyAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void RegisterReadyAsync_empty_view_name_throws()
    {
        var h = CreateGrain();
        var r = Readiness("", "view-tree-1", new[] { "v1" });
        Assert.That(async () => await h.Grain.RegisterReadyAsync(r),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RegisterReadyAsync_empty_view_tree_id_throws()
    {
        var h = CreateGrain();
        var r = Readiness("v1", "", new[] { "v1" });
        Assert.That(async () => await h.Grain.RegisterReadyAsync(r),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RegisterReadyAsync_null_wait_set_throws()
    {
        var h = CreateGrain();
        var r = Readiness("v1", "view-tree-1", null!);
        Assert.That(async () => await h.Grain.RegisterReadyAsync(r),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void RegisterReadyAsync_null_upserts_throws()
    {
        var h = CreateGrain();
        var r = new ViewCrossTreeReadiness
        {
            OperationId = OperationId,
            ViewName = "v1",
            ViewTreeId = "view-tree-1",
            WaitSet = new[] { "v1" },
            Upserts = null!,
        };
        Assert.That(async () => await h.Grain.RegisterReadyAsync(r),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ------------------------------------------------------------------
    // Memoized terminal decisions.
    // ------------------------------------------------------------------

    [Test]
    public async Task RegisterReadyAsync_already_applied_returns_committed()
    {
        var h = CreateGrain();
        h.State.State.Applied = true;

        var decision = await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", new[] { "v1" }));

        Assert.That(decision.Applied, Is.True);
        Assert.That(decision, Is.SameAs(ViewCrossTreeDecision.Committed));
    }

    [Test]
    public async Task RegisterReadyAsync_already_degraded_returns_degraded()
    {
        var h = CreateGrain();
        h.State.State.Degraded = true;

        var decision = await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", new[] { "v1" }));

        Assert.That(decision.Applied, Is.False);
        Assert.That(decision.Degraded, Is.True);
    }

    // ------------------------------------------------------------------
    // Wait-set rendezvous.
    // ------------------------------------------------------------------

    [Test]
    public async Task RegisterReadyAsync_incomplete_wait_set_returns_not_ready_and_persists()
    {
        var h = CreateGrain();

        var decision = await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1", "v2" }, Upsert("k", 1)));

        Assert.That(decision.Applied, Is.False);
        Assert.That(decision.Degraded, Is.False);
        Assert.That(h.State.State.WaitSet, Is.EquivalentTo(new[] { "v1", "v2" }));
        Assert.That(h.State.State.Slices.ContainsKey("v1"), Is.True);
        Assert.That(h.State.WriteCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task RegisterReadyAsync_wait_set_mismatch_throws()
    {
        var h = CreateGrain();

        // First registration freezes the wait set {v1, v2}.
        await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1", "v2" }, Upsert("k", 1)));

        // A second registration with a divergent wait set is rejected.
        Assert.That(async () => await h.Grain.RegisterReadyAsync(
                Readiness("v2", "view-tree-2", new[] { "v1", "v3" }, Upsert("k", 1))),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task RegisterReadyAsync_view_absent_from_wait_set_throws()
    {
        var h = CreateGrain();

        // Freeze wait set {v1, v2}.
        await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1", "v2" }, Upsert("k", 1)));

        // Same frozen wait set, but a view name that is not a member.
        Assert.That(async () => await h.Grain.RegisterReadyAsync(
                Readiness("v3", "view-tree-3", new[] { "v1", "v2" }, Upsert("k", 1))),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ------------------------------------------------------------------
    // Joint flip completion paths.
    // ------------------------------------------------------------------

    [Test]
    public async Task RegisterReadyAsync_single_non_empty_slice_flips_single_tree_and_commits()
    {
        var h = CreateGrain();
        var lattice = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("view-tree-1").Returns(lattice);

        var upserts = Upsert("k", 1, 2, 3);
        var decision = await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1" }, upserts));

        Assert.That(decision.Applied, Is.True);
        Assert.That(h.State.State.Applied, Is.True);
        await lattice.Received(1).SetManyAtomicAsync(
            upserts,
            Arg.Is<IReadOnlyList<string>>(d => d.Count == 0),
            Arg.Any<string>());
    }

    [Test]
    public async Task RegisterReadyAsync_single_slice_with_deletes_builds_mixed_batch_and_commits()
    {
        var h = CreateGrain();
        var lattice = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("view-tree-1").Returns(lattice);

        var upserts = Upsert("k", 9);
        var deletes = new[] { "old-key" };
        var decision = await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1" }, upserts, deletes));

        Assert.That(decision.Applied, Is.True);
        await lattice.Received(1).SetManyAtomicAsync(
            upserts,
            Arg.Is<IReadOnlyList<string>>(d => d.Count == 1 && d[0] == "old-key"),
            Arg.Any<string>());
    }

    [Test]
    public async Task RegisterReadyAsync_all_empty_slices_commit_without_write()
    {
        var h = CreateGrain();
        var lattice = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);

        // Single-view wait set, empty upserts and deletes -> batches.Count == 0.
        var decision = await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1" }));

        Assert.That(decision.Applied, Is.True);
        await lattice.DidNotReceive().SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(),
            Arg.Any<string>());
    }

    [Test]
    public async Task RegisterReadyAsync_two_non_empty_slices_issue_cross_tree_commit()
    {
        var h = CreateGrain();
        var tx = Substitute.For<ILatticeCrossTreeTxGrain>();
        h.GrainFactory.GetGrain<ILatticeCrossTreeTxGrain>(Arg.Any<string>()).Returns(tx);

        // First view registers -> wait set incomplete.
        var first = await h.Grain.RegisterReadyAsync(
            Readiness("v1", "view-tree-1", new[] { "v1", "v2" }, Upsert("a", 1)));
        Assert.That(first.Applied, Is.False);

        // Second view completes the wait set -> two non-empty batches -> cross-tree flip.
        var second = await h.Grain.RegisterReadyAsync(
            Readiness("v2", "view-tree-2", new[] { "v1", "v2" }, Upsert("b", 2)));

        Assert.That(second.Applied, Is.True);
        Assert.That(h.State.State.Applied, Is.True);
        await tx.Received(1).CommitAsync(Arg.Is<List<LatticeTreeBatch>>(b => b.Count == 2));
    }

    [Test]
    public async Task RegisterReadyAsync_redelivery_after_applied_returns_committed_without_reflip()
    {
        var h = CreateGrain();
        var lattice = Substitute.For<ILattice>();
        h.GrainFactory.GetGrain<ILattice>("view-tree-1").Returns(lattice);

        var upserts = Upsert("k", 1);
        var waitSet = new[] { "v1" };
        await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", waitSet, upserts));

        // A redelivered registration after the flip committed re-confirms without a second write.
        var again = await h.Grain.RegisterReadyAsync(Readiness("v1", "view-tree-1", waitSet, upserts));

        Assert.That(again, Is.SameAs(ViewCrossTreeDecision.Committed));
        await lattice.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(),
            Arg.Any<string>());
    }

    // ------------------------------------------------------------------
    // RegisterDegradedAsync.
    // ------------------------------------------------------------------

    [Test]
    public void RegisterDegradedAsync_empty_view_name_throws()
    {
        var h = CreateGrain();
        Assert.That(async () => await h.Grain.RegisterDegradedAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task RegisterDegradedAsync_fresh_terminally_degrades_and_persists()
    {
        var h = CreateGrain();

        var decision = await h.Grain.RegisterDegradedAsync("v1");

        Assert.That(decision.Degraded, Is.True);
        Assert.That(decision.Applied, Is.False);
        Assert.That(h.State.State.Degraded, Is.True);
        Assert.That(h.State.WriteCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task RegisterDegradedAsync_when_already_applied_returns_committed()
    {
        var h = CreateGrain();
        h.State.State.Applied = true;

        var decision = await h.Grain.RegisterDegradedAsync("v1");

        Assert.That(decision, Is.SameAs(ViewCrossTreeDecision.Committed));
    }

    [Test]
    public async Task RegisterDegradedAsync_idempotent_when_already_degraded()
    {
        var h = CreateGrain();
        await h.Grain.RegisterDegradedAsync("v1");
        var writesAfterFirst = h.State.WriteCount;

        var decision = await h.Grain.RegisterDegradedAsync("v1");

        Assert.That(decision.Degraded, Is.True);
        Assert.That(h.State.WriteCount, Is.EqualTo(writesAfterFirst));
    }
}
