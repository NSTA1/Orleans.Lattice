using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the receiver-side enrollment / merge-mode admission gate in
/// <see cref="ReplicationApplier"/> (issue #1267). The inbound apply path must
/// reject an entry whose <see cref="WalRecord.TreeId"/> is not enrolled for
/// replication on this receiver - the core <c>ThrowIfSystemTree</c> reserved
/// prefix guards only the <c>_lattice_</c> trees, not the <c>sys-</c>-prefixed
/// authorization / identity trees a cluster may keep cluster-local by not
/// enrolling them - and must re-resolve the merge mode locally rather than
/// trusting the peer-supplied wire <see cref="WalRecord.Mode"/>.
/// </summary>
public partial class ReplicationApplierTests
{
    private const string OrdersTree = "orders";
    private const string SysAuthTree = "sys-auth-policies";
    private const string SysMembershipTree = "sys-membership-roles";

    /// <summary>
    /// Dictionary-backed <see cref="ILatticeReplicationContext"/> that reports
    /// exactly the enrolled trees, mirroring what the production
    /// <c>ConfiguredLatticeReplicationContext</c> resolves for a cluster that
    /// enrolled only a subset of its trees.
    /// </summary>
    private sealed class MapReplicationContext(IReadOnlyDictionary<string, LatticeMergeMode> modes)
        : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => LocalCluster;

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            modes.TryGetValue(treeId, out var mode) ? mode : null;
    }

    private static (
        ReplicationApplier Applier,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm,
        IReplicationDeadLetterGrain Dlq)
        CreateEnrollmentApplier(
            ILatticeReplicationContext? context = null,
            IReadOnlyDictionary<string, LatticeMergeMode>? replicatedTrees = null)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        factory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.GetPinnedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = LocalCluster, ReplicatedTrees = replicatedTrees };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var applier = new ReplicationApplier(factory, monitor, replicationContext: context);
        return (applier, apply, hwm, dlq);
    }

    private static WalRecord EnrollmentEntry(
        string treeId,
        string key,
        HybridLogicalClock ts,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister,
        string origin = RemoteCluster) => new()
        {
            TreeId = treeId,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = origin,
            Mode = mode,
        };

    [Test]
    public async Task ApplyAsync_rejects_entry_for_tree_not_enrolled_here()
    {
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var result = await applier.ApplyAsync(EnrollmentEntry(SysAuthTree, "k", Hlc(10)));

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        // A non-enrolled tree id is peer-controlled, so it is dropped rather
        // than parked (parking would let a peer spawn unbounded DLQ activations).
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_rejects_sys_membership_tree_not_enrolled_here()
    {
        var (applier, apply, _, _) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var result = await applier.ApplyAsync(EnrollmentEntry(SysMembershipTree, "role-a", Hlc(11)));

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_applies_entry_for_enrolled_tree_with_matching_mode()
    {
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));
        var ts = Hlc(20, 1);

        var result = await applier.ApplyAsync(EnrollmentEntry(OrdersTree, "k", ts));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 0);
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_dead_letters_entry_whose_mode_disagrees_with_local_mode()
    {
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        // Enrolled tree, but the peer stamped an OrSet wire mode the receiver
        // resolves locally to LwwRegister.
        var result = await applier.ApplyAsync(
            EnrollmentEntry(OrdersTree, "k", Hlc(30), mode: LatticeMergeMode.OrSet));

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        await dlq.Received(1).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonModeMismatch,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_drops_entry_when_no_enrollment_source_is_configured()
    {
        // Fail closed on ambiguity (issue #1398): neither a replication context
        // nor a ReplicatedTrees map is wired, so the applier has no enrollment
        // signal and cannot evaluate the gate. The entry is dropped (not
        // admitted, not parked) - a peer holding the mesh secret must not be
        // able to write a tree a mis-wired receiver kept cluster-local.
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier();
        var ts = Hlc(40);

        var result = await applier.ApplyAsync(EnrollmentEntry(SysAuthTree, "k", ts));

        Assert.That(result.Applied, Is.False);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        // Dropped, never parked: the tree id is peer-controlled.
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_drops_multi_entry_run_when_no_enrollment_source_is_configured()
    {
        // The batch run path mirrors the per-entry fail-closed drop (issue #1398).
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier();

        var batch = new[]
        {
            EnrollmentEntry(SysAuthTree, "a", Hlc(41)),
            EnrollmentEntry(SysAuthTree, "b", Hlc(42)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyAsync_admits_tree_enabled_at_runtime_once_enrollment_source_reports_its_mode()
    {
        // A tree enabled at runtime via replication control resolves through the
        // same ILatticeReplicationContext (its resolver begins reporting the live
        // mode). Before enablement the context resolves the tree to null and the
        // entry is dropped as not-replicated; after enablement the identical
        // context path admits it. The enrollment MECHANISM is wired throughout,
        // so the fail-closed no-source arm (issue #1398) is never involved.
        var modes = new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister };
        var (applier, apply, hwm, _) = CreateEnrollmentApplier(new MapReplicationContext(modes));

        var beforeEnable = await applier.ApplyAsync(EnrollmentEntry("runtime-tree", "k", Hlc(45)));
        Assert.That(beforeEnable.Applied, Is.False, "A tree not yet enabled resolves to null and is dropped.");

        // Runtime-enable the tree: the resolver now reports its live mode.
        modes["runtime-tree"] = LatticeMergeMode.LwwRegister;
        var ts = Hlc(46, 1);
        var afterEnable = await applier.ApplyAsync(EnrollmentEntry("runtime-tree", "k", ts));

        Assert.That(afterEnable.Applied, Is.True, "Once enabled at runtime the same context path admits the entry.");
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 0);
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_enforces_enrollment_via_replicated_trees_map_when_no_context()
    {
        // No context: the ReplicatedTrees map is the enrollment source.
        var (applier, apply, _, _) = CreateEnrollmentApplier(
            replicatedTrees: new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister });

        var rejected = await applier.ApplyAsync(EnrollmentEntry(SysAuthTree, "k", Hlc(50)));
        var admitted = await applier.ApplyAsync(EnrollmentEntry(OrdersTree, "k", Hlc(51)));

        Assert.Multiple(() =>
        {
            Assert.That(rejected.Applied, Is.False);
            Assert.That(admitted.Applied, Is.True);
        });
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(51), RemoteCluster, null, 0);
    }

    [Test]
    public async Task ApplyAsync_rejection_is_idempotent_and_never_mutates_state()
    {
        var (applier, apply, hwm, _) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));
        var entry = EnrollmentEntry(SysAuthTree, "k", Hlc(60));

        var first = await applier.ApplyAsync(entry);
        var second = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.False);
            Assert.That(second.Applied, Is.False);
            Assert.That(first.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(second.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_rejects_multi_entry_run_for_non_enrolled_tree()
    {
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var batch = new[]
        {
            EnrollmentEntry(SysAuthTree, "a", Hlc(70)),
            EnrollmentEntry(SysAuthTree, "b", Hlc(71)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_dead_letters_multi_entry_run_whose_mode_disagrees()
    {
        var (applier, apply, hwm, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var batch = new[]
        {
            EnrollmentEntry(OrdersTree, "a", Hlc(80), mode: LatticeMergeMode.OrSet),
            EnrollmentEntry(OrdersTree, "b", Hlc(81), mode: LatticeMergeMode.OrSet),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        // Each mismatched entry is parked (the tree is enrolled and bounded).
        await dlq.Received(2).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonModeMismatch,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_applies_multi_entry_run_for_enrolled_tree_with_matching_mode()
    {
        var (applier, apply, _, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var batch = new[]
        {
            EnrollmentEntry(OrdersTree, "a", Hlc(90)),
            EnrollmentEntry(OrdersTree, "b", Hlc(91)),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
        await dlq.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default!, default, default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_dead_letters_entries_smuggled_behind_a_conforming_first_entry()
    {
        // Security regression: a run was segmented on (treeId, originClusterId)
        // only, so the merge-mode gate - which classifies a run from its
        // representative first entry - could be satisfied by a single conforming
        // entry while dispatch inside the run switched on each entry's own
        // peer-supplied Mode. A peer could therefore head a run with a
        // conforming LwwRegister entry and smuggle entries carrying an arbitrary
        // CRDT algebra behind it, writing state into an enrolled tree under a
        // merge mode the operator never configured, with no dead-letter and no
        // mismatch metric. Mode is now part of the run key, so the smuggled
        // entries form their own run and are classified on their own merits.
        var (applier, apply, _, dlq) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var batch = new[]
        {
            // Conforming head: matches the locally-resolved mode.
            EnrollmentEntry(OrdersTree, "a", Hlc(100)),
            // Smuggled tail: same tree and origin, attacker-chosen algebra.
            EnrollmentEntry(OrdersTree, "b", Hlc(101), mode: LatticeMergeMode.GCounter),
            EnrollmentEntry(OrdersTree, "c", Hlc(102), mode: LatticeMergeMode.GCounter),
        };

        await applier.ApplyBatchAsync(batch);

        // The mismatched entries are parked with the mode-mismatch reason
        // instead of being folded through the attacker-chosen merge algebra.
        await dlq.Received(2).EnqueueAsync(
            Arg.Any<WalRecord>(),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonModeMismatch,
            Arg.Any<CancellationToken>());
        await apply.DidNotReceiveWithAnyArgs().ApplyCrdtDeltaManyAsync(default!);
    }

    [Test]
    public async Task ApplyBatchAsync_still_applies_the_conforming_prefix_of_a_smuggled_batch()
    {
        // The conforming head is legitimate traffic and must still be applied:
        // splitting the run on a mode change rejects only the smuggled tail, it
        // does not punish the whole batch.
        var (applier, apply, _, _) = CreateEnrollmentApplier(
            new MapReplicationContext(new Dictionary<string, LatticeMergeMode> { [OrdersTree] = LatticeMergeMode.LwwRegister }));

        var batch = new[]
        {
            EnrollmentEntry(OrdersTree, "a", Hlc(110)),
            EnrollmentEntry(OrdersTree, "b", Hlc(111)),
            EnrollmentEntry(OrdersTree, "c", Hlc(112), mode: LatticeMergeMode.OrSet),
        };

        var result = await applier.ApplyBatchAsync(batch);

        Assert.That(result.Applied, Is.True, "the conforming prefix is honoured");
        await apply.Received(1).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
    }
}
