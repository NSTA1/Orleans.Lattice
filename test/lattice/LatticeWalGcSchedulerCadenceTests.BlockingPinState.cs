using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the blocking-pin classifier added by issue #3042.
/// <para>
/// <c>blocked</c> on <see cref="LatticeMetrics.WalGcPasses"/> says a tree
/// cannot reclaim. It cannot say whether that is a <i>defect</i> or
/// <i>correct behaviour</i>, because
/// <c>BPlusLeafGrain.CursorRegistry.ResolveDurablePinForPartition</c> publishes
/// <c>(Zero, -1)</c> from a single branch covering both routes in - so the
/// durable pin record preserves no difference between them and no amount of
/// reading the pin can recover it. Those two states call for opposite
/// responses: one is a repairable coverage hole, the other is retained by
/// design and must never be "repaired". Collapsed, a fix and a decision not to
/// fix are equally unfounded.
/// </para>
/// <para>
/// The classifier therefore reads the leaf's own persisted checkpoint
/// <b>without activating the leaf</b>, and that is load-bearing rather than an
/// optimisation. A leaf that could be activated would report a cursor, become
/// <i>present</i> in the live registry, and be skipped by the floor before its
/// pin was ever evaluated - so it could not have been the blocker. An
/// instrument that needed the leaf live would measure only the leaves that are
/// not the problem, which is the same error as the remedy it exists to
/// adjudicate.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private const string BlockingPinStatusTag = LatticeMetrics.TagStatus;

    private static readonly string[] EveryBlockingPinArm =
        ["checkpointed_uncovered", "never_checkpointed", "no_durable_state", "unreadable", "orphaned"];

    /// <summary>
    /// A storage provider that serves one canned leaf state, counting reads so
    /// the once-per-episode charge can be asserted rather than assumed.
    /// </summary>
    /// <remarks>
    /// Hand-rolled rather than substituted: the contract under test is a
    /// generic method that must populate its <c>out</c>-shaped
    /// <see cref="IGrainState{T}"/> argument, and a fake that does so explicitly
    /// is both clearer and immune to the arrangement silently not applying -
    /// which would present as the classifier reporting <c>no_durable_state</c>
    /// for every arm and look exactly like a real finding.
    /// </remarks>
    private sealed class CannedLeafStateStorage(LeafNodeState? state, Exception? throws = null) : IGrainStorage
    {
        public int Reads { get; private set; }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Reads++;

            if (throws is not null)
            {
                throw throws;
            }

            if (state is not null && grainState is IGrainState<LeafNodeState> leafState)
            {
                leafState.State = state;
                leafState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }

            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            Task.CompletedTask;

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            Task.CompletedTask;
    }

    private static LeafNodeState LeafCheckpointedAt(long offset, int partition = 0, int partitions = 1)
    {
        var byPartition = new long[partitions];
        Array.Fill(byPartition, -1L);
        byPartition[partition] = offset;

        return new LeafNodeState
        {
            // A bound tree id is not incidental here. Pin registration is
            // birth-gated on one, so a leaf that published a durable pin
            // necessarily persisted it; a state without one is a husk whose
            // leaf has been reclaimed, and the classifier reports that as
            // 'orphaned' before it ever looks at the checkpoint (issue #3105).
            // Omitting it would arrange a husk and assert the live-leaf arms.
            TreeId = StrandedTree,
            ProjectionCheckpointOffset = partition == 0 ? offset : -1L,
            ProjectionCheckpointOffsetsByPartition = byPartition,
        };
    }

    // ------------------------------------------------------------ the arming

    [Test]
    public void BlockingPinStateTag_arms_every_declared_blocking_pin_state()
    {
        var members = Enum.GetValues<WalGcBlockingPinState>();

        // An empty member list would make every assertion below vacuously true
        // and report a clean gate that scanned nothing.
        Assert.That(members, Has.Length.GreaterThanOrEqualTo(4),
            "the state enum must be non-trivial, or this gate passes without checking anything.");

        var armed = members
            .Select(member => LatticeWalGcScheduler.BlockingPinStateTag(member).Value as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(armed, Has.None.Null, "every state must map to a named arm.");
            Assert.That(armed, Is.Unique,
                "two states sharing an arm would silently sum - and on this instrument the two that would "
                    + "most plausibly be merged are the repairable state and the one that must never be "
                    + "repaired, so a fold here is a wrong answer rather than a coarse one.");
            Assert.That(armed, Is.EquivalentTo(EveryBlockingPinArm),
                "the arm set must match what the instrument description, the docs row and the dashboard panel all claim it is.");
        });
    }

    [Test]
    public void BlockingPinStateTag_throws_rather_than_folding_an_unmapped_state()
    {
        var undeclared = (WalGcBlockingPinState)int.MaxValue;

        Assert.That(() => LatticeWalGcScheduler.BlockingPinStateTag(undeclared),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "an unclassified state must fail loudly rather than join a neighbouring bucket.");
    }

    // -------------------------------------------------- the checkpoint mapping

    [Test]
    public void ClassifyCheckpoint_reads_a_checkpointed_partition_as_the_repairable_state()
    {
        var state = LeafCheckpointedAt(offset: 42, partition: 1, partitions: 4);

        Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(state, partition: 1),
            Is.EqualTo(WalGcBlockingPinState.CheckpointedUncovered),
            "a durable checkpoint means there IS a WAL offset the leaf could honestly claim, so the unusable "
                + "pin is the coverage half being absent - which is the repairable state.");
    }

    [Test]
    public void ClassifyCheckpoint_reads_the_sentinel_as_never_checkpointed()
    {
        var state = LeafCheckpointedAt(offset: 42, partition: 1, partitions: 4);

        Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(state, partition: 2),
            Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed),
            "partition 2 carries the -1 sentinel, so there is no offset the leaf could honestly claim and the "
                + "blocking pin is correct by design.");
    }

    [Test]
    public void ClassifyCheckpoint_does_not_read_one_partitions_checkpoint_as_anothers()
    {
        // The defect this excludes is the one that makes the whole instrument
        // worthless: reading partition 0's checkpoint for every partition would
        // report the estate as uniformly repairable, which is a plausible wrong
        // answer in the exact place a reader trusts one.
        var state = LeafCheckpointedAt(offset: 7, partition: 0, partitions: 8);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(state, partition: 0),
                Is.EqualTo(WalGcBlockingPinState.CheckpointedUncovered));
            Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(state, partition: 6),
                Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed),
                "partition 6 has its own sentinel and must not inherit partition 0's checkpoint.");
        });
    }

    [Test]
    public void ClassifyCheckpoint_treats_a_partition_beyond_the_persisted_width_as_never_checkpointed()
    {
        var state = LeafCheckpointedAt(offset: 9, partition: 0, partitions: 2);

        Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(state, partition: 5),
            Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed),
            "a partition the leaf has never persisted a slot for has necessarily never been checkpointed; "
                + "indexing past the array would throw and take the GC pass down with it.");
    }

    [Test]
    public void ClassifyCheckpoint_falls_back_to_the_scalar_slot_for_legacy_single_partition_state()
    {
        // State written before multi-partition replay carries the scalar slot
        // only, and it holds partition 0's checkpoint. Ignoring it would report
        // every pre-upgrade leaf as never_checkpointed - an entire population
        // misfiled into the arm that says "do not repair this".
        var legacy = new LeafNodeState
        {
            ProjectionCheckpointOffset = 88,
            ProjectionCheckpointOffsetsByPartition = null,
        };

        Assert.Multiple(() =>
        {
            Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(legacy, partition: 0),
                Is.EqualTo(WalGcBlockingPinState.CheckpointedUncovered),
                "the scalar slot is partition 0's checkpoint under the legacy layout.");
            Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(legacy, partition: 1),
                Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed),
                "the scalar slot speaks only for partition 0, so a higher partition has genuinely never been "
                    + "checkpointed under that layout.");
        });
    }

    [Test]
    public void ClassifyCheckpoint_reads_a_legacy_sentinel_as_never_checkpointed()
    {
        var legacy = new LeafNodeState
        {
            ProjectionCheckpointOffset = -1,
            ProjectionCheckpointOffsetsByPartition = null,
        };

        Assert.That(LatticeWalGcScheduler.ClassifyCheckpoint(legacy, partition: 0),
            Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed));
    }

    // ------------------------------------------------------------- the priming

    [Test]
    public async Task Every_blocking_pin_arm_is_primed_per_tree_even_when_nothing_is_blocked()
    {
        // The reachability claim, and the layer the per-partition priming
        // structurally cannot make: a partition is only known once a blocker has
        // been parsed, so on a tree that has never blocked there would otherwise
        // be no series of any shape - which is equally consistent with a silo
        // that predates this build. That is the exact ambiguity #3042 exists to
        // remove, so leaving it would reproduce the defect one level up.
        var tree = "walgc-pinstate-prime-healthy";
        var time = new VirtualTimeProvider();

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), GcReaching("idle"), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var armsAtNone = states.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagPartition) as string) == LatticeMetrics.PartitionNone)
            .Select(m => m.Tag(BlockingPinStatusTag) as string)
            .Distinct()
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(armsAtNone, Is.EquivalentTo(EveryBlockingPinArm),
                "a tree that never blocked must still mint all five arms, so an absent series means the "
                    + "classifier is not running on this silo rather than that nothing was classified.");
            Assert.That(states.Measurements.Where(m =>
                    (m.Tag(LatticeMetrics.TagPartition) as string) == LatticeMetrics.PartitionNone)
                .Select(m => m.Value),
                Has.All.Zero,
                "the reachability arm carries no count of its own - a non-zero there would be a classification "
                    + "filed under a partition that does not exist.");
        });
    }

    // ------------------------------------------------------- the classification

    [Test]
    public async Task A_blocking_leaf_that_has_checkpointed_is_classified_as_repairable()
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));

        var storage = new CannedLeafStateStorage(LeafCheckpointedAt(offset: 1234));
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time, leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var recorded = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => (Partition: m.Tag(LatticeMetrics.TagPartition) as string,
                          Status: m.Tag(BlockingPinStatusTag) as string))
            .ToArray();

        var primedAtPartition = states.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagPartition) as string) == "0")
            .Select(m => m.Tag(BlockingPinStatusTag) as string)
            .Distinct()
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(storage.Reads, Is.EqualTo(1),
                "the classifier must read the leaf's durable slot exactly once, and must do so without "
                    + "activating it.");
            Assert.That(recorded, Is.EqualTo(new[] { ("0", "checkpointed_uncovered") }),
                "a leaf with a durable checkpoint is in the repairable state, filed under its own partition.");
            Assert.That(primedAtPartition, Is.EquivalentTo(EveryBlockingPinArm),
                "the states that did not apply must read as a measured zero for this partition rather than "
                    + "as an absence the reader has to interpret.");
        });
    }

    [Test]
    public async Task A_tree_that_stays_blocked_is_classified_once_not_once_per_pass()
    {
        // The cost bound, asserted rather than asserted-about. A LeafNodeState read
        // carries the leaf's whole projection, and a blocked tree stays blocked for
        // as long as the episode lasts - so an unlatched classifier would re-read
        // that projection every cadence tick, indefinitely, on exactly the trees
        // that are already unwell. One pass cannot show this; it takes two.
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));

        var storage = new CannedLeafStateStorage(LeafCheckpointedAt(offset: 1234));
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time, leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        var classifications = states.Measurements.Count(m => m.Value > 0);

        Assert.Multiple(() =>
        {
            Assert.That(storage.Reads, Is.EqualTo(1),
                "three passes over a tree that never unblocked must cost one durable read, not three - the "
                    + "latch is taken before the read, so even a read that throws is not retried per pass.");
            Assert.That(classifications, Is.EqualTo(1),
                "a re-classification each pass would also inflate the counter, making a single stuck leaf "
                    + "read as a rising population of them.");
        });
    }

    [Test]
    public async Task A_blocking_leaf_with_no_durable_state_is_not_folded_into_never_checkpointed()
    {
        // Folding this into never_checkpointed would assert "the leaf holds live
        // data it has never checkpointed" on the strength of an ABSENCE - which
        // is precisely the merge ResolveDurablePinForPartition already performs
        // and that this instrument exists to undo.
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));

        var storage = new CannedLeafStateStorage(state: null);
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time, leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var recorded = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(BlockingPinStatusTag) as string)
            .ToArray();

        Assert.That(recorded, Is.EqualTo(new[] { "no_durable_state" }),
            "a provider that answered and reported nothing persisted is a fourth state, not a flavour of "
                + "either of the two that describe a leaf holding data.");
    }

    [Test]
    public async Task A_read_that_throws_is_counted_as_unreadable_rather_than_as_a_finding()
    {
        // unreadable reports a failure of the MEASUREMENT. Folding it into a
        // state arm would render a defect in the instrument as a finding about
        // the system, which is the failure this epic has paid for repeatedly.
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));

        var storage = new CannedLeafStateStorage(state: null, throws: new InvalidOperationException("store down"));
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time, leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var recorded = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(BlockingPinStatusTag) as string)
            .ToArray();

        Assert.That(recorded, Is.EqualTo(new[] { "unreadable" }),
            "a storage failure must not be presented as a property of the leaf.");
    }

    [Test]
    public async Task A_silo_with_no_storage_provider_reports_unreadable_rather_than_no_durable_state()
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));

        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time, leafStateStorage: null);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var recorded = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(BlockingPinStatusTag) as string)
            .ToArray();

        Assert.That(recorded, Is.EqualTo(new[] { "unreadable" }),
            "a silo with no provider registered must not present as an estate of leaves that never persisted "
                + "anything.");
    }

    [Test]
    public async Task An_unparseable_consumer_id_is_classified_under_the_unknown_partition()
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming("not-a-materialiser-consumer")));

        var storage = new CannedLeafStateStorage(LeafCheckpointedAt(offset: 5));
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time, leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var recorded = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => (Partition: m.Tag(LatticeMetrics.TagPartition) as string,
                          Status: m.Tag(BlockingPinStatusTag) as string))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Is.EqualTo(new[] { (LatticeMetrics.PartitionUnknown, "unreadable") }),
                "a consumer id that does not parse names no partition, so it must not be filed under a "
                    + "numeric one it was never shown to belong to.");
            Assert.That(storage.Reads, Is.Zero,
                "an unresolvable id must not send the classifier at an arbitrary grain - reading the wrong "
                    + "leaf's state would be worse than declining to classify.");
        });
    }
}
