using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for issue #4238: the scheduler parses a materialiser consumer id back
/// into a leaf and a WAL partition, and every pin-removal decision it makes
/// rests on that parse.
/// <para>
/// <b>The primary fix is the fail-safe.</b> The orphan sweep and the drive's
/// orphan retirement both delete a durable pin on the strength of a storage
/// read of the parsed leaf. A parse that names the wrong grain reads an absent
/// record, classifies it <c>no_durable_state</c>, and deletes the pin of a leaf
/// that is still live - after which the trim floor no longer holds that leaf's
/// un-replayed WAL tail, and the GC may trim committed entries it still needs.
/// Neither removal path may therefore act on a consumer id that is not exactly
/// the id a leaf would publish: a 32-hex guid leaf key, carrying a partition
/// suffix exactly when the tree is partitioned, in range of its pinned count.
/// </para>
/// <para>
/// <b>The instance fix is the partition source.</b> The parse used the
/// configured <see cref="LatticeOptions.WalPartitions"/> rather than the count
/// the tree registry pinned, so a tree pinned to 8 partitions on a silo
/// configured for 1 never had its <c>_3</c> suffix stripped, and
/// <c>GrainId.TryParse</c> - which splits only on the first slash - accepted the
/// remainder as a grain that does not exist.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private const string PinnedTree = "pinned-partitions-4238";

    /// <summary>
    /// A leaf id with the exact shape <c>BPlusLeafGrain</c> carries: a guid key,
    /// rendered as Orleans renders one.
    /// </summary>
    private static GrainId GuidLeafGrainId(int ordinal) =>
        GrainId.Create(
            GrainType.Create("bplusleaf"),
            GrainIdKeyExtensions.CreateGuidKey(new Guid(ordinal, 4238, 0, new byte[8])));

    private static string PinnedConsumerId(GrainId leaf, int? partition = null, string treeId = PinnedTree) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{leaf}"
            + (partition is { } p ? "_" + p.ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty);

    /// <summary>
    /// A live leaf state that has checkpointed every one of
    /// <paramref name="partitions"/> partitions, so it classifies as a real leaf
    /// on any partition the parse resolves - and only a misdirected read finds
    /// no record at all.
    /// </summary>
    private static LeafNodeState LiveOnEveryPartition(string treeId, int partitions) => new()
    {
        TreeId = treeId,
        ProjectionCheckpointOffset = 1234,
        ProjectionCheckpointOffsetAssigned = true,
        ProjectionCheckpointOffsetsByPartition = Enumerable.Repeat(1234L, partitions).ToArray(),
    };

    /// <summary>
    /// Serves <see cref="PinnedTree"/> from a registry row pinned to
    /// <paramref name="pinned"/> WAL partitions, on a silo whose options say
    /// <paramref name="configured"/>, and wires the scheduler to the
    /// registry-backed resolver exactly as <c>AddLattice</c> does.
    /// </summary>
    private static LatticeWalGcScheduler SchedulerWithPinnedPartitions(
        FakePinStore pins,
        LeafStateBook storage,
        VirtualTimeProvider time,
        int configured,
        int pinned,
        string blockingConsumerId)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(blockingConsumerId)));

        var (factory, _) = FactoryWithBlockedLeaf(PinnedTree);
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(PinnedTree)
            .Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry { WalPartitions = pinned }));

        var options = OrphanSweepOptions(walPartitions: configured);
        var resolver = new LatticeOptionsResolver(factory, Monitor(options));
        return CreateScheduler(factory, gc, options, time, leafStateStorage: storage, optionsResolver: resolver);
    }

    private sealed record PinnedSweepOutcome(
        IReadOnlyList<string> Removed,
        long Retired,
        long Live,
        long Unresolved,
        long NoDurableState,
        IReadOnlyList<string?> ClassifiedPartitions);

    private static async Task<PinnedSweepOutcome> SweepPinnedTreeAsync(
        int configured,
        int pinned,
        string consumerId,
        Action<LeafStateBook> seed)
    {
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        seed(storage);

        var pins = new FakePinStore();
        pins.Seed(PinnedTree, consumerId);

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, PinnedTree);
        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, PinnedTree);
        var scheduler = SchedulerWithPinnedPartitions(pins, storage, time, configured, pinned, consumerId);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        long Sum(InstrumentRecorder recorder, string status) => (long)recorder.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status)
            .Sum(m => m.Value);

        return new PinnedSweepOutcome(
            pins.Removals.Select(r => r.ConsumerId).ToArray(),
            Sum(sweep, "retired"),
            Sum(sweep, "live"),
            Sum(sweep, "unresolved"),
            Sum(states, "no_durable_state"),
            states.Measurements
                .Where(m => m.Value > 0)
                .Select(m => m.Tag(LatticeMetrics.TagPartition) as string)
                .ToArray());
    }

    // ------------------------------------------- the instance: partition source

    [Test]
    public async Task A_live_leaf_pin_on_a_tree_pinned_to_more_partitions_than_configured_is_never_retired()
    {
        // The data-loss shape. Configured 1, pinned 8: the parse kept the _3
        // suffix, GrainId.TryParse accepted "bplusleaf/<hex>_3", the storage read
        // of that phantom grain found nothing, and the sweep deleted the live
        // leaf's pin. Asserted on the two operator-visible arms as well, because
        // they are what tells an estate it was hit.
        var leaf = GuidLeafGrainId(1);
        var outcome = await SweepPinnedTreeAsync(
            configured: 1,
            pinned: 8,
            PinnedConsumerId(leaf, partition: 3),
            storage => storage.Put(leaf, LiveOnEveryPartition(PinnedTree, 8)));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Removed, Is.Empty,
                "the pin belongs to a live leaf; removing it lets the GC trim WAL the leaf has not replayed.");
            Assert.That(outcome.Retired, Is.Zero, "wal_gc_orphan_pin_sweep{status=retired} must stay 0.");
            Assert.That(outcome.Live, Is.EqualTo(1), "the pin must be read against the leaf that published it.");
            Assert.That(outcome.NoDurableState, Is.Zero,
                "wal_gc_blocking_pin_state{status=no_durable_state} must stay 0: the leaf has durable state.");
            Assert.That(outcome.ClassifiedPartitions, Is.EqualTo(new[] { "3" }),
                "the blocking pin must be attributed to the partition its suffix names, under the pinned count.");
        });
    }

    [Test]
    public async Task A_leaf_id_ending_in_digits_on_a_tree_pinned_to_one_partition_is_not_truncated()
    {
        // The opposite direction. Configured 8, pinned 1: the parse stripped a
        // trailing _5 that belongs to the grain id, and read the wrong leaf.
        // A real BPlusLeafGrain key is a guid and cannot end in _<digits>, so
        // this is a parse property rather than an observed estate shape - but
        // it is the one the issue names, and it must hold.
        var leaf = GrainId.Create("bplusleaf", "leaf-4238_5");
        var outcome = await SweepPinnedTreeAsync(
            configured: 8,
            pinned: 1,
            PinnedConsumerId(leaf),
            storage => storage.Put(leaf, LiveOnEveryPartition(PinnedTree, 1)));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Removed, Is.Empty);
            Assert.That(outcome.Retired, Is.Zero);
            Assert.That(outcome.Live, Is.EqualTo(1),
                "a single-partition consumer id carries no suffix, so the whole remainder is the leaf id.");
            Assert.That(outcome.NoDurableState, Is.Zero);
            Assert.That(outcome.ClassifiedPartitions, Is.EqualTo(new[] { "0" }));
        });
    }

    [Test]
    public async Task The_bank_is_graded_against_the_head_of_the_partition_the_registry_pinned()
    {
        // The release-proof read. Configured 1, pinned 8, a floor holder on
        // partition 3: the head must be read from WAL shard {tree}/3. Reading
        // {tree}/0 grades the bank against another shard's head, which can
        // suppress a drive the floor holder still needs or spend one it does not.
        var leaf = GuidLeafGrainId(2);
        var consumerId = PinnedConsumerId(leaf, partition: 3, treeId: OrphanSweepTree);

        var storage = new LeafStateBook();
        storage.Put(leaf, LiveOnEveryPartition(OrphanSweepTree, 8));

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, consumerId, UsablePin, FloorOffset);

        var banked = new List<GrainId>();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(call =>
        {
            var id = call.ArgAt<GrainId>(0);
            var substitute = Substitute.For<IBPlusLeafGrain>();
            substitute.BankDurablePinAsync().Returns(_ =>
            {
                banked.Add(id);
                pins.Seed(OrphanSweepTree, consumerId, UsablePin, AdvancedOffset);
                return Task.CompletedTask;
            });
            substitute.DriveStarvedCheckpointAsync()
                .Returns(Task.FromResult(LeafStarvationDriveOutcome.Lifted));
            return substitute;
        });
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));
        var heads = new HeadBook(AdvancedOffset + 1);
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>())
            .Returns(call => heads.For(call.ArgAt<string>(0)));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(OrphanSweepTree)
            .Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry { WalPartitions = 8 }));

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var options = OrphanSweepOptions(walPartitions: 1);
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            factory, gc, options, time,
            leafStateStorage: storage,
            optionsResolver: new LatticeOptionsResolver(factory, Monitor(options)));

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(banked, Does.Contain(leaf),
                "the bank must reach the leaf that published the pin, or the grading below is vacuous.");
            Assert.That(heads.Keys, Does.Contain($"{OrphanSweepTree}/3"),
                "the head must come from the WAL shard of the partition the consumer is pinned on.");
            Assert.That(heads.Keys, Does.Not.Contain($"{OrphanSweepTree}/0"),
                "partition 0's head says nothing about a pin on partition 3.");
        });
    }

    // --------------------------------------------- the primary fix: fail safe

    [Test]
    public async Task The_orphan_sweep_never_retires_a_pin_whose_leaf_id_is_not_a_guid_leaf_key()
    {
        // The defence in depth. Whatever produced the id, a remainder that is
        // not a guid key is not a leaf BPlusLeafGrain could have published, so
        // an absent or tree-less record behind it proves nothing about the real
        // publisher. The pin is left holding the floor.
        var husk = GrainId.Create("bplusleaf", "leaf-4238-not-a-guid");
        var outcome = await SweepPinnedTreeAsync(
            configured: 1,
            pinned: 1,
            PinnedConsumerId(husk),
            storage => storage.PutHusk(husk));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Removed, Is.Empty, "an unrecognised leaf id must never authorise a removal.");
            Assert.That(outcome.Retired, Is.Zero);
            Assert.That(outcome.Unresolved, Is.EqualTo(1),
                "the refusal must be counted, so a parse defect shows up as unresolved rather than retired.");
        });
    }

    [Test]
    public async Task The_orphan_sweep_never_retires_an_unsuffixed_pin_on_a_partitioned_tree()
    {
        // A partitioned tree's leaves publish one suffixed id per partition; an
        // unsuffixed id there does not say which partition it speaks for, so it
        // is not the id the leaf would have built and is not retired.
        var husk = GuidLeafGrainId(3);
        var outcome = await SweepPinnedTreeAsync(
            configured: 8,
            pinned: 8,
            PinnedConsumerId(husk),
            storage => storage.PutHusk(husk));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Removed, Is.Empty);
            Assert.That(outcome.Retired, Is.Zero);
            Assert.That(outcome.Unresolved, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_orphan_sweep_never_retires_a_pin_whose_partition_suffix_is_out_of_range()
    {
        var husk = GuidLeafGrainId(4);
        var outcome = await SweepPinnedTreeAsync(
            configured: 8,
            pinned: 8,
            PinnedConsumerId(husk, partition: 9),
            storage => storage.PutHusk(husk));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Removed, Is.Empty,
                "partition 9 does not exist on an 8-partition tree, so the suffix is not a partition.");
            Assert.That(outcome.Retired, Is.Zero);
        });
    }

    [Test]
    public async Task The_orphan_sweep_still_retires_a_well_formed_orphan_on_a_partitioned_tree()
    {
        // The negative control for the three refusals above: without it they
        // cannot tell "refuses malformed ids" from "refuses everything".
        var husk = GuidLeafGrainId(5);
        var outcome = await SweepPinnedTreeAsync(
            configured: 1,
            pinned: 8,
            PinnedConsumerId(husk, partition: 3),
            storage => storage.PutHusk(husk));

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Removed, Is.EqualTo(new[] { PinnedConsumerId(husk, partition: 3) }));
            Assert.That(outcome.Retired, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_drive_never_retires_a_pin_whose_leaf_id_is_not_a_guid_leaf_key()
    {
        // The drive's NotDriven verdict is the second removal path. It trusts
        // the same parse, so it takes the same gate.
        const string TreeId = "orphan-drive-4238";
        var reporter = Substitute.For<ILeafCursorReporter>();
        var time = new VirtualTimeProvider();
        // Suffixed as a leaf of a default (multi-partition) tree publishes, so the
        // only defect in the id is the leaf key.
        var consumerId =
            $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{TreeId}_{GrainId.Create("bplusleaf", "leaf-4238-drive")}_0";

        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: reporter);

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);

            Assert.That(Outcomes(recorder, "orphaned"), Is.GreaterThan(0),
                "the drive must have reported NotDriven, or the non-removal below is vacuous.");
        }

        await reporter.DidNotReceive().UnregisterAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
