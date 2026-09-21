using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the per-partition scope of the unusable durable-pin block in
/// <see cref="LatticeWalGc"/> (issue #2849).
/// <para>
/// The defect: one quiet, data-bearing, never-checkpointed leaf publishes a
/// durable materialiser pin at <see cref="HybridLogicalClock.Zero"/>, and that
/// single pin disabled the cursor branch for the <i>whole tree</i>. Every other
/// leaf's WAL was then retained indefinitely on account of a leaf that owns the
/// keys of one partition. Worse, the condition is not self-clearing: the
/// in-memory cursor registry is per-activation state, so a restart rebuilds the
/// block from cold rather than draining it.
/// </para>
/// <para>
/// The fix attributes the block to the WAL partition the pin names, and does so
/// without touching the trim <i>floor</i>, which remains a tree-wide minimum
/// over every usable pin. That distinction is the whole safety argument: a
/// minimum over leaves is not in general safely recomputed over a subset, so an
/// unblocked partition here trims against exactly the floor the tree would have
/// used had nothing been blocked, never a higher one. What is decomposed is the
/// block, and attribution is sound by construction - an entry routes to one
/// partition under <c>WalPartitionHash</c>, a leaf publishes one pin per
/// partition under the same hash, and the GC already trims partitions
/// separately.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeWalGcPartitionedPinBlockTests
{
    private const string Tree = "tree";
    private const int Partitions = 4;

    /// <summary>The partition whose leaf has never reached a durable checkpoint.</summary>
    private const int QuietPartition = 2;

    /// <summary>
    /// The blocking leaf's consumer id. The trailing <c>_2</c> is the WAL
    /// partition, appended by <c>BPlusLeafGrain.BuildConsumerId</c> whenever the
    /// tree has more than one partition.
    /// </summary>
    private const string QuietLeafConsumer = "_lattice_materialiser_tree_leaf-1_2";

    /// <summary>A pin id carrying no partition suffix - the single-partition / legacy shape.</summary>
    private const string UnattributableConsumer = "_lattice_materialiser_tree_leaf-1";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    /// <summary>
    /// Three entries at HLC 10/20/30 in every partition, so "did this partition
    /// reclaim?" is a question about the partition rather than about where the
    /// test happened to put data.
    /// </summary>
    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        for (var partition = 0; partition < Partitions; partition++)
        {
            await provider.AppendBatchAsync(
                Tree,
                partition,
                new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(30)) },
                CancellationToken.None);
        }

        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Issue #3300: the durability hold engages by default for any tree that
        // has never published a durable offset floor, which is true of every
        // tree in this fixture. This fixture asserts per-partition pin blocking,
        // a different axis, so opt out explicitly (0 disables the hold) rather
        // than depending on a default that has since changed.
        var options = new LatticeOptions { WalPartitions = Partitions, WalDurabilityHoldCeilingBytes = 0 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    private static async Task<long[]> SurvivingOffsetsAsync(IWalStorageProvider provider, int partition)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(
            Tree, partition, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }

        return [.. survivors];
    }

    /// <summary>
    /// Reads every partition's surviving offsets, so an assertion can speak
    /// about the whole tree at once rather than one partition at a time.
    /// </summary>
    private static async Task<long[][]> SurvivorsPerPartitionAsync(IWalStorageProvider provider)
    {
        var survivors = new long[Partitions][];
        for (var partition = 0; partition < Partitions; partition++)
        {
            survivors[partition] = await SurvivingOffsetsAsync(provider, partition);
        }

        return survivors;
    }

    // ------------------------------------------------------------ the defect

    [Test]
    public async Task RunOnceAsync_one_leafs_unusable_pin_blocks_only_its_own_partition()
    {
        // The reproduction of issue #2849. A forward consumer (the shipper) is
        // at the WAL head, so every partition HAS a usable floor available; the
        // only thing standing between the tree and reclamation is one quiet
        // leaf's Zero pin. Before the fix that pin short-circuited the floor to
        // null for the entire tree and NOTHING anywhere was trimmed.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [QuietLeafConsumer] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            for (var partition = 0; partition < Partitions; partition++)
            {
                if (partition == QuietPartition)
                {
                    Assert.That(survivors[partition], Is.EqualTo(new[] { 0L, 1L, 2L }),
                        "the quiet leaf's own partition must retain its WAL - that leaf has no "
                        + "durable checkpoint, so every entry it owns is still unmaterialised.");
                }
                else
                {
                    Assert.That(survivors[partition], Is.Empty,
                        $"partition {partition} carries no entry the quiet leaf can own, so "
                        + "retaining it strands WAL on account of a leaf that cannot read it. "
                        + "This is the liveness defect of issue #2849.");
                }
            }

            Assert.That(report.EntriesTrimmed, Is.EqualTo(9),
                "three partitions of three entries reclaim; the blocked one does not.");
        });
    }

    [Test]
    public async Task RunOnceAsync_a_partially_blocked_tree_still_reports_blocked_and_names_the_leaf()
    {
        // The report shape is deliberately UNCHANGED by the fix. The scheduler's
        // blocked-leaf reactivation remedy (issues #2710/#2783) and its cadence
        // floor both key off these three fields, so a tree that is blocked in
        // one partition must still present as blocked, with a null cursor and a
        // named consumer. Narrowing the blast radius of the block must not
        // narrow the diagnosis of it.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [QuietLeafConsumer] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "a tree with any blocked partition is still in the defect state and must still be "
                + "scheduled as one.");
            Assert.That(report.MinCursor, Is.Null,
                "the reported cursor stays null so no reader mistakes a partial trim for a healthy "
                + "tree-wide floor.");
            Assert.That(report.BlockingConsumerId, Is.EqualTo(QuietLeafConsumer),
                "and the blocking leaf is still named, or an operator has to guess which of "
                + "thousands of leaves is holding the partition.");
        });
    }

    [Test]
    public async Task RunOnceAsync_a_pin_that_names_no_partition_still_blocks_the_whole_tree()
    {
        // Fail-closed. A consumer id with no partition suffix states nothing
        // about WHICH partition its leaf owns, and guessing one would trim WAL
        // a leaf still needs. The pre-#2849 whole-tree block is the correct
        // behaviour for an id this build cannot attribute, and it is what the
        // single-partition shape and any legacy pin must keep getting.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [UnattributableConsumer] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            for (var partition = 0; partition < Partitions; partition++)
            {
                Assert.That(survivors[partition], Is.EqualTo(new[] { 0L, 1L, 2L }),
                    $"partition {partition} must retain its WAL: an unattributable pin is applied "
                    + "to every partition rather than guessed at.");
            }

            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
        });
    }

    [Test]
    public async Task RunOnceAsync_an_out_of_range_partition_suffix_blocks_the_whole_tree()
    {
        // The other half of fail-closed, and the one a parser is most likely to
        // get wrong: a suffix that parses as a number but names no partition of
        // this tree. Clamping it, or taking it modulo the partition count, would
        // free three partitions on the strength of a value the build does not
        // understand.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            ["_lattice_materialiser_tree_leaf-1_9"] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            for (var partition = 0; partition < Partitions; partition++)
            {
                Assert.That(survivors[partition], Is.EqualTo(new[] { 0L, 1L, 2L }),
                    $"partition {partition} must retain its WAL: a suffix outside [0, {Partitions}) "
                    + "names no partition and must not be coerced into one.");
            }

            Assert.That(report.EntriesTrimmed, Is.Zero);
        });
    }

    [Test]
    public async Task RunOnceAsync_blocked_partitions_accumulate_and_the_rest_still_reclaim()
    {
        // Two quiet leaves in two different partitions. The interesting part is
        // the enumeration: the old code short-circuited on the FIRST Zero pin,
        // so a fix that kept short-circuiting would block one partition and
        // silently ignore the second - freeing WAL a second leaf genuinely
        // needs. Every blocking pin must be applied, not just the first.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            ["_lattice_materialiser_tree_leaf-1_0"] = HybridLogicalClock.Zero,
            ["_lattice_materialiser_tree_leaf-2_3"] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(survivors[0], Is.EqualTo(new[] { 0L, 1L, 2L }),
                "the first blocking pin's partition is retained.");
            Assert.That(survivors[3], Is.EqualTo(new[] { 0L, 1L, 2L }),
                "and so is the second's - a short-circuit on the first pin would have trimmed "
                + "this partition out from under a leaf that has never checkpointed.");
            Assert.That(survivors[1], Is.Empty);
            Assert.That(survivors[2], Is.Empty);
        });
    }

    [Test]
    public async Task RunOnceAsync_an_unblocked_partition_trims_no_further_than_the_tree_wide_floor()
    {
        // The safety lemma, stated as a test. The floor is NOT decomposed: an
        // unblocked partition trims against the minimum over every usable pin on
        // the tree, which is exactly what it would have trimmed against had
        // nothing been blocked. Here a second, healthy-but-lagging leaf pins the
        // tree at HLC 10 while the shipper is at 30, so a partition that trimmed
        // per-partition rather than tree-wide would trim to 30 and lose the tail
        // that lagging leaf has not read.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [QuietLeafConsumer] = HybridLogicalClock.Zero,
            ["_lattice_materialiser_tree_leaf-9_1"] = Hlc(10),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(survivors[0], Is.EqualTo(new[] { 1L, 2L }),
                "partition 0 is unblocked but must still respect the lagging leaf's pin, even "
                + "though that leaf lives in partition 1. Narrowing the floor to a partition's own "
                + "pins is the unsafe change this test forbids.");
            Assert.That(survivors[3], Is.EqualTo(new[] { 1L, 2L }));
            Assert.That(survivors[QuietPartition], Is.EqualTo(new[] { 0L, 1L, 2L }),
                "and the quiet leaf's partition is untouched.");
        });
    }

    [Test]
    public async Task RunOnceAsync_a_cold_registry_does_not_re_arm_the_whole_tree_block()
    {
        // The half of issue #2849 that makes it a liveness defect rather than a
        // transient one. The in-memory cursor registry is per-activation state,
        // so a restart presents EVERY consumer as missing and every durable pin
        // as applicable. Before the fix that meant a restart re-established the
        // whole-tree block from cold, indefinitely.
        //
        // The precondition here is established independently of the code under
        // test: the registry is simply empty, as it is after a restart, and the
        // durable pins are read from the pin grain. Nothing in the setup asserts
        // or relies on the partition attribution being correct.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [QuietLeafConsumer] = HybridLogicalClock.Zero,
            ["_lattice_materialiser_tree_leaf-7_0"] = Hlc(30),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(survivors[0], Is.Empty,
                "with no in-memory cursor at all, the healthy partitions must still reclaim "
                + "against the durable pins, or a restart strands the tree permanently.");
            Assert.That(survivors[1], Is.Empty);
            Assert.That(survivors[3], Is.Empty);
            Assert.That(survivors[QuietPartition], Is.EqualTo(new[] { 0L, 1L, 2L }));
            Assert.That(report.BlockingConsumerId, Is.EqualTo(QuietLeafConsumer));
        });
    }

    [Test]
    public async Task RunOnceAsync_a_present_consumers_durable_pin_is_still_skipped()
    {
        // Steady-state regression guard. A consumer present in the in-memory
        // registry has a fresher cursor already folded into the floor, so its
        // durable pin - Zero or otherwise - must not block anything. Decomposing
        // the block must not change which pins are considered, only which
        // partitions a considered pin reaches.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, QuietLeafConsumer, Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [QuietLeafConsumer] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        var survivors = await SurvivorsPerPartitionAsync(provider);

        Assert.Multiple(() =>
        {
            for (var partition = 0; partition < Partitions; partition++)
            {
                Assert.That(survivors[partition], Is.Empty,
                    $"partition {partition} must reclaim: the leaf is live and its in-memory "
                    + "cursor governs.");
            }

            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.Available));
            Assert.That(report.BlockingConsumerId, Is.Null);
        });
    }
}
