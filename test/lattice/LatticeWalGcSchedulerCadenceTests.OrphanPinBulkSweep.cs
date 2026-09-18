using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the bulk orphan-pin sweep added by issue #3105.
/// <para>
/// An orphaned durable materialiser pin - one whose leaf was reclaimed or
/// purged after the pin was written - could previously only be retired as a
/// by-product of a reactivation touch. That path inherits every bound sized for
/// driving a <i>live but stuck</i> leaf: it sees at most
/// <c>LatticeWalGc.MaxReportedBlockingConsumers</c> blockers, waits out a
/// minimum block age, and spends one of a handful of attempts followed by a
/// retry cooldown. Each of those is justified for a live leaf and none of them
/// applies to a row whose publisher no longer exists.
/// </para>
/// <para>
/// The resulting ceiling was measured at 63 pins an hour against a backlog of
/// 9,468 on one tree: a 6.3-day drain during which the trim floor - a minimum
/// over <i>every</i> pin - reclaimed nothing whatsoever and 11.7 GB of WAL
/// stayed resident. A backlog that is 99% drained trims exactly as much as one
/// that is untouched, which is why "it is making progress" was never evidence
/// that it would converge in a useful time.
/// </para>
/// <para>
/// These tests pin both halves of the fix: the classifier must be able to
/// <i>name</i> an orphan (it could not - a husk retains its checkpoint offset,
/// so it classified as the repairable state and an estate of thousands
/// presented as a coverage problem), and the sweep must be able to
/// <i>drain</i> one in bulk, off the reactivation budget entirely.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private const string OrphanSweepTree = "orphan-sweep";

    /// <summary>
    /// An in-memory stand-in for the durable pin store, serving a distinct
    /// grain per shard key and recording every removal so the sweep's effect is
    /// observable rather than inferred.
    /// </summary>
    private sealed class FakePinStore
    {
        private readonly Dictionary<string, FakePinGrain> _grains = new(StringComparer.Ordinal);

        public List<(string Key, string ConsumerId)> Removals { get; } = [];

        public List<string> KeysRead { get; } = [];

        public Exception? ReadThrowsFor { get; set; }

        public void Seed(string key, string consumerId)
        {
            if (!_grains.TryGetValue(key, out var grain))
            {
                grain = new FakePinGrain(this, key);
                _grains[key] = grain;
            }

            grain.Pins[consumerId] = HybridLogicalClock.Zero;
        }

        /// <summary>
        /// Seeds a pin at an explicit frontier. The durable materialiser offset
        /// floor is a minimum over every pin, so the frontier is what decides
        /// which pins are actually holding it - and therefore which ones the
        /// bounded floor-holder sample of issue #3158 must pick.
        /// </summary>
        public void Seed(string key, string consumerId, HybridLogicalClock frontier)
        {
            if (!_grains.TryGetValue(key, out var grain))
            {
                grain = new FakePinGrain(this, key);
                _grains[key] = grain;
            }

            grain.Pins[consumerId] = frontier;
        }

        public IWalMaterialiserPinGrain For(string key)
        {
            if (!_grains.TryGetValue(key, out var grain))
            {
                grain = new FakePinGrain(this, key);
                _grains[key] = grain;
            }

            return grain;
        }

        public int RemainingPins => _grains.Values.Sum(g => g.Pins.Count);

        private sealed class FakePinGrain(FakePinStore store, string key) : IWalMaterialiserPinGrain
        {
            public Dictionary<string, HybridLogicalClock> Pins { get; } = new(StringComparer.Ordinal);

            public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync()
            {
                store.KeysRead.Add(key);

                if (store.ReadThrowsFor is { } ex)
                {
                    throw ex;
                }

                return Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                    new Dictionary<string, HybridLogicalClock>(Pins, StringComparer.Ordinal));
            }

            public Task RemoveAsync(string consumerId)
            {
                store.Removals.Add((key, consumerId));
                Pins.Remove(consumerId);
                return Task.CompletedTask;
            }

            public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
                Task.FromResult<IReadOnlyDictionary<string, long>>(
                    new Dictionary<string, long>(StringComparer.Ordinal));

            public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => Task.CompletedTask;

            public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Task.CompletedTask;

            public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Task.CompletedTask;

            public Task ClearAsync()
            {
                Pins.Clear();
                return Task.CompletedTask;
            }
        }
    }

    /// <summary>
    /// A storage provider serving a per-leaf canned state, so a single sweep
    /// can span live leaves, husks and leaves with no record at all - which is
    /// the population the fix has to tell apart.
    /// </summary>
    private sealed class LeafStateBook : IGrainStorage
    {
        private readonly Dictionary<GrainId, LeafNodeState?> _states = [];

        public Exception? Throws { get; set; }

        public int Reads { get; private set; }

        public void PutLive(GrainId leaf, string treeId) =>
            _states[leaf] = new LeafNodeState
            {
                TreeId = treeId,
                ProjectionCheckpointOffset = 1234,
                ProjectionCheckpointOffsetsByPartition = [1234],
            };

        /// <summary>
        /// A husk: the state blob survives with its last checkpoint offset
        /// intact, but its tree id has been cleared. This is the shape a
        /// reclaimed leaf actually leaves behind, and the reason the checkpoint
        /// alone cannot classify it - the offset says "repairable" while the
        /// missing tree id says "there is nothing left to repair".
        /// </summary>
        public void PutHusk(GrainId leaf) =>
            _states[leaf] = new LeafNodeState
            {
                TreeId = null,
                ProjectionCheckpointOffset = 1234,
                ProjectionCheckpointOffsetsByPartition = [1234],
            };

        public void PutMissing(GrainId leaf) => _states[leaf] = null;

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Reads++;

            if (Throws is { } ex)
            {
                throw ex;
            }

            if (_states.TryGetValue(grainId, out var state)
                && state is not null
                && grainState is IGrainState<LeafNodeState> leafState)
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

    private static GrainId OrphanLeafGrainId(int ordinal) =>
        GrainId.Create("bplusleaf", "leaf-3105-" + ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static string OrphanConsumerId(int ordinal, string treeId = OrphanSweepTree) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{OrphanLeafGrainId(ordinal)}";

    private static LatticeOptions OrphanSweepOptions(int pinShards = 1)
    {
        var options = Adaptive();
        options.WalMaterialiserPinShards = pinShards;
        return options;
    }

    /// <summary>
    /// Wires a grain factory that serves the tree registry, a blocked leaf and
    /// the fake pin store, and returns a scheduler primed to sweep
    /// <see cref="OrphanSweepTree"/> on its first pass.
    /// </summary>
    private static LatticeWalGcScheduler SchedulerSweeping(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        int pinShards = 1,
        string? blockingConsumerId = null)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(
                BlockedReportNaming(blockingConsumerId ?? OrphanConsumerId(0))));

        var (factory, _) = FactoryWithBlockedLeaf(OrphanSweepTree);
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return CreateScheduler(factory, gc, OrphanSweepOptions(pinShards), time, leafStateStorage: storage);
    }

    // --------------------------------------------------------- classification

    [Test]
    public async Task A_husk_leaf_is_classified_as_orphaned_rather_than_repairable()
    {
        // The diagnostic half of #3105, and the half that made the drain half
        // invisible. ClassifyCheckpoint reads only the projection checkpoint,
        // which a husk retains, so before the tree-id branch existed every
        // orphan landed in 'checkpointed_uncovered' - the arm that says "a
        // snapshot would fix this". Nine thousand pins with nothing left to
        // snapshot were reported as a coverage problem.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        storage.PutHusk(OrphanLeafGrainId(0));

        var pins = new FakePinStore();
        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var counted = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(counted, Does.Contain("orphaned"),
                "a state blob with no bound tree id proves the leaf was reclaimed after the pin was written, "
                    + "because pin registration is birth-gated on a persisted tree id.");
            Assert.That(counted, Does.Not.Contain("checkpointed_uncovered"),
                "the husk's retained checkpoint offset must not be read as a repairable coverage hole - that "
                    + "misclassification is what concealed the backlog.");
        });
    }

    [Test]
    public async Task A_leaf_that_still_holds_its_tree_id_is_classified_by_its_checkpoint()
    {
        // The over-reach guard. Reading the tree id first must not swallow the
        // two states that describe a real leaf, or the orphan arm would absorb
        // the very population the classifier exists to adjudicate.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        storage.PutLive(OrphanLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var counted = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(counted, Does.Contain("checkpointed_uncovered"));
            Assert.That(counted, Does.Not.Contain("orphaned"));
        });
    }

    // ----------------------------------------------------------- the draining

    [Test]
    public async Task The_sweep_retires_far_more_orphans_in_one_pass_than_the_floor_can_report()
    {
        // The defect, stated as a test. The floor names at most
        // MaxReportedBlockingConsumers (8) blockers, and before this sweep
        // existed that report was the sweep's only input - so no pass could
        // retire a ninth pin even in principle, however many were orphaned.
        const int Orphans = 40;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Orphans; i++)
        {
            storage.PutHusk(OrphanLeafGrainId(i));
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));
        }

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var retired = sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "retired")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(pins.RemainingPins, Is.Zero,
                "every orphaned pin must be gone after a single sweep; the trim floor is a minimum over all "
                    + "of them, so a backlog that is 39/40 drained retains exactly as much WAL as an "
                    + "untouched one.");
            Assert.That(retired, Is.EqualTo(Orphans));
            Assert.That(Orphans, Is.GreaterThan(8),
                "the population must exceed the floor's blocking-report cap, or this test passes without "
                    + "exercising the defect at all.");
        });
    }

    [Test]
    public async Task The_sweep_never_retires_a_pin_whose_leaf_is_still_live()
    {
        // The safety property the whole change rests on. Retiring a live
        // leaf's pin authorises a trim over a WAL prefix that leaf has not
        // replayed, which is data loss - strictly worse than the retention the
        // sweep exists to relieve.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();

        for (var i = 0; i < 6; i++)
        {
            if (i % 2 == 0)
            {
                storage.PutHusk(OrphanLeafGrainId(i));
            }
            else
            {
                storage.PutLive(OrphanLeafGrainId(i), OrphanSweepTree);
            }

            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));
        }

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var removed = pins.Removals.Select(r => r.ConsumerId).ToArray();
        var live = sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "live")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EquivalentTo(new[]
            {
                OrphanConsumerId(0), OrphanConsumerId(2), OrphanConsumerId(4),
            }), "only the husks may be retired.");
            Assert.That(live, Is.EqualTo(3), "the live leaves must be counted, not silently skipped.");
            Assert.That(pins.RemainingPins, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task The_sweep_fails_closed_when_a_leaf_state_read_throws()
    {
        // An unreadable leaf is a failure of the instrument, not evidence about
        // the leaf. Treating it as an orphan would turn a transient provider
        // fault into a mass retirement of live pins.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook { Throws = new InvalidOperationException("provider down") };
        var pins = new FakePinStore();
        for (var i = 0; i < 5; i++)
        {
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));
        }

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var unreadable = sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "unreadable")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(pins.Removals, Is.Empty, "no pin may be retired on a read that failed.");
            Assert.That(unreadable, Is.EqualTo(5), "the failure must be counted so it is diagnosable.");
        });
    }

    [Test]
    public async Task The_sweep_does_not_run_at_all_without_a_storage_provider()
    {
        // Without a provider there is no way to distinguish an orphan from a
        // live leaf, and the sweep's entire authority to delete rests on that
        // distinction. It must decline rather than guess.
        var time = new VirtualTimeProvider();
        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));

        var scheduler = SchedulerSweeping(pins, storage: null, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pins.Removals, Is.Empty);
            Assert.That(pins.KeysRead, Is.Empty,
                "the sweep must decline before it reads the pin store, not after.");
        });
    }

    [Test]
    public async Task The_sweep_retires_a_pin_from_every_key_it_was_found_under()
    {
        // A pin stranded at a key written under an earlier routing still floors
        // the trim, because the floor reads the union of every read key
        // (issue #2433). Removing it from only one of them leaves the tree
        // blocked by a row the sweep has already declared dead.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        storage.PutHusk(OrphanLeafGrainId(0));

        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(OrphanSweepTree, shardCount: 8);
        var pins = new FakePinStore();
        pins.Seed(keys[0], OrphanConsumerId(0));
        pins.Seed(keys[^1], OrphanConsumerId(0));

        var scheduler = SchedulerSweeping(pins, storage, time, pinShards: 8);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(keys, Has.Count.GreaterThan(2),
                "eight shards must enumerate more keys than the two seeded, or the precision claim below is "
                    + "vacuous.");
            Assert.That(pins.Removals.Select(r => r.Key), Is.EquivalentTo(new[] { keys[0], keys[^1] }),
                "removal must address exactly the keys the pin was found under - no fewer, so no copy "
                    + "survives to floor the trim, and no more, since the sweep enumerated the store and "
                    + "does not need to guess.");
            Assert.That(pins.RemainingPins, Is.Zero);
        });
    }

    [Test]
    public async Task Every_orphan_sweep_arm_is_primed_for_a_tree_that_holds_no_orphans()
    {
        // Without priming, "the sweep found nothing" and "the sweep is not
        // running on this build" export the same thing: no series at all. That
        // ambiguity is exactly what let a six-day stall read as healthy.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        storage.PutLive(OrphanLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var arms = sweep.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string)
            .Distinct()
            .ToArray();

        Assert.That(arms, Is.EquivalentTo(new[] { "retired", "deferred", "live", "unresolved", "unreadable" }),
            "every arm must be minted for a tree the sweep reached, so a zero is a measured zero.");
    }

    [Test]
    public async Task The_sweep_reports_a_backlog_it_could_not_finish_as_deferred()
    {
        // 'deferred' is the backlog signal, and the reason this instrument is a
        // counter rather than a gauge. Before it existed the only orphan series
        // was the reactivation sweep's monotonically-advancing 'orphaned'
        // count, on which 63 pins an hour against a 9,468-pin backlog is
        // indistinguishable from steady healthy progress.
        const int Orphans = 600;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Orphans; i++)
        {
            storage.PutHusk(OrphanLeafGrainId(i));
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));
        }

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var retired = sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "retired")
            .Sum(m => m.Value);
        var deferred = sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "deferred")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(retired, Is.EqualTo(512), "the per-pass retirement budget must be honoured.");
            Assert.That(deferred, Is.EqualTo(Orphans - 512),
                "what the budget could not reach must be reported, or a partially-drained backlog is "
                    + "indistinguishable from a drained one.");
            Assert.That(retired + deferred, Is.EqualTo(Orphans),
                "the arms must partition the examined population, or sum-by-tree is not the pin count.");
        });
    }
}
