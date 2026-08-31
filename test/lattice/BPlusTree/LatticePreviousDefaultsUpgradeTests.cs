using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The upgrade-shaped tests: a silo started on the shipped defaults against
/// durable state that a silo running the <b>previous</b> defaults produced must
/// come up correctly, heal, and lose nothing.
/// <para>
/// This is the criterion the epic refuses to compromise on. "Fast on a fresh
/// volume, but needs a re-index on an existing one" is explicitly not done: the
/// deployments these mechanisms exist for are the ones already carrying damaged
/// state, so an upgrade path that requires an operator to rebuild is no upgrade
/// path at all. Each test therefore <i>produces</i> the durable artefact by
/// running the real code path under the previous defaults rather than
/// hand-building the shape it is assumed to have written.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticePreviousDefaultsUpgradeTests
{
    /// <summary>The tree's registry-pinned base shard count in these fixtures.</summary>
    private const int BaseShards = 64;

    /// <summary>
    /// The defaults as they stood before this epic: no binary snapshot frame, no
    /// bounded hydration, no leaf-cache pre-warm, no adaptive WAL-GC band, no
    /// shape-aware split admission, and no automatic healing. This is the
    /// configuration that produced the durable state under test.
    /// </summary>
    private static LatticeOptions PreviousDefaults() => new()
    {
        WalPartitions = 1,
        LeafSnapshotBinaryEncodingEnabled = false,
        LeafPartialHydrationEnabled = false,
        LeafCachePreWarmCount = 0,
        WalGcStartupDelay = LatticeOptions.DefaultWalGcInterval,
        WalGcMinInterval = TimeSpan.Zero,
        HotShardMinSkewRatio = 1.0d,
        HotShardMinShardEntries = 0,
        MaxPhysicalShardsPerTree = 0,
        ShardHealingEnabled = false,
    };

    /// <summary>The shipped defaults, differing from the fixtures only in the single-partition WAL the harness needs.</summary>
    private static LatticeOptions ShippedDefaults() => new() { WalPartitions = 1 };

    private static (BPlusLeafGrain Grain, ILeafSnapshotStorageGrain SnapshotStub, FakePersistentState<LeafNodeState> State)
        CreateLeaf(LatticeOptions options)
    {
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "tree-upgrade";

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            TestOptionsResolver.Create(baseOptions: options, maxLeafKeys: 128, shardCount: BaseShards, factory: grainFactory),
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());

        return (grain, snapshotStub, state);
    }

    private static Dictionary<string, string> ExpectedEntries(int count)
    {
        var expected = new Dictionary<string, string>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            expected[$"key-{i:D4}"] = $"value-{i}";
        }

        return expected;
    }

    private static Dictionary<string, string> Decode(IReadOnlyList<KeyValuePair<string, byte[]>> entries)
    {
        var actual = new Dictionary<string, string>(entries.Count, StringComparer.Ordinal);
        foreach (var (key, value) in entries)
        {
            actual[key] = Encoding.UTF8.GetString(value);
        }

        return actual;
    }

    /// <summary>
    /// Runs a leaf under the previous defaults and returns the snapshot blob it
    /// actually persisted, so the upgrade tests consume a real artefact rather
    /// than an assumption about one.
    /// </summary>
    private static async Task<LeafSnapshotBlob> CaptureUnderPreviousDefaultsAsync(int rowCount)
    {
        var (grain, stub, state) = CreateLeaf(PreviousDefaults());
        foreach (var (key, value) in ExpectedEntries(rowCount))
        {
            await grain.SetAsync(key, Encoding.UTF8.GetBytes(value));
        }

        state.State.ProjectionCheckpointOffset = 41L;

        LeafSnapshotBlob? captured = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => captured = b), Arg.Any<CancellationToken>());
        await grain.CaptureSnapshotAsync();

        Assert.That(captured, Is.Not.Null, "the previous defaults must still capture a snapshot");
        Assert.That(captured!.HasBinaryRowPayload(), Is.False,
            "the artefact under test must be the legacy shape a pre-epic build wrote");
        Assert.That(captured.EncodedRows, Is.Null);
        Assert.That(captured.Rows, Has.Count.EqualTo(rowCount));
        return captured;
    }

    // ------------------------------------------------------- durable leaf state

    [Test]
    public async Task A_leaf_snapshot_written_by_the_previous_defaults_comes_up_intact_on_the_shipped_defaults()
    {
        const int Rows = 40;
        var legacyBlob = await CaptureUnderPreviousDefaultsAsync(Rows);

        var (upgraded, stub, _) = CreateLeaf(ShippedDefaults());
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(legacyBlob));

        Assert.That(await upgraded.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True,
            "a silo on the shipped defaults must accept durable state the previous defaults wrote");

        Assert.Multiple(() =>
        {
            Assert.That(Decode(upgraded.GetEntriesAsync().GetAwaiter().GetResult()),
                Is.EqualTo(ExpectedEntries(Rows)),
                "every row must survive the upgrade; a short row set here is silent data loss");
            Assert.That(upgraded.DurableSnapshotCoverageForPartition(0), Is.EqualTo(41L),
                "coverage must be stamped from the legacy blob's own offsets");
        });
    }

    [Test]
    public async Task Bounded_hydration_declines_a_legacy_blob_and_falls_back_without_losing_a_row()
    {
        // Partial hydration needs a seekable binary frame. A legacy blob has no
        // frame at all, so the attach declines and the rehydrate takes the full
        // decode - the pre-epic path, byte for byte. The mechanism being on by
        // default must therefore be invisible to an unmigrated leaf rather than
        // a new failure mode for it.
        const int Rows = 24;
        var legacyBlob = await CaptureUnderPreviousDefaultsAsync(Rows);

        var (withHydration, hydrationStub, _) = CreateLeaf(ShippedDefaults());
        hydrationStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(legacyBlob));
        Assert.That(await withHydration.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);

        var (withoutHydration, withoutStub, _) = CreateLeaf(
            new LatticeOptions { WalPartitions = 1, LeafPartialHydrationEnabled = false });
        withoutStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(legacyBlob));
        Assert.That(await withoutHydration.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);

        Assert.That(
            Decode(await withHydration.GetEntriesAsync()),
            Is.EqualTo(Decode(await withoutHydration.GetEntriesAsync())),
            "the shipped defaults and the pre-epic hydration path must agree exactly on a legacy blob");
    }

    [Test]
    public async Task The_upgraded_leaf_heals_its_own_durable_state_on_the_next_natural_capture()
    {
        // Healing here is the lazy rewrite: no migration pass and no startup
        // rewrite, just the next capture persisting the frame instead. What must
        // hold across it is that the rewritten artefact is readable and carries
        // every row, since the coverage-gated WAL GC has been authorised to trim
        // the prefix this blob covers.
        const int Rows = 40;
        var legacyBlob = await CaptureUnderPreviousDefaultsAsync(Rows);

        var (upgraded, stub, state) = CreateLeaf(ShippedDefaults());
        stub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(legacyBlob));
        Assert.That(await upgraded.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);
        var coverageBefore = upgraded.DurableSnapshotCoverageForPartition(0);

        LeafSnapshotBlob? rewritten = null;
        await stub.SaveAsync(Arg.Do<LeafSnapshotBlob>(b => rewritten = b), Arg.Any<CancellationToken>());
        await upgraded.CaptureSnapshotAsync();

        Assert.That(rewritten, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(rewritten!.HasBinaryRowPayload(), Is.True, "the leaf must have healed itself onto the frame");
            Assert.That(rewritten.ValidateRowPayload(), Is.True);
            Assert.That(rewritten.Rows, Is.Empty, "exactly one row carrier may hold rows");
            Assert.That(rewritten.GetRowCount(), Is.EqualTo(Rows));
            Assert.That(rewritten.SnapshotOffsetsByPartition![0], Is.GreaterThanOrEqualTo(coverageBefore),
                "coverage must not regress across the rewrite");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(41L));
        });

        // The whole round trip: legacy artefact -> upgraded silo -> healed
        // artefact -> a further silo. Nothing may be lost at any hop.
        var (afterHealing, healedStub, _) = CreateLeaf(ShippedDefaults());
        healedStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(rewritten));
        Assert.That(await afterHealing.TryRehydrateFromSnapshotAsync(CancellationToken.None), Is.True);

        Assert.That(Decode(await afterHealing.GetEntriesAsync()), Is.EqualTo(ExpectedEntries(Rows)));
    }

    // -------------------------------------------------------- durable shard map

    [Test]
    public void An_over_split_tree_left_by_the_previous_defaults_is_unattended_and_then_healed()
    {
        // The measured damage shape: a tree grown far past its base by a bulk
        // ingest under admission rules that had neither a skew gate nor a shard
        // ceiling. Nothing about that durable state changes on upgrade - what
        // changes is that somebody is now looking at it.
        const int Damaged = 1_110;
        var sample = new ShardHealingSample
        {
            PhysicalShardCount = Damaged,
            BaseShardCount = BaseShards,
            SkewRatio = 1.0d,
            MedianShardOpsPerSecond = 0d,
            InFlightConsolidations = 0,
            IsSplitting = false,
            InTreeMaintenance = false,
            InCooldown = false,
        };

        Assert.Multiple(() =>
        {
            Assert.That(
                ShardHealingDecisionCore.Decide(sample, ShardHealingPolicy.FromOptions(PreviousDefaults())),
                Is.EqualTo(ShardHealingDecision.Disabled),
                "under the previous defaults the damage was permanent");
            Assert.That(
                ShardHealingDecisionCore.Decide(sample, ShardHealingPolicy.FromOptions(ShippedDefaults())),
                Is.EqualTo(ShardHealingDecision.Admitted),
                "the shipped defaults must pick the tree up with no operator action");
            Assert.That(
                ShardHealingDecisionCore.ComputeBacklog(Damaged, BaseShards),
                Is.EqualTo(Damaged - BaseShards),
                "the backlog is the machine-checkable measure of how much healing is outstanding");
        });
    }

    [Test]
    public void Healing_an_upgraded_tree_converges_on_its_base_shard_count_without_undoing_a_fold()
    {
        // A bounded drive of the real decision core, one admitted fold per sweep,
        // to show the upgrade terminates rather than merely starting. Each sweep
        // must strictly reduce the backlog and the tree must settle exactly at
        // its base, never below it.
        var policy = ShardHealingPolicy.FromOptions(ShippedDefaults());
        var shards = 128;
        var backlog = ShardHealingDecisionCore.ComputeBacklog(shards, BaseShards);
        var sweeps = 0;

        while (backlog > 0)
        {
            var decision = ShardHealingDecisionCore.Decide(
                new ShardHealingSample
                {
                    PhysicalShardCount = shards,
                    BaseShardCount = BaseShards,
                    SkewRatio = 1.0d,
                    MedianShardOpsPerSecond = 0d,
                },
                policy);

            Assert.That(decision, Is.EqualTo(ShardHealingDecision.Admitted));
            shards--;
            var next = ShardHealingDecisionCore.ComputeBacklog(shards, BaseShards);
            Assert.That(next, Is.LessThan(backlog), "every admitted sweep must reduce the outstanding work");
            backlog = next;

            Assert.That(++sweeps, Is.LessThanOrEqualTo(128), "the drive must terminate");
        }

        Assert.Multiple(() =>
        {
            Assert.That(shards, Is.EqualTo(BaseShards), "the tree settles at its base, not below it");
            Assert.That(
                ShardHealingDecisionCore.Decide(
                    new ShardHealingSample
                    {
                        PhysicalShardCount = shards,
                        BaseShardCount = BaseShards,
                        SkewRatio = 1.0d,
                        MedianShardOpsPerSecond = 0d,
                    },
                    policy),
                Is.EqualTo(ShardHealingDecision.NotOverSplit),
                "a healed tree is then left alone, so nothing oscillates");
        });
    }

    [Test]
    public void A_tree_whose_registry_pin_is_missing_is_left_alone_rather_than_folded_on_a_guess()
    {
        // An upgraded deployment may carry a tree whose registry entry predates
        // the pin. Guessing a base would fold a tree that might be correctly
        // sized, so an unknown base reports not-over-split instead.
        Assert.That(
            ShardHealingDecisionCore.Decide(
                new ShardHealingSample
                {
                    PhysicalShardCount = 1_110,
                    BaseShardCount = 0,
                    SkewRatio = 1.0d,
                    MedianShardOpsPerSecond = 0d,
                },
                ShardHealingPolicy.FromOptions(ShippedDefaults())),
            Is.EqualTo(ShardHealingDecision.NotOverSplit));
    }

    // ------------------------------------------------- durable shard-root state

    [Test]
    public void A_shard_root_carrying_no_persisted_access_model_still_warms_up_on_the_shipped_defaults()
    {
        // The previous defaults never tracked leaf access, so every upgraded
        // shard root's durable state carries a null model. Pre-warm being on by
        // default must degrade to priming nothing on the first activation and
        // rebuild from live traffic, never fail warm-up.
        var restored = LeafAccessFrequencyModel.Restore(new ShardRootState().LeafAccessModel);

        Assert.Multiple(() =>
        {
            Assert.That(restored.TrackedLeafCount, Is.Zero);
            Assert.That(restored.Observations, Is.Zero);
            Assert.That(restored.RankTopLeaves(LatticeOptions.DefaultLeafCachePreWarmCount), Is.Empty,
                "an absent model must rank nothing, so warm-up issues no priming calls at all");
            Assert.That(restored.IsDirty, Is.False, "an untouched restored model must not force a state write");
        });
    }

    [Test]
    public void The_access_model_rebuilds_from_live_traffic_after_the_upgrade()
    {
        var model = LeafAccessFrequencyModel.Restore(new ShardRootState().LeafAccessModel);
        var hot = GrainId.Create("leaf", "hot");
        var cold = GrainId.Create("leaf", "cold");

        for (var i = 0; i < 10; i++) model.Record(hot);
        model.Record(cold);

        Assert.Multiple(() =>
        {
            Assert.That(model.RankTopLeaves(1), Is.EqualTo(new[] { hot }).AsCollection,
                "the ranking must recover from live reads without any persisted history");
            Assert.That(model.IsDirty, Is.True, "the rebuilt model must be persisted on the next flush");
        });
    }
}
