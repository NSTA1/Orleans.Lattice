using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Cluster-free coverage for the parts of <see cref="SnapshotLeafGrain"/> that only run
/// off the happy path: the legacy from-zero write-ahead-log replay, the coordinate
/// guards that refuse to re-open or re-seed an activation against a different snapshot,
/// the durable-baseline sliding TTL, the expired-baseline failure, and the donor-orphan
/// ownership filter.
/// <para>
/// A snapshot leaf serves a point-in-time view, so every one of these paths exists to
/// keep that view honest: replaying the wrong coordinate, resurrecting a key a split
/// migrated away, or silently falling back to a from-zero replay over a
/// garbage-collected log would each surface as a plausible-looking but wrong answer
/// rather than as an error. They are exercised here directly because a cluster fixture
/// cannot force a reactivation, a lost baseline, or a legacy coordinate on demand.
/// </para>
/// </summary>
[TestFixture]
public sealed class SnapshotLeafGrainReplayAndLifecycleTests
{
    private const string TreeId = "snap-leaf-tree";
    private const int ShardIndex = 0;
    private static readonly Guid Token = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly long[] Offsets = [5L];

    private sealed class Harness
    {
        public required SnapshotLeafGrain Grain { get; init; }
        public required IGrainFactory GrainFactory { get; init; }
        public required ISnapshotBaselineStorageGrain Storage { get; init; }
        public required ILeafReplayCoordinatorGrain Replay { get; init; }
        public required LatticeOptions Options { get; init; }
    }

    private static Harness CreateHarness(ILogger<SnapshotLeafGrain>? logger = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("snapshot-leaf", $"{TreeId}/{ShardIndex}"));
        context.ActivationServices.GetService(typeof(CrdtShapeRegistry)).Returns(new CrdtShapeRegistry());
        context.ActivationServices.GetService(typeof(ILatticeEnvelopeCodec)).Returns(null);

        var grainFactory = Substitute.For<IGrainFactory>();
        var storage = Substitute.For<ISnapshotBaselineStorageGrain>();
        var replay = Substitute.For<ILeafReplayCoordinatorGrain>();
        grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(Arg.Any<string>()).Returns(storage);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(replay);
        replay.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]));

        var options = new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(options);
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        return new Harness
        {
            Grain = AttachContext(
                new SnapshotLeafGrain(
                    context, grainFactory, optionsMonitor, logger ?? NullLogger<SnapshotLeafGrain>.Instance),
                context),
            GrainFactory = grainFactory,
            Storage = storage,
            Replay = replay,
            Options = options,
        };
    }

    /// <summary>
    /// Attaches the activation context to the <see cref="Grain"/> base, which Orleans
    /// normally does after construction. Without it every diagnostic that names the
    /// grain by primary key - which is every guard message on this grain - faults with
    /// a null reference before it can throw the real error. Test-only reflection; no
    /// production code changes.
    /// </summary>
    /// <param name="grain">The directly-constructed grain.</param>
    /// <param name="context">The activation context to attach.</param>
    /// <returns>The same grain, for fluent construction.</returns>
    private static SnapshotLeafGrain AttachContext(SnapshotLeafGrain grain, IGrainContext context)
    {
        const System.Reflection.BindingFlags Flags =
            System.Reflection.BindingFlags.Instance
            | System.Reflection.BindingFlags.NonPublic
            | System.Reflection.BindingFlags.Public;

        var property = typeof(Grain).GetProperty("GrainContext", Flags);
        if (property?.SetMethod is not null)
        {
            property.SetValue(grain, context);
            return grain;
        }

        typeof(Grain).GetField("<GrainContext>k__BackingField", Flags)?.SetValue(grain, context);
        return grain;
    }

    private static HybridLogicalClock Clock(long wall) => new() { WallClockTicks = wall, Counter = 0 };

    private static LeafSnapshotRow Row(string key, byte[] value) =>
        new(key, new LwwValue<byte[]> { Value = value, Timestamp = Clock(100), IsTombstone = false });

    private static SnapshotShardBaseline Baseline(params LeafSnapshotRow[] rows) => new()
    {
        Rows = rows,
        CapturedHeadPerPartition = Offsets,
        CapturedAtTicks = 1000,
        RowBytes = 0,
    };

    private static CommitLogSliceEntry Slice(long offset, LatticeMutation mutation) => new(offset, mutation);

    private static LatticeMutation Set(string key, byte[] value, long ts, int shardIndex = ShardIndex) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = Clock(ts),
        ShardIndex = shardIndex,
    };

    // ------------------------------------------------------------------
    // Legacy from-zero WAL replay (an empty baseline token).
    // ------------------------------------------------------------------

    [Test]
    public async Task OpenAsync_with_a_legacy_coordinate_replays_the_wal_from_zero()
    {
        var harness = CreateHarness();
        harness.Replay
            .ReadSliceAsync(-1L, 4L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
            [
                Slice(0, Set("a", [1], 10)),
                Slice(1, Set("b", [2], 20)),
            ]));

        // Guid.Empty is the pre-frozen-baseline wire coordinate: there is no durable
        // baseline to reload, so the view has to be rebuilt from the log.
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Guid.Empty, CancellationToken.None);

        var keys = await harness.Grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
        await harness.Storage.DidNotReceive().LoadAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_legacy_replay_skips_a_partition_whose_captured_offset_is_zero()
    {
        var harness = CreateHarness();

        await harness.Grain.OpenAsync(TreeId, ShardIndex, [0L], null, 0, Guid.Empty, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.Empty);
        await harness.Replay.DidNotReceiveWithAnyArgs().ReadSliceAsync(default, default, default, default);
    }

    [Test]
    public async Task A_legacy_replay_defers_saga_terminals_until_every_partition_is_absorbed()
    {
        var harness = CreateHarness();
        var txId = Guid.NewGuid();
        harness.Replay
            .ReadSliceAsync(-1L, 4L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
            [
                // The terminal precedes its own prepare in log order. Applying it in
                // place would drain an empty bucket and lose the committed write.
                Slice(0, new LatticeMutation
                {
                    TreeId = TreeId,
                    Kind = MutationKind.TxCommit,
                    TransactionId = txId,
                    ShardIndex = ShardIndex,
                }),
                Slice(1, Set("k", [7], 30) with { TransactionId = txId, IsPrepared = true }),
            ]));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Guid.Empty, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.EqualTo(new[] { "k" }),
            "The deferred terminal must run after the prepare, so the saga's write becomes visible.");
    }

    [Test]
    public async Task A_legacy_replay_drops_a_record_stamped_for_a_different_shard()
    {
        var harness = CreateHarness();
        harness.Replay
            .ReadSliceAsync(-1L, 4L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
            [
                Slice(0, Set("mine", [1], 10)),
                Slice(1, Set("theirs", [2], 20, shardIndex: ShardIndex + 1)),
            ]));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Guid.Empty, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.EqualTo(new[] { "mine" }));
    }

    [Test]
    public async Task A_legacy_replay_stops_when_a_slice_fails_to_advance()
    {
        var harness = CreateHarness();
        var calls = 0;
        harness.Replay
            .ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                calls++;
                // A slice whose last offset does not advance the cursor would otherwise
                // loop forever; the defensive break is what stops a silo spinning.
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([Slice(-1, Set("a", [1], 10))]);
            });

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Guid.Empty, CancellationToken.None);

        Assert.That(calls, Is.EqualTo(1), "A non-advancing slice must break the loop, not retry forever.");
    }

    [Test]
    public async Task A_legacy_replay_reads_every_partition_of_a_multi_partition_log()
    {
        var harness = CreateHarness();
        harness.GrainFactory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/0").Returns(harness.Replay);
        var second = Substitute.For<ILeafReplayCoordinatorGrain>();
        harness.GrainFactory.GetGrain<ILeafReplayCoordinatorGrain>($"{TreeId}/1").Returns(second);
        harness.Replay.ReadSliceAsync(-1L, 1L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([Slice(0, Set("p0", [1], 10))]));
        second.ReadSliceAsync(-1L, 2L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([Slice(0, Set("p1", [2], 20))]));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, [2L, 3L], null, 0, Guid.Empty, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.EqualTo(new[] { "p0", "p1" }));
    }

    // ------------------------------------------------------------------
    // Coordinate guards.
    // ------------------------------------------------------------------

    [Test]
    public async Task OpenAsync_refuses_to_re_open_an_opened_leaf_against_a_different_coordinate()
    {
        var harness = CreateHarness();
        await harness.Grain.SeedAsync(TreeId, ShardIndex, Baseline(Row("a", [1])), Token, CancellationToken.None);
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        // A different coordinate belongs to a different grain key, so reaching this
        // activation with one is an upstream routing defect that must not be papered
        // over by silently serving the first snapshot's rows.
        Assert.That(
            async () => await harness.Grain.OpenAsync(
                TreeId, ShardIndex, [99L], null, 0, Token, CancellationToken.None),
            Throws.InvalidOperationException.With.Message.Contains("refusing to re-open"));
    }

    [Test]
    public async Task OpenAsync_refuses_to_open_a_seeded_leaf_against_a_different_baseline_token()
    {
        var harness = CreateHarness();
        await harness.Grain.SeedAsync(TreeId, ShardIndex, Baseline(Row("a", [1])), Token, CancellationToken.None);

        Assert.That(
            async () => await harness.Grain.OpenAsync(
                TreeId, ShardIndex, Offsets, null, 0, Guid.NewGuid(), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.Contains("refusing to open"));
    }

    [Test]
    public async Task SeedAsync_refuses_to_re_seed_a_materialised_leaf_against_a_different_coordinate()
    {
        var harness = CreateHarness();
        await harness.Grain.SeedAsync(TreeId, ShardIndex, Baseline(Row("a", [1])), Token, CancellationToken.None);

        Assert.That(
            async () => await harness.Grain.SeedAsync(
                TreeId, ShardIndex, Baseline(Row("b", [2])), Guid.NewGuid(), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.Contains("refusing to re-seed"));
    }

    // ------------------------------------------------------------------
    // Durable baseline: expiry, flush skip, and the sliding TTL.
    // ------------------------------------------------------------------

    [Test]
    public void OpenAsync_surfaces_a_lost_baseline_as_an_expired_snapshot()
    {
        var harness = CreateHarness();
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<SnapshotShardBaseline?>(null));

        // Falling back to a from-zero replay here would risk an empty or partial view
        // over a GC-trimmed log - a silently wrong answer - so the open must fail.
        Assert.That(
            async () => await harness.Grain.OpenAsync(
                TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<LatticeSnapshotExpiredException>());
    }

    [Test]
    public async Task EnsurePersistedAsync_is_a_no_op_for_a_legacy_replay_leaf()
    {
        var harness = CreateHarness();
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Guid.Empty, CancellationToken.None);

        await harness.Grain.EnsurePersistedAsync(CancellationToken.None);

        await harness.Storage.DidNotReceive().SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsurePersistedAsync_is_a_no_op_for_a_reloaded_leaf_that_is_already_durable()
    {
        var harness = CreateHarness();
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>()).Returns(Baseline(Row("a", [1])));
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        await harness.Grain.EnsurePersistedAsync(CancellationToken.None);

        await harness.Storage.DidNotReceive().SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_re_open_slides_the_durable_baseline_ttl_while_the_scan_is_still_paging()
    {
        var harness = CreateHarness();
        // A short TTL puts the touch throttle's floor behind us immediately, so the
        // second page's re-open is due for a refresh.
        harness.Options.SnapshotBaselineTtl = TimeSpan.FromMinutes(10);
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>()).Returns(Baseline(Row("a", [1])));
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        SetLastTouch(harness.Grain, DateTime.UtcNow - TimeSpan.FromHours(1));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        await harness.Storage.Received(1).TouchAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_ttl_slide_that_fails_does_not_fail_the_page()
    {
        var harness = CreateHarness();
        harness.Options.SnapshotBaselineTtl = TimeSpan.FromMinutes(10);
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>()).Returns(Baseline(Row("a", [1])));
        harness.Storage.TouchAsync(Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("the baseline store is unreachable"));
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        SetLastTouch(harness.Grain, DateTime.UtcNow - TimeSpan.FromHours(1));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.EqualTo(new[] { "a" }),
            "A failed slide only risks an early expiry; it must not fail the page being served.");
    }

    [Test]
    public async Task A_disabled_baseline_ttl_suppresses_the_slide_entirely()
    {
        var harness = CreateHarness();
        harness.Options.SnapshotBaselineTtl = Timeout.InfiniteTimeSpan;
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>()).Returns(Baseline(Row("a", [1])));
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        SetLastTouch(harness.Grain, DateTime.UtcNow - TimeSpan.FromHours(1));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        await harness.Storage.DidNotReceive().TouchAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_re_open_inside_the_throttle_window_does_not_rewrite_the_reminder()
    {
        var harness = CreateHarness();
        harness.Options.SnapshotBaselineTtl = TimeSpan.FromHours(1);
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>()).Returns(Baseline(Row("a", [1])));
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        await harness.Storage.DidNotReceive().TouchAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsurePersistedAsync_is_a_no_op_after_an_open_that_could_not_load_its_baseline()
    {
        var harness = CreateHarness();
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<SnapshotShardBaseline?>(null));
        Assert.That(
            async () => await harness.Grain.OpenAsync(
                TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<LatticeSnapshotExpiredException>());

        // The failed open recorded the coordinate but never produced a baseline, so a
        // flush has nothing to write and must not save an empty view over the real one.
        await harness.Grain.EnsurePersistedAsync(CancellationToken.None);

        await harness.Storage.DidNotReceive().SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------------------
    // Donor-orphan ownership filter.
    // ------------------------------------------------------------------

    [Test]
    public async Task A_reloaded_baseline_drops_keys_the_pinned_map_says_this_shard_no_longer_owns()
    {
        var harness = CreateHarness();
        harness.Storage.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Baseline(Row("a", [1]), Row("b", [2]), Row("c", [3])));
        const int virtualShardCount = 16;
        var owned = new[] { ShardMap.GetVirtualSlot("a", virtualShardCount) };
        Array.Sort(owned);

        await harness.Grain.OpenAsync(
            TreeId, ShardIndex, Offsets, owned, virtualShardCount, Token, CancellationToken.None);

        var keys = await harness.Grain.GetKeysAsync();
        Assert.That(keys, Does.Contain("a"));
        Assert.That(
            keys.Where(k => !owned.Contains(ShardMap.GetVirtualSlot(k, virtualShardCount))),
            Is.Empty,
            "A key an adaptive split migrated away must be surfaced only by its pinned-map owner.");
    }

    [Test]
    public async Task A_legacy_replay_resolves_ownership_through_the_pinned_map_when_one_is_supplied()
    {
        var harness = CreateHarness();
        const int virtualShardCount = 16;
        var owned = new[] { ShardMap.GetVirtualSlot("a", virtualShardCount) };
        Array.Sort(owned);
        harness.Replay
            .ReadSliceAsync(-1L, 4L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
            [
                // Stamped for a sibling shard but owned by this one under the pinned
                // map: a shadow-forwarded record that must still be applied here.
                Slice(0, Set("a", [1], 10, shardIndex: ShardIndex + 5)),
            ]));

        await harness.Grain.OpenAsync(
            TreeId, ShardIndex, Offsets, owned, virtualShardCount, Guid.Empty, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.EqualTo(new[] { "a" }));
    }

    // ------------------------------------------------------------------
    // Reader guards and scan bounds.
    // ------------------------------------------------------------------

    [Test]
    public void The_readers_refuse_to_serve_a_leaf_that_was_never_opened()
    {
        var harness = CreateHarness();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await harness.Grain.GetKeysAsync(),
                Throws.InvalidOperationException.With.Message.Contains("has not been opened"));
            Assert.That(async () => await harness.Grain.GetEntriesAsync(),
                Throws.InvalidOperationException.With.Message.Contains("has not been opened"));
            Assert.That(async () => await harness.Grain.GetRawEntriesAsync(),
                Throws.InvalidOperationException.With.Message.Contains("has not been opened"));
        });
    }

    [Test]
    public async Task Every_reader_stops_early_at_an_exclusive_upper_bound()
    {
        var grain = await SeededOpenGrainAsync(Row("a", [1]), Row("b", [2]), Row("c", [3]), Row("d", [4]));

        Assert.Multiple(async () =>
        {
            Assert.That((await grain.GetEntriesAsync(endExclusive: "c")).Select(e => e.Key),
                Is.EqualTo(new[] { "a", "b" }));
            Assert.That((await grain.GetEntriesAsync(beforeExclusive: "c")).Select(e => e.Key),
                Is.EqualTo(new[] { "a", "b" }));
            Assert.That((await grain.GetRawEntriesAsync(endExclusive: "c")).Select(e => e.Key),
                Is.EqualTo(new[] { "a", "b" }));
            Assert.That((await grain.GetRawEntriesAsync(beforeExclusive: "c")).Select(e => e.Key),
                Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task Every_reader_honours_a_forward_and_a_reverse_limit()
    {
        var grain = await SeededOpenGrainAsync(Row("a", [1]), Row("b", [2]), Row("c", [3]));

        Assert.Multiple(async () =>
        {
            Assert.That((await grain.GetEntriesAsync(limit: 2)).Select(e => e.Key),
                Is.EqualTo(new[] { "a", "b" }));
            Assert.That((await grain.GetEntriesAsync(limit: 2, reverse: true)).Select(e => e.Key),
                Is.EqualTo(new[] { "b", "c" }));
            Assert.That((await grain.GetRawEntriesAsync(limit: 2)).Select(e => e.Key),
                Is.EqualTo(new[] { "a", "b" }));
            Assert.That((await grain.GetRawEntriesAsync(limit: 2, reverse: true)).Select(e => e.Key),
                Is.EqualTo(new[] { "b", "c" }));
        });
    }

    // ------------------------------------------------------------------
    // Debug diagnostics.
    // ------------------------------------------------------------------

    [Test]
    public async Task The_seed_and_flush_lifecycle_emits_its_debug_diagnostics()
    {
        var sink = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(sink);
        });
        var harness = CreateHarness(loggerFactory.CreateLogger<SnapshotLeafGrain>());

        await harness.Grain.SeedAsync(TreeId, ShardIndex, Baseline(Row("a", [1])), Token, CancellationToken.None);
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        await harness.Grain.EnsurePersistedAsync(CancellationToken.None);

        await harness.Storage.Received(1).SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(sink.Messages, Has.Some.Contains("seeded in memory"));
            Assert.That(sink.Messages, Has.Some.Contains("flushed frozen baseline"));
        });
    }

    [Test]
    public async Task A_legacy_replay_emits_its_open_diagnostics()
    {
        var sink = new CapturingLoggerProvider();
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(sink);
        });
        var harness = CreateHarness(loggerFactory.CreateLogger<SnapshotLeafGrain>());
        harness.Replay
            .ReadSliceAsync(-1L, 4L, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([Slice(0, Set("a", [1], 10))]));

        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Guid.Empty, CancellationToken.None);

        Assert.That(await harness.Grain.GetKeysAsync(), Is.EqualTo(new[] { "a" }));
        Assert.That(sink.Messages, Has.Some.Contains("SnapshotLeafGrain opened"));
    }

    /// <summary>
    /// A minimal logger provider that records formatted messages, so a diagnostic the
    /// grain only emits under an <c>IsEnabled(Debug)</c> guard is both executed and
    /// asserted. A logger factory with no provider reports Debug as disabled, so the
    /// guarded arms would otherwise never run.
    /// </summary>
    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly List<string> _messages = [];

        internal IReadOnlyList<string> Messages
        {
            get
            {
                lock (_messages)
                {
                    return _messages.ToArray();
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(_messages);

        public void Dispose()
        {
        }

        private sealed class CapturingLogger(List<string> messages) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (messages)
                {
                    messages.Add(formatter(state, exception));
                }
            }
        }
    }

    private static async Task<SnapshotLeafGrain> SeededOpenGrainAsync(params LeafSnapshotRow[] rows)
    {
        var harness = CreateHarness();
        await harness.Grain.SeedAsync(TreeId, ShardIndex, Baseline(rows), Token, CancellationToken.None);
        await harness.Grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        return harness.Grain;
    }

    /// <summary>
    /// Rewinds the grain's last-touch stamp so the sliding-TTL throttle is due without
    /// the test having to wait out a real interval. Test-only reflection over the
    /// grain's own field; no production code changes.
    /// </summary>
    /// <param name="grain">The grain whose throttle to rewind.</param>
    /// <param name="value">The instant to record as the last touch.</param>
    private static void SetLastTouch(SnapshotLeafGrain grain, DateTime value) =>
        typeof(SnapshotLeafGrain)
            .GetField("_lastTtlTouchUtc", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .SetValue(grain, value);
}
