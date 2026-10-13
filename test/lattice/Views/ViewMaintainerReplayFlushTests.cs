using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression coverage for a replay of a pass whose atomic batch already
/// flushed. The filter path applies a pass's ordinary survivors with blind
/// point writes and then flushes each completed atomic batch through a view
/// saga keyed by the deterministic <c>mv-tx-{txId}</c> operation id. When the
/// pass is replayed - a lost flush response, or a crash before the checkpoint
/// persists - the replay re-applies the lower-HLC ordinary write blindly while
/// the batch flush re-attaches to the already-completed saga as a no-op. Before
/// the fix nothing re-asserted the batch, so the view was left permanently at
/// the older ordinary value while the source held the batch value.
/// <para>
/// The grain is driven directly (the pattern of
/// <see cref="ViewMaintainerAggregationDrainTests"/>) with a stub WAL subscriber
/// and a fake view tree whose atomic writes are deduplicated by operation id,
/// exactly as the real saga re-attach behaves.
/// </para>
/// </summary>
[TestFixture]
public partial class ViewMaintainerReplayFlushTests
{
    private const string ViewName = "orders-by-key";
    private const string SourceTreeId = "orders";
    private const string Key = "k";

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static LatticeMutation Ordinary(byte value, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.Set,
        Key = Key,
        Value = [value],
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
    };

    private static LatticeMutation Prepared(Guid txId, byte value, long ticks) => Prepared(txId, Key, value, ticks, size: 1, index: 0);

    private static LatticeMutation Prepared(Guid txId, string key, byte value, long ticks, int size, int index) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [value],
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
        IsPrepared = true,
        TransactionId = txId,
        AtomicBatchSize = size,
        AtomicBatchIndex = index,
    };

    private static LatticeMutation RangeDelete(string start, string endExclusive, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.DeleteRange,
        Key = start,
        EndExclusiveKey = endExclusive,
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
    };

    private static LatticeMutation Commit(Guid txId, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.TxCommit,
        Key = string.Empty,
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
        TransactionId = txId,
    };

    private static Harness Create(
        FakeViewTree view,
        FakePersistentState<ViewCheckpointState> state,
        ViewRegistration? peer = null,
        IViewCrossTreeCoordinatorGrain? coordinator = null,
        ViewRegistration? registration = null,
        HistoryRowCodec? historyCodec = null,
        IViewJointRepairAdmissionGate? admissionGate = null)
    {
        var catalog = new StaticCatalog(registration ?? new ViewRegistration(ViewName, SourceTreeId, new IdentityProjection()), peer);

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(SourceTreeId);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        // The source tree is distinct from the view tree: the maintainer confirms a
        // completed atomic batch against the source's committed state before it
        // flushes it, so the source answers from SourceRows (seeded by ArmLog with
        // the log's last-writer state), never from the view's rows.
        var sourceRows = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var sourceTree = Substitute.For<ILattice>();
        sourceTree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(call.ArgAt<List<string>>(0)
                .Where(sourceRows.ContainsKey)
                .ToDictionary(k => k, k => sourceRows[k], StringComparer.Ordinal)));
        factory.GetGrain<ILattice>(Arg.Any<string>())
            .Returns(call => string.Equals(call.ArgAt<string>(0), SourceTreeId, StringComparison.Ordinal) ? sourceTree : view.Tree);
        // The source's stored last-writer version per key (tombstones included),
        // which the maintainer reads to drop an ordinary write the source already
        // superseded (#4885). Seeded by SeedSource alongside SourceRows.
        var sourceVersions = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var routing = new RoutingInfo(SourceTreeId, ShardMap.CreateDefault(1, 1));
        sourceTree.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<RoutingInfo>(routing));
        sourceTree.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<RoutingInfo>(routing));
        var sourceRoot = Substitute.For<IShardRootGrain>();
        sourceRoot.GetRawVersionsAsync(Arg.Any<List<string>>())
            .Returns(call => Task.FromResult(call.ArgAt<List<string>>(0)
                .Select(k => sourceVersions.GetValueOrDefault(k, HybridLogicalClock.Zero))
                .ToList()));
        // Live rows only (null for a tombstone or an absent key), as the shard root
        // answers: the exact lowering of a range delete spares a key the source
        // still holds live.
        sourceRoot.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(call => Task.FromResult(call.ArgAt<List<string>>(0)
                .Select(k => sourceRows.TryGetValue(k, out var value)
                    ? new LwwEntry { Key = k, Value = value, Timestamp = sourceVersions.GetValueOrDefault(k, HybridLogicalClock.Zero) }
                    : (LwwEntry?)null)
                .ToList()));
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(sourceRoot);
        sourceRoot.GetVisibleRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(call => Task.FromResult(call.ArgAt<List<string>>(0)
                .Select(k => sourceRows.TryGetValue(k, out var value)
                    ? new LwwEntry { Key = k, Value = value, Timestamp = sourceVersions.GetValueOrDefault(k) }
                    : sourceVersions.TryGetValue(k, out var timestamp)
                        ? new LwwEntry { Key = k, IsTombstone = true, Timestamp = timestamp }
                        : (LwwEntry?)null)
                .ToList()));
        if (coordinator is not null)
        {
            factory.GetGrain<IViewCrossTreeCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        }

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));
        if (admissionGate is null)
        {
            admissionGate = Substitute.For<IViewJointRepairAdmissionGate>();
            admissionGate.IsOpenAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
        }

        context.ActivationServices.Returns(new ServiceCollection()
            .AddSingleton(admissionGate).BuildServiceProvider());

        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());

        var latticeOptions = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        latticeOptions.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalRetention = TimeSpan.Zero });

        var replicationContext = Substitute.For<ILatticeReplicationContext>();
        replicationContext.IsReplicationEnabled.Returns(false);

        var subscriber = Substitute.For<IWalSubscriber>();
        state.State.BoundPhysicalTreeId = SourceTreeId;

        // The WAL head (next offset to append) the maintainer samples to decide when
        // the drain has consumed every write a source read observed, on partition 0 -
        // the only partition the stub log populates. Zero unless a test simulates
        // writes appended beyond what the drain has read.
        var headOffset = new StrongBox<long>();
        var commitLogReader = Substitute.For<ICommitLogReader>();
        commitLogReader.GetHeadOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(call.ArgAt<int>(1) == 0 ? headOffset.Value : 0L));

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            reminderRegistry: null!,
            NullLogger<ViewMaintainerGrain>.Instance,
            catalog,
            commitLogReader: commitLogReader,
            subscriber: subscriber,
            cursorRegistry: Substitute.For<IWalCursorRegistry>(),
            optionsResolver: TestOptionsResolver.ForFactory(factory, new LatticeOptions()),
            viewOptions,
            latticeOptions,
            replicationContext,
            saturationSignal: null,
            historyRowCodec: historyCodec!,
            state);

        return new Harness(grain, state, subscriber, [], [], sourceRows, sourceVersions, headOffset, sourceRoot);
    }

    /// <summary>
    /// The source log of the defect: an ordinary write to <c>k</c>, then an atomic
    /// batch that overwrites <c>k</c> with a higher HLC. Source last-writer is the
    /// batch value <c>3</c>.
    /// </summary>
    private static List<LatticeMutation> OrdinaryThenBatch(Guid txId) =>
    [
        Ordinary(2, ticks: 10),
        Prepared(txId, 3, ticks: 20),
        Commit(txId, ticks: 21),
    ];

    [Test]
    public async Task A_pass_replayed_after_a_lost_flush_response_keeps_the_batch_value_over_the_older_ordinary_write()
    {
        var view = new FakeViewTree { LoseNextAtomicResponse = true };
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, OrdinaryThenBatch(Guid.NewGuid()));

        // The view saga completes and writes 3, but its response is lost, so the
        // pass fails before it records its read position or persists.
        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.DrainAsync());
        Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }), "the lost-response saga did complete");

        // The retry re-reads from the unchanged position, re-applies the ordinary
        // 2 blindly and re-attaches the flush to the completed saga.
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.ReadFrom[1], Is.EqualTo(0),
                "the retry must re-read the pass from the same position, replaying the ordinary write");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }),
                "the view must converge on the source last-writer (the batch value), not the replayed older ordinary value");
        });
    }

    [Test]
    public async Task A_reactivation_that_replays_an_already_flushed_batch_keeps_the_batch_value_over_the_older_ordinary_write()
    {
        var view = new FakeViewTree();
        var txId = Guid.NewGuid();
        var log = OrdinaryThenBatch(txId);

        // First activation: the pass applies 2, flushes the batch (3), and then
        // crashes before its checkpoint persists.
        var first = Create(view, new FakePersistentState<ViewCheckpointState>
        {
            ThrowOnWrite = new InvalidOperationException("simulated crash before the checkpoint persists"),
        });
        ArmLog(first, log);
        Assert.ThrowsAsync<InvalidOperationException>(() => first.Grain.DrainAsync());
        Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }), "the first activation flushed the batch");

        // A fresh activation from the last durable state (nothing persisted) replays
        // the whole pass against the same view tree.
        var second = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(second, log);
        await second.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.CompletedOperations, Has.Count.EqualTo(1),
                "the replayed flush re-attached to the completed view saga instead of minting a new one");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }),
                "the view must converge on the source last-writer (the batch value), not the replayed older ordinary value");
        });
    }

    /// <summary>The second key of the multi-key batch, which no ordinary write touches.</summary>
    private const string OtherKey = "j";

    /// <summary>
    /// An ordinary write to <c>k</c>, then a two-key atomic batch writing
    /// <c>k = 3</c> and <c>j = 4</c> with a higher HLC; source last-writer is the
    /// whole batch slice. When <paramref name="withRangeDelete"/> is set an
    /// unrelated range delete forces the pass onto the source-ordered range path.
    /// </summary>
    private static List<LatticeMutation> OrdinaryThenTwoKeyBatch(Guid txId, bool withRangeDelete)
    {
        var log = new List<LatticeMutation>();
        if (withRangeDelete)
        {
            log.Add(RangeDelete("x", "y", ticks: 5));
        }

        log.Add(Ordinary(2, ticks: 10));
        log.Add(Prepared(txId, Key, 3, ticks: 20, size: 2, index: 0));
        log.Add(Prepared(txId, OtherKey, 4, ticks: 20, size: 2, index: 1));
        log.Add(Commit(txId, ticks: 21));
        return log;
    }

    [Test]
    public async Task A_point_path_replay_never_rewrites_the_flushed_batch_slice_non_atomically()
    {
        var view = new FakeViewTree { LoseNextAtomicResponse = true };
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, OrdinaryThenTwoKeyBatch(Guid.NewGuid(), withRangeDelete: false));

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.DrainAsync());
        var pointWritesBeforeReplay = view.PointWrites.Count;
        var atomicBeforeReplay = view.AtomicOperations.Count;

        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.PointWrites.Skip(pointWritesBeforeReplay), Is.Empty,
                "the replay must not re-apply the older ordinary write to the batch's key: a reader would observe k = 2 beside j = 4, a slice that never existed at the source");
            Assert.That(view.AtomicOperations.Skip(atomicBeforeReplay).Where(id => id.StartsWith("mv-reassert-", StringComparison.Ordinal)), Is.Empty,
                "with the older write never re-applied the slice already matches, so no repair is needed");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }));
        });
    }

    [Test]
    public async Task A_point_path_reactivation_replay_never_rewrites_the_flushed_batch_slice_non_atomically()
    {
        var view = new FakeViewTree();
        var log = OrdinaryThenTwoKeyBatch(Guid.NewGuid(), withRangeDelete: false);
        var first = Create(view, new FakePersistentState<ViewCheckpointState>
        {
            ThrowOnWrite = new InvalidOperationException("simulated crash before the checkpoint persists"),
        });
        ArmLog(first, log);
        Assert.ThrowsAsync<InvalidOperationException>(() => first.Grain.DrainAsync());
        var pointWritesBeforeReplay = view.PointWrites.Count;

        var second = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(second, log);
        await second.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.PointWrites.Skip(pointWritesBeforeReplay), Is.Empty,
                "the reactivation replay must not re-apply the older ordinary write over the flushed batch slice");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }));
        });
    }
}
