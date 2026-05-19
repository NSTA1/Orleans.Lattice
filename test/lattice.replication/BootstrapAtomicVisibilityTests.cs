using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Acceptance coverage for the bootstrap atomic-visibility invariant:
/// a saga whose prepare-commit pair straddles the producer's snapshot
/// cut must be observed by the bootstrapped peer either at every key
/// or at none, never at a strict subset of keys.
/// <para>
/// The producer-side <see cref="LatticeSnapshotProvider"/> freezes a
/// tree-wide <see cref="ITxRegistryGrain"/> snapshot up front and
/// runs two passes:
/// <list type="number">
///   <item><description><b>Pass 2 (prepared rows)</b> walks every leaf and emits a
///     <see cref="SnapshotEntry"/> with <see cref="SnapshotEntry.IsPrepared"/>
///     for every <c>(transactionId, key)</c> pair the leaf is holding
///     in a pending-tx bucket whose registry status is
///     <see cref="TxStatus.InFlight"/>.</description></item>
///   <item><description><b>Pass 1 (committed projection)</b> drains
///     <see cref="ILattice.EntriesAsync"/> under the frozen registry
///     scope. Sagas snap0 had as <see cref="TxStatus.Committed"/>
///     surface their prepared value as the live one; sagas snap0 had
///     as <see cref="TxStatus.Aborted"/> are dropped; sagas snap0 had
///     as <see cref="TxStatus.InFlight"/> are hidden (already covered
///     by pass 2).</description></item>
/// </list>
/// The receiver replays prepared rows through
/// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/> into
/// its per-tx pending bucket; the matching terminal record - delivered
/// by the post-snapshot incremental WAL stream - flips visibility
/// atomically via
/// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/>.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class BootstrapAtomicVisibilityTests
{
    private const string ClusterId = "snap-prep-site";

    private TestCluster _cluster = null!;
    private LatticeSnapshotProvider _provider = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _provider = new LatticeSnapshotProvider(
            _cluster.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private static async Task<List<SnapshotEntry>> DrainAsync(SnapshotStream stream)
    {
        var collected = new List<SnapshotEntry>();
        await foreach (var entry in stream.Entries)
        {
            collected.Add(entry);
        }
        return collected;
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    /// <summary>
    /// Drives the source leaves into a state where two keys share a
    /// pending-tx bucket whose registry status remains InFlight
    /// throughout the export, then asserts the snapshot stream emits
    /// both keys as prepared rows with the same transaction id and the
    /// original source HLC.
    /// </summary>
    [Test]
    public async Task ExportAsync_emits_prepared_rows_for_in_flight_saga()
    {
        const string tree = "snap-prep-inflight";
        const string keyA = "alpha";
        const string keyB = "beta";
        var sourceHlc = Hlc(1_000);
        var txid = Guid.NewGuid();

        var apply = _cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            keyA,
            new byte[] { 1 },
            sourceHlc,
            ClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 2,
            atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            keyB,
            new byte[] { 2 },
            sourceHlc,
            ClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 2,
            atomicBatchIndex: 1);

        // Sanity: the prepared mutations must NOT be visible to public
        // reads before any terminal arrives - this is the steady-state
        // atomic-visibility invariant that the bootstrap path must
        // preserve.
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        Assert.That(await lattice.GetAsync(keyA), Is.Null);
        Assert.That(await lattice.GetAsync(keyB), Is.Null);

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);

        var preparedEntries = entries.Where(e => e.IsPrepared).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(preparedEntries, Has.Count.EqualTo(2),
                "Both prepared rows must be emitted for the in-flight saga.");
            Assert.That(preparedEntries.All(e => e.TransactionId == txid), Is.True,
                "Every prepared row must carry the source saga's transaction id verbatim.");
            Assert.That(preparedEntries.All(e => e.Timestamp == sourceHlc), Is.True,
                "Prepared rows must round-trip the producer-stamped source HLC.");
            Assert.That(preparedEntries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { keyA, keyB }));
        });

        // The committed projection must NOT contain either saga key
        // because their registry status is InFlight - they're hidden
        // from the committed scan.
        var committedKeys = entries.Where(e => !e.IsPrepared).Select(e => e.Key).ToHashSet(StringComparer.Ordinal);
        Assert.That(committedKeys.Contains(keyA), Is.False);
        Assert.That(committedKeys.Contains(keyB), Is.False);
    }

    /// <summary>
    /// Asserts that a prepared mutation stamped strictly above
    /// <c>asOfHlc</c> is filtered out of the export. The
    /// post-snapshot incremental WAL stream is responsible for
    /// delivering such records, so leaking them across the cut would
    /// double-deliver to the receiver.
    /// </summary>
    [Test]
    public async Task ExportAsync_filters_prepared_rows_above_as_of_hlc()
    {
        const string tree = "snap-prep-asof";
        const string key = "k";
        var sourceHlc = Hlc(5_000);
        var txid = Guid.NewGuid();

        var apply = _cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            key,
            new byte[] { 7 },
            sourceHlc,
            ClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);

        var stream = await _provider.ExportAsync(tree, Hlc(1_000));
        var entries = await DrainAsync(stream);

        Assert.That(entries.Any(e => e.IsPrepared), Is.False,
            "Prepared rows stamped above asOfHlc must be deferred to the incremental WAL stream.");
    }

    /// <summary>
    /// Asserts a prepared-tombstone (delete inside a saga) is emitted
    /// with both <see cref="SnapshotEntry.IsPrepared"/> and
    /// <see cref="SnapshotEntry.IsTombstone"/> set, so the receiver
    /// routes it through
    /// <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>
    /// rather than re-installing a stale value.
    /// </summary>
    [Test]
    public async Task ExportAsync_emits_prepared_tombstone_for_in_flight_delete()
    {
        const string tree = "snap-prep-tombstone";
        const string key = "doomed";

        // Seed a committed value so the leaf entry exists, then layer
        // a prepared tombstone on top via the apply seam.
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync(key, new byte[] { 0xFF });

        var sourceHlc = Hlc(2_000);
        var txid = Guid.NewGuid();
        var apply = _cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedDeleteAsync(
            key,
            sourceHlc,
            ClusterId,
            sourceVectorClock: null,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);

        var prepared = entries.SingleOrDefault(e => e.IsPrepared && e.Key == key);
        Assert.Multiple(() =>
        {
            Assert.That(prepared.IsPrepared, Is.True,
                "Prepared tombstone must surface in the snapshot stream.");
            Assert.That(prepared.IsTombstone, Is.True,
                "Prepared delete must carry IsTombstone so the receiver routes via ApplyPreparedDeleteAsync.");
            Assert.That(prepared.TransactionId, Is.EqualTo(txid));
            Assert.That(prepared.Timestamp, Is.EqualTo(sourceHlc));
        });
    }

    /// <summary>
    /// Asserts an empty export contains no prepared rows when the
    /// tree has never seen a saga - the prepared pass must be a pure
    /// no-op in the steady-state.
    /// </summary>
    [Test]
    public async Task ExportAsync_emits_no_prepared_rows_when_no_saga_is_in_flight()
    {
        const string tree = "snap-prep-none";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("plain", new byte[] { 1 });

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);

        Assert.That(entries.Any(e => e.IsPrepared), Is.False);
        Assert.That(entries.Where(e => !e.IsPrepared).Select(e => e.Key), Is.EqualTo(new[] { "plain" }));
    }

    /// <summary>
    /// End-to-end bootstrap-boundary check: drives a saga's prepared
    /// rows from a producer's export through the receiver-side apply
    /// seam (<see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/>),
    /// then delivers the terminal record via
    /// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/> on a
    /// fresh receiver tree. The receiver must observe all saga keys
    /// atomically on the terminal arrival - never a partial subset
    /// during the apply itself.
    /// </summary>
    [Test]
    public async Task Prepared_rows_replayed_on_receiver_become_atomically_visible_on_terminal()
    {
        const string sourceTree = "snap-e2e-source";
        const string receiverTree = "snap-e2e-receiver";
        const string keyA = "shard-a";
        const string keyB = "shard-b";
        var sourceHlc = Hlc(7_500);
        var terminalHlc = Hlc(8_000);
        var txid = Guid.NewGuid();

        // 1. Drive the producer tree into a state where a saga's
        //    prepared mutations are held in pending buckets across two
        //    keys (they will hash to different leaves on the
        //    DefaultShardCount layout, which is the worst-case
        //    partial-visibility surface).
        var sourceApply = _cluster.Client.GetGrain<IReplicationApplyGrain>(sourceTree);
        await sourceApply.ApplyPreparedSetAsync(
            keyA, new byte[] { 10 }, sourceHlc, ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await sourceApply.ApplyPreparedSetAsync(
            keyB, new byte[] { 20 }, sourceHlc, ClusterId, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);

        // 2. Export the snapshot - the prepared rows must be present.
        var stream = await _provider.ExportAsync(sourceTree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);
        var prepared = entries.Where(e => e.IsPrepared && e.TransactionId == txid).ToList();
        Assert.That(prepared, Has.Count.EqualTo(2),
            "Producer export must emit both prepared rows for the in-flight saga.");

        // 3. Replay the prepared rows on a *fresh* receiver tree
        //    through the same apply seam the bootstrap coordinator
        //    uses. Visibility must remain hidden because no terminal
        //    has arrived yet.
        var receiverApply = _cluster.Client.GetGrain<IReplicationApplyGrain>(receiverTree);
        var receiverLattice = _cluster.Client.GetGrain<ILattice>(receiverTree);
        foreach (var entry in prepared)
        {
            await receiverApply.ApplyPreparedSetAsync(
                entry.Key, entry.Value, entry.Timestamp, ClusterId,
                sourceVectorClock: null,
                expiresAtTicks: entry.ExpiresAtTicks,
                entry.TransactionId,
                atomicBatchSize: entry.AtomicBatchSize,
                atomicBatchIndex: entry.AtomicBatchIndex);
        }
        Assert.Multiple(async () =>
        {
            Assert.That(await receiverLattice.GetAsync(keyA), Is.Null,
                "After prepared-only replay, keyA must remain hidden on the receiver.");
            Assert.That(await receiverLattice.GetAsync(keyB), Is.Null,
                "After prepared-only replay, keyB must remain hidden on the receiver.");
        });

        // 4. Deliver the terminal record per the cross-cluster atomic-
        //    visibility protocol: one terminal per source shard the
        //    saga touched. Both keys hash to the same DefaultShardCount
        //    layout on both trees, so the receiver routes each
        //    terminal to the correct shard root.
        var shardA = LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount);
        var shardB = LatticeSharding.GetShardIndex(keyB, LatticeConstants.DefaultShardCount);
        await receiverApply.ApplyTxTerminalAsync(
            txid, committed: true, shardIndex: shardA, terminalHlc, ClusterId);
        if (shardB != shardA)
        {
            await receiverApply.ApplyTxTerminalAsync(
                txid, committed: true, shardIndex: shardB, terminalHlc, ClusterId);
        }

        // 5. The receiver must now see BOTH keys atomically - never
        //    just one. This is the bootstrap-boundary acceptance
        //    criterion.
        var valueA = await receiverLattice.GetAsync(keyA);
        var valueB = await receiverLattice.GetAsync(keyB);
        Assert.Multiple(() =>
        {
            Assert.That(valueA, Is.EqualTo(new byte[] { 10 }),
                "After commit terminal, the receiver must surface keyA.");
            Assert.That(valueB, Is.EqualTo(new byte[] { 20 }),
                "After commit terminal, the receiver must surface keyB - the all-or-nothing invariant requires both keys to become visible together.");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
