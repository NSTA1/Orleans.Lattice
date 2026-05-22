using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Shadowed-saga read-window tests for the destination side of an
/// online shard split. When the split coordinator's drain phase
/// migrates an entry whose source-side saga is still in flight
/// (or has committed but whose backstop terminal has not yet reached
/// destination), the destination leaf's <c>Entries[K]</c> inherits
/// the source's pre-saga value with <c>IsMigrated=true</c> but the
/// destination has no pending bucket for the saga - the bucket only
/// appears after the retroactive sweep / shadow-forward catches the
/// prepare, and only then if the prepare landed on source before the
/// sweep walked past its leaf. The pre-fix read path serves the
/// migrated pre-saga value out of <c>Entries</c>, producing the
/// "split (pre=N, post=M)" chaos shape on the reshard fixture: a
/// continuous reader observes some keys at their pre-saga round and
/// others at their post-saga round in the same batch, violating
/// atomic visibility.
/// <para>
/// The post-fix invariant: the destination leaf carries a per-saga
/// shadow marker installed by the split coordinator naming the keys
/// the saga affected on source. When the read path is about to
/// surface an <c>IsMigrated=true</c> value for a shadowed key, it
/// consults the registry:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <see cref="TxStatus.InFlight"/>: the migrated value is the
///     correct pre-saga snapshot (strict isolation), so it is
///     served.
///   </description></item>
///   <item><description>
///     <see cref="TxStatus.Aborted"/>: the saga rolled back, the
///     pre-saga value stands, served.
///   </description></item>
///   <item><description>
///     <see cref="TxStatus.Committed"/> and the backstop terminal has
///     already landed (txid is in <c>_recentlyTerminal</c>): the
///     authoritative post-saga value is now in <c>Entries</c>, served.
///   </description></item>
///   <item><description>
///     <see cref="TxStatus.Committed"/> and the backstop has NOT yet
///     landed: serving the migrated pre-saga value would violate
///     atomic visibility, so the read throws
///     <see cref="StaleShardRoutingException"/> to force the
///     <c>LatticeGrain</c> deadline-bounded retry loop to re-fan
///     under a fresh snapshot once the backstop reaches this leaf.
///   </description></item>
/// </list>
/// <para>
/// The shadow marker is cleared as part of every
/// <see cref="IBPlusLeafGrain.ApplyTxTerminalAsync"/> call, so the
/// guard degenerates to a no-op once the saga's terminal has been
/// applied here.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearShadowedSagaReadAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticeRegistrySnapshotContext.Current = null;
    }

    /// <summary>
    /// Seeds the leaf with a single migrated entry under
    /// <paramref name="key"/> mimicking the post-drain destination
    /// state: <c>IsMigrated=true</c>, a high HLC stamped at the
    /// source's pre-saga terminal-flip moment, and the destination
    /// Clock pinned low to simulate the freshly-created shard root.
    /// </summary>
    private static void SeedMigratedEntry(
        FakePersistentState<LeafNodeState> state,
        BPlusLeafGrain grain,
        string key,
        byte[] value)
    {
        var migratedHlc = new HybridLogicalClock { WallClockTicks = 5_000_000, Counter = 0 };
        grain.EntriesForTest[key] = LwwValue<byte[]>.Create(value, migratedHlc) with { IsMigrated = true };
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 1_000_000, Counter = 0 };
    }

    [Test]
    public async Task GetAsync_shadowed_migrated_entry_with_inflight_saga_serves_migrated_value()
    {
        // Arrange: destination leaf with a migrated pre-saga entry
        // and a shadow marker naming the source-side saga as the
        // owner of "k". Registry says the saga is still InFlight.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        SeedMigratedEntry(state, grain, "k", [99]);

        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "k" });

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            // Act + Assert: InFlight => pre-saga value is the correct
            // strict-isolation answer; the migrated value is served.
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 99 }),
                "InFlight saga must surface the migrated pre-saga value (strict isolation).");
        }
    }

    [Test]
    public async Task GetAsync_shadowed_migrated_entry_with_aborted_saga_serves_migrated_value()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        SeedMigratedEntry(state, grain, "k", [99]);

        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "k" });

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Aborted };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 99 }),
                "Aborted saga leaves the pre-saga value authoritative.");
        }
    }

    [Test]
    public void GetAsync_shadowed_migrated_entry_with_committed_saga_and_no_backstop_throws_stale_routing()
    {
        // Arrange: registry says Committed, but the backstop has NOT
        // yet been applied on this destination - serving the migrated
        // pre-saga value here would split observation against any
        // sibling leaf whose backstop has already landed.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        SeedMigratedEntry(state, grain, "k", [99]);

        var txid = Guid.NewGuid();
        grain.MarkSagaShadowAsync(txid, new[] { "k" }).GetAwaiter().GetResult();

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            // Act + Assert: the shadow guard surfaces a stale-routing
            // signal so the LatticeGrain deadline-bounded retry loop
            // re-fans once the backstop terminal reaches this leaf.
            Assert.That(
                async () => await grain.GetAsync("k"),
                Throws.TypeOf<StaleShardRoutingException>(),
                "Pre-fix would have surfaced the migrated pre-saga [99] under a Committed saga.");
        }
    }

    [Test]
    public async Task GetAsync_shadowed_migrated_entry_with_committed_saga_and_backstop_serves_entries()
    {
        // Once the saga's backstop terminal has been applied here,
        // Entries[k] reflects the post-saga value (or absence) and
        // the shadow marker is cleared as a side effect of
        // ApplyTxTerminalAsync, so the read serves Entries directly.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        SeedMigratedEntry(state, grain, "k", [99]);

        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "k" });

        // Saga's terminal lands here, carrying the authoritative
        // post-saga value via the cross-migration LWW backstop.
        await grain.ApplyTxTerminalAsync(
            txid,
            committed: true,
            committedValues: new Dictionary<string, byte[]> { ["k"] = [42] });

        // The registry-snapshot scope is irrelevant after the
        // backstop application because the shadow marker is cleared;
        // assert via an explicit snapshot to keep the test focused on
        // the read path.
        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 42 }),
                "Backstop application must clear the shadow marker and the read must serve the post-saga value.");
        }
    }

    [Test]
    public void GetManyAsync_shadowed_migrated_entry_with_committed_saga_and_no_backstop_throws_stale_routing()
    {
        // Batch variant: the same guard must fire from the scan-path
        // read so a single shadowed-and-Committed-no-backstop key in
        // the batch invalidates the call's routing and triggers the
        // lattice retry, rather than returning a partial dictionary
        // that silently omits or stalely serves the key.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        SeedMigratedEntry(state, grain, "k1", [99]);
        SeedMigratedEntry(state, grain, "k2", [100]);

        var txid = Guid.NewGuid();
        grain.MarkSagaShadowAsync(txid, new[] { "k1" }).GetAwaiter().GetResult();

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            Assert.That(
                async () => await grain.GetManyAsync(new List<string> { "k1", "k2" }),
                Throws.TypeOf<StaleShardRoutingException>(),
                "Pre-fix would have served k1's migrated pre-saga value alongside k2.");
        }
    }

    [Test]
    public async Task GetAsync_non_migrated_entry_under_shadow_marker_serves_entries_unchanged()
    {
        // Defensive: the shadow-marker guard targets ONLY migrated
        // entries. A foreground-written entry (IsMigrated=false) on
        // the destination is authoritative regardless of any
        // shadowing saga on a different leaf, so it must serve
        // unchanged. This pins the marker's narrow scope so a future
        // refactor cannot widen it accidentally.
        var grain = CreateGrain();
        await grain.SetAsync("k", new byte[] { 7 });

        var txid = Guid.NewGuid();
        await grain.MarkSagaShadowAsync(txid, new[] { "k" });

        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 7 }),
                "Foreground (IsMigrated=false) entries must not be shadowed by the migration-only marker.");
        }
    }

    [Test]
    public async Task MarkSagaShadowAsync_with_empty_keys_is_a_no_op()
    {
        // Null/empty defensive contract: the split coordinator may
        // ask to install a marker for zero keys (e.g. a saga that
        // touched only non-moved slots) and must not throw or
        // install a marker that would shadow unrelated reads.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        SeedMigratedEntry(state, grain, "k", [99]);
        var txid = Guid.NewGuid();

        await grain.MarkSagaShadowAsync(txid, Array.Empty<string>());

        // Reading any key must succeed even though we registered the
        // saga - because no key was claimed, the migration-only
        // guard cannot fire.
        var snapshot = new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed };
        using (LatticeRegistrySnapshotContext.BeginScope(snapshot))
        {
            var result = await grain.GetAsync("k");
            Assert.That(result, Is.EqualTo(new byte[] { 99 }),
                "An empty-keys shadow marker must not shadow any read.");
        }
    }

    [Test]
    public void MarkSagaShadowAsync_with_null_keys_throws_argument_null()
    {
        var grain = CreateGrain();
        Assert.That(
            async () => await grain.MarkSagaShadowAsync(Guid.NewGuid(), null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void MarkSagaShadowAsync_with_empty_txid_throws_argument_exception()
    {
        var grain = CreateGrain();
        Assert.That(
            async () => await grain.MarkSagaShadowAsync(Guid.Empty, new[] { "k" }),
            Throws.TypeOf<ArgumentException>());
    }
}
