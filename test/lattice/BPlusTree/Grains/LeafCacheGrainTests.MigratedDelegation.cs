using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the cache-side migrated-entry delegation: any cached
/// <c>LwwValue</c> with <c>IsMigrated=true</c> represents a key whose
/// authoritative ownership has shifted to another physical shard via a
/// cross-shard migration saga. The destination leaf installs a saga
/// shadow marker (see <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.MarkSagaShadowAsync"/>)
/// for the in-flight window where the migrated entry exists at the
/// source's pre-saga value but the saga's destination terminal backstop
/// has not yet landed - that marker is the only place the saga's
/// linearization point against the per-tree <c>TxRegistry</c> is
/// honored. If the cache short-circuits a migrated entry directly from
/// <c>_cache</c>, it bypasses the leaf's shadow guard and serves a
/// pre-saga value past the saga's commit point - producing the
/// split-snapshot read failure mode observed under chaos
/// (<c>round=N: split pre=X post=Y</c>).
///
/// Mitigation: the cache must treat <c>IsMigrated=true</c> entries
/// exactly like keys in <c>_pendingKeys</c> - delegate the read to the
/// primary leaf so the leaf's shadow-marker / TxRegistry consultation
/// applies uniformly across cache and leaf.
/// </summary>
public partial class LeafCacheGrainTests
{
    private static StateDelta MigratedDeltaWith(string key, byte[] value)
    {
        var clock = HybridLogicalClock.Tick(new HybridLogicalClock());
        var version = new VersionVector();
        version.Tick("primary");
        return new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                [key] = LwwValue<byte[]>.Create(value, clock) with { IsMigrated = true }
            },
            Version = version
        };
    }

    // --- GetAsync: migrated entries must delegate to the primary leaf ---

    [Test]
    public async Task GetAsync_delegates_to_primary_when_cached_entry_is_migrated()
    {
        var (grain, leaf) = CreateGrain();

        // The primary publishes a migrated entry into the cache's delta.
        // The cache merges it into _cache with IsMigrated=true. On the
        // NEXT read for this key, the cache must NOT serve the migrated
        // value from _cache - it must delegate to the primary leaf so
        // the leaf's shadow-marker / TxRegistry guard runs.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("k1", Encoding.UTF8.GetBytes("pre-saga")));
        leaf.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("post-saga"));

        // Prime the cache: the first GetAsync triggers a refresh that
        // loads the migrated entry. The first call's return value is
        // intentionally not asserted here - the test's invariant is
        // that the SECOND call (with no further delta) does not
        // short-circuit on _cache for a migrated entry.
        await grain.GetAsync("k1");

        // No new delta on the second call. If the cache short-circuits
        // on _cache, it returns "pre-saga". The correct behavior is to
        // delegate to leaf.GetAsync, which returns "post-saga".
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("post-saga"),
            "Cache must delegate IsMigrated=true reads to the primary leaf so the leaf's shadow-marker guard runs.");
        // The cache must have actually called the leaf at least once
        // for this key - proving the delegation path was taken rather
        // than the _cache short-circuit.
        await leaf.Received().GetAsync("k1");
    }

    [Test]
    public async Task GetAsync_delegating_migrated_entry_surfaces_leaf_stale_routing_exception()
    {
        var (grain, leaf) = CreateGrain();

        // Prime the cache with a migrated entry.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("k1", Encoding.UTF8.GetBytes("pre-saga")));
        await grain.GetAsync("k1");

        // The leaf's shadow-marker guard fires on the next read: the
        // saga has committed but the destination backstop has not yet
        // landed, so the leaf throws StaleShardRoutingException. The
        // cache MUST propagate that exception (it must not swallow it
        // and serve the cached pre-saga value), so the LatticeGrain
        // retry loop can re-fan against a fresh routing snapshot.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.GetAsync("k1").Returns<byte[]?>(_ => throw new StaleShardRoutingException(-1, -1, -1));

        Assert.That(async () => await grain.GetAsync("k1"),
            Throws.TypeOf<StaleShardRoutingException>(),
            "Cache must propagate the primary leaf's shadow-marker StaleShardRoutingException for migrated entries.");
    }

    // --- GetManyAsync: migrated entries must delegate to the primary leaf ---

    [Test]
    public async Task GetManyAsync_delegates_to_primary_when_cached_entry_is_migrated()
    {
        var (grain, leaf) = CreateGrain();

        // Mixed batch: one migrated entry, one non-migrated entry.
        var migratedHlc = HybridLogicalClock.Tick(new HybridLogicalClock());
        var liveHlc = HybridLogicalClock.Tick(migratedHlc);
        var version = new VersionVector();
        version.Tick("primary");
        version.Tick("primary");

        var mixed = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["migrated-k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("pre-saga"), migratedHlc) with { IsMigrated = true },
                ["live-k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("live-v"), liveHlc)
            },
            Version = version
        };
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(mixed);
        leaf.GetManyAsync(Arg.Any<List<string>>())
            .Returns(_ => new Dictionary<string, byte[]>
            {
                ["migrated-k"] = Encoding.UTF8.GetBytes("post-saga")
            });

        // Prime the cache.
        await grain.GetManyAsync(new List<string> { "migrated-k", "live-k" });

        // Second call: no further delta. The non-migrated key must
        // serve from _cache. The migrated key must NOT - it must
        // delegate to the primary leaf so the shadow guard runs.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var result = await grain.GetManyAsync(new List<string> { "migrated-k", "live-k" });

        Assert.That(result, Contains.Key("migrated-k"));
        Assert.That(Encoding.UTF8.GetString(result["migrated-k"]), Is.EqualTo("post-saga"),
            "Migrated cache entries must delegate to the primary leaf so the shadow-marker guard runs.");
        Assert.That(result, Contains.Key("live-k"));
        Assert.That(Encoding.UTF8.GetString(result["live-k"]), Is.EqualTo("live-v"),
            "Non-migrated cache entries continue to serve from _cache as before.");

        // The cache must have batched the migrated key into a single
        // GetManyAsync delegation call to the leaf.
        await leaf.Received().GetManyAsync(Arg.Is<List<string>>(list => list.Contains("migrated-k")));
    }

    [Test]
    public async Task GetManyAsync_delegated_migrated_subset_omitted_by_leaf_does_not_fall_back_to_cache()
    {
        var (grain, leaf) = CreateGrain();

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("migrated-k", Encoding.UTF8.GetBytes("pre-saga")));
        // The leaf returns an empty dictionary for the migrated key -
        // a legitimate outcome when the destination has applied a
        // tombstone or the saga aborted. The cache must NOT fall back
        // to serving _cache["migrated-k"]=pre-saga: that would
        // recreate the bypass we are fixing.
        leaf.GetManyAsync(Arg.Any<List<string>>())
            .Returns(_ => new Dictionary<string, byte[]>());

        // Prime the cache.
        await grain.GetManyAsync(new List<string> { "migrated-k" });

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var result = await grain.GetManyAsync(new List<string> { "migrated-k" });

        Assert.That(result, Does.Not.ContainKey("migrated-k"),
            "When the primary leaf omits a migrated key from the delegated GetManyAsync response, the cache must NOT fall back to the cached pre-saga value.");
    }

    [Test]
    public async Task ExistsAsync_delegates_to_primary_when_cached_entry_is_migrated()
    {
        var (grain, leaf) = CreateGrain();

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("k1", Encoding.UTF8.GetBytes("pre-saga")));
        // The leaf's shadow guard reports the key as absent after
        // saga commit (the destination tombstoned the migrated row,
        // or it never landed on this leaf).
        leaf.ExistsAsync("k1").Returns(false);

        await grain.ExistsAsync("k1");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());

        var exists = await grain.ExistsAsync("k1");

        Assert.That(exists, Is.False,
            "ExistsAsync must delegate IsMigrated=true reads to the primary leaf rather than reporting true from a cached migrated entry.");
        await leaf.Received().ExistsAsync("k1");
    }

    // --- Refresh interaction: a migrated entry that later receives a
    //     post-saga overwrite (non-migrated) must clear the delegation. ---

    [Test]
    public async Task Cache_stops_delegating_when_migrated_entry_is_superseded_by_non_migrated_value()
    {
        var (grain, leaf) = CreateGrain();

        // Initial state: migrated entry primes the cache.
        var migratedHlc = HybridLogicalClock.Tick(new HybridLogicalClock());
        var followUpHlc = HybridLogicalClock.Tick(migratedHlc);
        var v1 = new VersionVector();
        v1.Tick("primary");
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("pre"), migratedHlc) with { IsMigrated = true }
                },
                Version = v1
            });
        await grain.GetAsync("k1");

        // The next refresh delivers a NEW, non-migrated value for the
        // same key (e.g. the destination shard wrote it locally after
        // the migration saga settled). After the merge, the cache must
        // serve the new value directly without delegating.
        var v2 = new VersionVector();
        v2.Tick("primary");
        v2.Tick("primary");
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("post"), followUpHlc)
                },
                Version = v2
            });
        leaf.ClearReceivedCalls();

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("post"),
            "After a higher-HLC non-migrated value supersedes the migrated entry, the cache must serve it directly.");
        await leaf.DidNotReceive().GetAsync("k1");
    }

    // --- Predicate-edge coverage: the delegation predicate
    //     `_pendingKeys.Contains(key) || (hasCached && cached.IsMigrated)`
    //     has four interesting points (pending-only, migrated-only,
    //     both, neither) plus a tombstone short-circuit. The migrated-
    //     only and superseded paths are covered above; the four below
    //     pin the remaining edges so a future refactor of the
    //     predicate cannot silently regress them. ---

    [Test]
    public async Task GetAsync_pending_and_migrated_both_set_delegates_exactly_once()
    {
        // When a key is BOTH pending (active prepare on the leaf) AND
        // its cached LwwValue has IsMigrated=true, the delegation
        // predicate must short-circuit on the pending branch and issue
        // exactly one leaf.GetAsync round-trip - not two. The fix's
        // diagnostic seam reads `reason=pending` here; the production
        // contract is "delegate once, no double round-trip."
        var (grain, leaf) = CreateGrain();

        leaf.GetPendingKeysAsync().Returns(new List<string> { "k1" });
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("k1", Encoding.UTF8.GetBytes("pre-saga")));
        leaf.GetAsync("k1").Returns(Encoding.UTF8.GetBytes("post-saga"));

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("post-saga"));
        await leaf.Received(1).GetAsync("k1");
    }

    [Test]
    public async Task GetManyAsync_pending_and_migrated_both_set_does_not_double_delegate()
    {
        // Mirror of the GetAsync test for the batched path: a key that
        // is BOTH pending and migrated must appear in the delegated
        // batch exactly once. The partition predicate uses
        // `migrated = !pending && ...` precisely to prevent the
        // double-add; this test pins that contract.
        var (grain, leaf) = CreateGrain();

        leaf.GetPendingKeysAsync().Returns(new List<string> { "k1" });
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("k1", Encoding.UTF8.GetBytes("pre-saga")));
        leaf.GetManyAsync(Arg.Any<List<string>>())
            .Returns(_ => new Dictionary<string, byte[]>
            {
                ["k1"] = Encoding.UTF8.GetBytes("post-saga")
            });

        var result = await grain.GetManyAsync(new List<string> { "k1" });

        Assert.That(result, Contains.Key("k1"));
        Assert.That(Encoding.UTF8.GetString(result["k1"]), Is.EqualTo("post-saga"));
        // Exactly one delegated call, with exactly one entry in the
        // batch (no duplicate from the OR-of-flags partition).
        await leaf.Received(1).GetManyAsync(Arg.Is<List<string>>(list =>
            list.Count == 1 && list[0] == "k1"));
    }

    [Test]
    public async Task GetManyAsync_duplicate_keys_in_input_delegate_once()
    {
        // Duplicate keys in the caller's input list must collapse to a
        // single entry in the delegated batch. The `delegatedSet.Add`
        // guard in the partition loop is the deduplication seam; this
        // test pins it so a future simplification cannot accidentally
        // delegate the same key twice and double the leaf round-trip.
        var (grain, leaf) = CreateGrain();

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MigratedDeltaWith("k1", Encoding.UTF8.GetBytes("pre")));
        leaf.GetManyAsync(Arg.Any<List<string>>())
            .Returns(_ => new Dictionary<string, byte[]>
            {
                ["k1"] = Encoding.UTF8.GetBytes("post")
            });

        // Prime the cache so k1 is recorded as IsMigrated=true.
        await grain.GetManyAsync(new List<string> { "k1" });

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.ClearReceivedCalls();

        var result = await grain.GetManyAsync(new List<string> { "k1", "k1", "k1" });

        Assert.That(result, Contains.Key("k1"));
        Assert.That(Encoding.UTF8.GetString(result["k1"]), Is.EqualTo("post"));
        await leaf.Received(1).GetManyAsync(Arg.Is<List<string>>(list =>
            list.Count == 1 && list[0] == "k1"));
    }

    [Test]
    public async Task GetAsync_tombstoned_migrated_entry_does_not_delegate()
    {
        // A tombstoned entry has hasCached=false (the `!cached.IsTombstone`
        // clause filters it out), so the delegation predicate
        // `pending || (hasCached && IsMigrated)` evaluates to false for
        // a non-pending tombstone even when IsMigrated=true. The cache
        // must fall through to the `null` return branch rather than
        // round-tripping to the leaf. This guards the tombstone-clause
        // ordering in the predicate.
        var (grain, leaf) = CreateGrain();

        var tombstoneHlc = HybridLogicalClock.Tick(new HybridLogicalClock());
        var version = new VersionVector();
        version.Tick("primary");
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    // Tombstone-with-IsMigrated=true: a degenerate but
                    // legal state - the destination shard tombstoned a
                    // migrated row.
                    ["k1"] = LwwValue<byte[]>.Tombstone(tombstoneHlc) with { IsMigrated = true }
                },
                Version = version
            });

        // Prime the cache.
        await grain.GetAsync("k1");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        leaf.ClearReceivedCalls();

        var result = await grain.GetAsync("k1");

        Assert.That(result, Is.Null,
            "A tombstoned migrated entry must surface as null without delegating to the primary leaf.");
        await leaf.DidNotReceive().GetAsync("k1");
    }
}
