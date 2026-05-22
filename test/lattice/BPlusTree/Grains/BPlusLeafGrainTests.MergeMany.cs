using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- GetAllRawEntriesAsync (internal, called directly on grain implementation) ---

    [Test]
    public async Task GetAllRawEntries_returns_empty_for_new_leaf()
    {
        var grain = CreateGrain();

        var result = await grain.GetAllRawEntriesAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task GetAllRawEntries_includes_live_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("key-1", Encoding.UTF8.GetBytes("val-1"));

        var result = await grain.GetAllRawEntriesAsync();

        Assert.That(result, Contains.Key("key-1"));
        Assert.That(result["key-1"].IsTombstone, Is.False);
    }

    [Test]
    public async Task GetAllRawEntries_includes_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("key-1", Encoding.UTF8.GetBytes("val-1"));
        await grain.DeleteAsync("key-1");

        var result = await grain.GetAllRawEntriesAsync();

        Assert.That(result, Contains.Key("key-1"));
        Assert.That(result["key-1"].IsTombstone, Is.True);
    }

    // --- MergeManyAsync ---

    [Test]
    public async Task MergeMany_inserts_new_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key-a"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("val-a"), clock),
        };

        var result = await grain.MergeManyAsync(entries);

        Assert.That(result, Is.Null);
        Assert.That(grain.EntriesForTest, Contains.Key("key-a"));
    }

    [Test]
    public async Task MergeMany_lww_resolves_newer_wins()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var newClock = HybridLogicalClock.Tick(oldClock);

        // Set existing with old timestamp.
        var existing = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("old"), oldClock),
        };
        await grain.MergeManyAsync(existing);

        // Merge with newer timestamp.
        var newer = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), newClock),
        };
        await grain.MergeManyAsync(newer);

        Assert.That(grain.EntriesForTest["key"].Timestamp, Is.EqualTo(newClock));
    }

    [Test]
    public async Task MergeMany_lww_rejects_older()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var newClock = HybridLogicalClock.Tick(oldClock);

        // Set existing with new timestamp.
        var existing = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), newClock),
        };
        await grain.MergeManyAsync(existing);

        // Merge with older timestamp - should be rejected.
        var older = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("old"), oldClock),
        };
        await grain.MergeManyAsync(older);

        Assert.That(grain.EntriesForTest["key"].Timestamp, Is.EqualTo(newClock));
    }

    [Test]
    public async Task MergeMany_triggers_split_when_overflowing()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Pre-populate to just under MaxLeafKeys (128).
        var clock = HybridLogicalClock.Zero;
        for (int i = 0; i < 127; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            grain.EntriesForTest[$"key-{i:D4}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v-{i}"), clock);
        }

        // Simulate that a split was triggered during a previous MergeMany and
        // persisted the split intent. This tests the split-aware code path.
        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "key-0064"; // midpoint
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";

        // MergeMany should complete the interrupted split and merge the new entry.
        clock = HybridLogicalClock.Tick(clock);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["new-key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), clock),
        };

        var result = await grain.MergeManyAsync(entries);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.PromotedKey, Is.EqualTo("key-0064"));
        Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
    }

    [Test]
    public async Task MergeMany_ticks_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var versionBefore = state.State.Version.GetClock("leaf/test-leaf");

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create([1], clock),
        };
        await grain.MergeManyAsync(entries);

        var versionAfter = state.State.Version.GetClock("leaf/test-leaf");
        Assert.That(versionAfter, Is.GreaterThan(versionBefore));
    }

    [Test]
    public async Task MergeMany_empty_entries_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var writeCountBefore = state.WriteCount;

        var result = await grain.MergeManyAsync([]);

        Assert.That(result, Is.Null);
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore), "Empty merge should not write state");
        Assert.That(grain.EntriesForTest, Is.Empty);
    }

    [Test]
    public async Task MergeMany_tombstone_only_entries_are_merged()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["dead-key"] = LwwValue<byte[]>.Tombstone(clock),
        };

        var result = await grain.MergeManyAsync(entries);

        Assert.That(result, Is.Null);
        Assert.That(grain.EntriesForTest, Contains.Key("dead-key"));
        Assert.That(grain.EntriesForTest["dead-key"].IsTombstone, Is.True);
    }

    [Test]
    public async Task MergeMany_does_not_tick_version_for_empty()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var versionBefore = state.State.Version.GetClock("leaf/test-leaf");

        await grain.MergeManyAsync([]);

        var versionAfter = state.State.Version.GetClock("leaf/test-leaf");
        Assert.That(versionAfter, Is.EqualTo(versionBefore));
    }

    // --- WAL routing: merge-many envelopes ---

    [Test]
    public async Task MergeMany_appends_one_WAL_envelope_per_accepted_entry()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v-a"), clock),
            ["b"] = LwwValue<byte[]>.Tombstone(HybridLogicalClock.Tick(clock)),
        };

        await grain.MergeManyAsync(entries);

        Assert.That(commitLog.AppendCount, Is.EqualTo(2));
        Assert.That(commitLog.Appended.All(m => m.IsMerge), Is.True);
    }

    [Test]
    public async Task MergeMany_skips_WAL_append_for_cross_shard_migration_rejected_by_asymmetric_guard()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        // Seed a foreground (non-migration) entry that the asymmetric
        // guard must protect from a later cross-shard migration import.
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("foreground"));
        var foregroundAppends = commitLog.AppendCount;

        // Import a higher-HLC migration entry; the asymmetric guard
        // drops it and the WAL must not see an envelope for the
        // rejected key.
        var newer = HybridLogicalClock.Tick(state.State.Clock);
        var imports = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("migrated"), newer),
        };
        await grain.MergeManyAsync(imports, isCrossShardMigration: true);

        Assert.That(commitLog.AppendCount, Is.EqualTo(foregroundAppends),
            "the asymmetric migration guard must short-circuit before the WAL append fires");
        Assert.That(Encoding.UTF8.GetString(grain.EntriesForTest["k"].Value!), Is.EqualTo("foreground"));
    }

    [Test]
    public async Task MergeMany_empty_does_not_append_to_WAL()
    {
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);

        await grain.MergeManyAsync([]);

        Assert.That(commitLog.AppendCount, Is.EqualTo(0));
    }

    [Test]
    public async Task MergeMany_does_not_call_state_write_state_async_in_steady_path()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);
        var writeCountBefore = state.WriteCount;

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), clock),
        };

        await grain.MergeManyAsync(entries);

        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore),
            "MergeManyAsync steady-state path must route durability through the WAL; the "
            + "legacy state.WriteStateAsync() call site is gone now that merge-many is WAL-routed. "
            + "The split-recovery branch's topology-only persist still fires when an interrupted split is recovered.");
    }

    // --- batched WAL append on the merge-many path ---

    [Test]
    public async Task MergeMany_collapses_per_key_wal_appends_into_a_single_batched_call()
    {
        // MergeManyAsync is the cross-shard migration entry point (hot
        // on online-reshard and shard splits). The per-entry WAL grain
        // hop must be collapsed into a single ICommitLogWriter.AppendManyAsync
        // call so the migration channel inherits the same O(1) grain-hop
        // savings as the foreground SetManyAsync fast path.
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);

        var entries = new Dictionary<string, LwwValue<byte[]>>();
        var clock = HybridLogicalClock.Tick(default);
        for (var i = 0; i < 16; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            entries[$"k{i:D2}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v{i}"), clock);
        }

        await grain.MergeManyAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(commitLog.AppendManyCallCount, Is.EqualTo(1),
                "MergeManyAsync must dispatch as a single batched commit-log call.");
            Assert.That(commitLog.AppendCount, Is.EqualTo(16),
                "Every accepted entry must still be captured in the WAL append record.");
            Assert.That(commitLog.Appended.All(m => m.IsMerge), Is.True,
                "Every batched envelope must carry IsMerge=true.");
        });
    }

    [Test]
    public async Task MergeMany_batched_call_excludes_entries_dropped_by_asymmetric_guard()
    {
        // Cross-shard migration imports MUST be filtered by the
        // asymmetric guard BEFORE the batched WAL dispatch fires;
        // otherwise the WAL would record envelopes for entries that
        // never made it into the projection and crash recovery would
        // re-apply ghost writes. The batched-merge path runs the guard
        // in step 0 (filter+build) and only mutations that survive the
        // filter are appended.
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        // Two foreground (non-migration) entries that the asymmetric
        // guard must protect from later migration imports.
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("foreground-a"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("foreground-b"));
        var foregroundAppends = commitLog.AppendCount;
        var foregroundBatchCalls = commitLog.AppendManyCallCount;

        // Mixed batch: two protected keys (guard drops them) and one
        // fresh key (guard accepts). The batched call must include
        // exactly one mutation - the fresh "c" key.
        var newer = HybridLogicalClock.Tick(state.State.Clock);
        var imports = new Dictionary<string, LwwValue<byte[]>>
        {
            ["a"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("migrated-a"), newer),
            ["b"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("migrated-b"), newer),
            ["c"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("migrated-c"), newer),
        };
        await grain.MergeManyAsync(imports, isCrossShardMigration: true);

        Assert.Multiple(() =>
        {
            Assert.That(commitLog.AppendManyCallCount, Is.EqualTo(foregroundBatchCalls + 1),
                "the migration import must dispatch exactly one batched commit-log call");
            Assert.That(commitLog.AppendCount, Is.EqualTo(foregroundAppends + 1),
                "only the un-guarded entry survives the asymmetric filter");
            Assert.That(commitLog.Appended.Any(m => m.Key == "c"), Is.True,
                "the surviving migration envelope is the fresh key");
            Assert.That(commitLog.Appended.Any(m => m.Key == "a" && m.IsMerge), Is.False,
                "the guard-rejected keys must not appear in the WAL append record");
        });
        Assert.That(Encoding.UTF8.GetString(grain.EntriesForTest["a"].Value!), Is.EqualTo("foreground-a"));
        Assert.That(Encoding.UTF8.GetString(grain.EntriesForTest["b"].Value!), Is.EqualTo("foreground-b"));
        Assert.That(Encoding.UTF8.GetString(grain.EntriesForTest["c"].Value!), Is.EqualTo("migrated-c"));
    }

    [Test]
    public async Task MergeMany_empty_does_not_call_AppendManyAsync()
    {
        // Sanity check: the empty-batch shortcut must not dispatch a
        // zero-length batched call (which would bias the merge channel's
        // LeafWriteDuration percentile reads).
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);

        await grain.MergeManyAsync([]);

        Assert.That(commitLog.AppendManyCallCount, Is.EqualTo(0));
        Assert.That(commitLog.AppendCount, Is.EqualTo(0));
    }

    [Test]
    public async Task MergeMany_non_migration_path_applies_every_dominant_entry()
    {
        // The non-migration path (default isCrossShardMigration=false)
        // bypasses the per-batch `accepted` work-list allocation and
        // iterates the input dictionary directly in the apply step.
        // This test pins the projection-identity invariant on that
        // bypass: every dominant incoming entry must land in the
        // projection, every tombstone must surface as a tombstone, and
        // every batched WAL envelope must be emitted under a single
        // commit-log call.
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        var entries = new Dictionary<string, LwwValue<byte[]>>();
        var clock = HybridLogicalClock.Tick(default);
        for (var i = 0; i < 8; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            entries[$"live-{i:D2}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v{i}"), clock);
        }
        clock = HybridLogicalClock.Tick(clock);
        entries["dead"] = LwwValue<byte[]>.Tombstone(clock);

        await grain.MergeManyAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(commitLog.AppendManyCallCount, Is.EqualTo(1),
                "non-migration MergeManyAsync must still dispatch as a single batched call");
            Assert.That(commitLog.AppendCount, Is.EqualTo(9),
                "every incoming entry must be captured in the WAL append record");
            Assert.That(grain.EntriesForTest, Has.Count.EqualTo(9));
            Assert.That(grain.EntriesForTest["dead"].IsTombstone, Is.True);
            for (var i = 0; i < 8; i++)
            {
                var key = $"live-{i:D2}";
                Assert.That(Encoding.UTF8.GetString(grain.EntriesForTest[key].Value!), Is.EqualTo($"v{i}"));
            }
        });
    }
}
