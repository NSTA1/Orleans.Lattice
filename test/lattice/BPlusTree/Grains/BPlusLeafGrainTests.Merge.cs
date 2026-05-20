using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- MergeEntriesAsync ---

    [Test]
    public async Task MergeEntries_preserves_original_timestamps()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(clock));
    }

    [Test]
    public async Task MergeEntries_preserves_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Tombstone(clock)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries["k1"].IsTombstone, Is.True);
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(clock));
    }

    [Test]
    public async Task MergeEntries_is_idempotent()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock)
        };

        await grain.MergeEntriesAsync(entries);
        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries, Has.Count.EqualTo(1));
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(clock));
    }

    [Test]
    public async Task MergeEntries_keeps_newer_local_value()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Write a value locally first (gets a fresh timestamp).
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("local"));
        var localTimestamp = state.State.Entries["k1"].Timestamp;

        // Merge an older entry - should be ignored by LWW.
        var olderClock = default(HybridLogicalClock);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("stale"), olderClock)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(Encoding.UTF8.GetString(state.State.Entries["k1"].Value!), Is.EqualTo("local"));
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(localTimestamp));
    }

    [Test]
    public async Task MergeEntries_with_mixed_live_and_tombstone_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock1 = HybridLogicalClock.Tick(default);
        var clock2 = HybridLogicalClock.Tick(clock1);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["live"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("value"), clock1),
            ["dead"] = LwwValue<byte[]>.Tombstone(clock2)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries.Count, Is.EqualTo(2));
        Assert.That(state.State.Entries["live"].IsTombstone, Is.False);
        Assert.That(Encoding.UTF8.GetString(state.State.Entries["live"].Value!), Is.EqualTo("value"));
        Assert.That(state.State.Entries["dead"].IsTombstone, Is.True);
    }

    [Test]
    public async Task MergeEntries_with_empty_dictionary_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("existing", Encoding.UTF8.GetBytes("v"));
        var countBefore = state.State.Entries.Count;

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.That(state.State.Entries.Count, Is.EqualTo(countBefore));
    }

    // --- WAL routing: merge envelopes ---

    [Test]
    public async Task MergeEntries_appends_one_WAL_envelope_per_entry_with_IsMerge_flag()
    {
        var commitLog = new FakeCommitLogWriter();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, commitLog: commitLog);

        var clock1 = HybridLogicalClock.Tick(default);
        var clock2 = HybridLogicalClock.Tick(clock1);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["live"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), clock1),
            ["dead"] = LwwValue<byte[]>.Tombstone(clock2),
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(commitLog.AppendCount, Is.EqualTo(2));
        Assert.That(commitLog.Appended.All(m => m.IsMerge), Is.True,
            "every merge envelope must carry IsMerge=true so receivers can tag the kind dimension");
        Assert.That(commitLog.Appended.Any(m =>
            m.Key == "live" && m.Kind == MutationKind.Set && !m.IsTombstone), Is.True);
        Assert.That(commitLog.Appended.Any(m =>
            m.Key == "dead" && m.Kind == MutationKind.Delete && m.IsTombstone), Is.True);
    }

    [Test]
    public async Task MergeEntries_empty_dictionary_does_not_append_to_WAL()
    {
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.That(commitLog.AppendCount, Is.EqualTo(0));
    }

    [Test]
    public async Task MergeEntries_does_not_call_state_write_state_async()
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

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore),
            "MergeEntriesAsync must route durability through the WAL; the legacy "
            + "state.WriteStateAsync() call site is gone now that merge is WAL-routed.");
    }

    // --- batched WAL append on the merge path ---

    [Test]
    public async Task MergeEntries_collapses_per_key_wal_appends_into_a_single_batched_call()
    {
        // MergeEntriesAsync is the merge channel called by bulk-load
        // (ShardRootGrain.BulkLoadAsync / BulkLoadRawAsync /
        // BulkAppendAsync), sibling redistribute, and replication-apply.
        // The per-entry WAL grain hop must be collapsed into a single
        // ICommitLogWriter.AppendManyAsync call so a 250-entry leaf
        // pays one WAL grain hop instead of 250.
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);

        var entries = new Dictionary<string, LwwValue<byte[]>>();
        var clock = HybridLogicalClock.Tick(default);
        for (var i = 0; i < 16; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            entries[$"k{i:D2}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v{i}"), clock);
        }

        await grain.MergeEntriesAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(commitLog.AppendManyCallCount, Is.EqualTo(1),
                "MergeEntriesAsync must dispatch as a single batched commit-log call.");
            Assert.That(commitLog.AppendCount, Is.EqualTo(16),
                "Every merged entry must still be captured in the WAL append record.");
            Assert.That(commitLog.Appended.All(m => m.IsMerge), Is.True,
                "Every batched envelope must carry IsMerge=true.");
        });
    }

    [Test]
    public async Task MergeEntries_empty_dictionary_does_not_call_AppendManyAsync()
    {
        // Sanity-check the empty-batch shortcut: a zero-entry merge must
        // not dispatch a batched WAL call at all (it would otherwise show
        // up as a zero-length write on the LeafWriteDuration histogram,
        // biasing the percentile reads of the merge channel).
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.That(commitLog.AppendManyCallCount, Is.EqualTo(0));
        Assert.That(commitLog.AppendCount, Is.EqualTo(0));
    }
}
