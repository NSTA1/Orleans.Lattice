using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Leaf-seam coverage for per-entry (whole-key) TTL on the CRDT write path.
/// Drives the expiry-carrying <see cref="BPlusLeafGrain.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], long)"/>
/// overload directly with absolute past / future ticks (no wall-clock advance,
/// no <see cref="System.Threading.Tasks.Task.Delay(int)"/>, no GC or ordering
/// dependence) so every assertion is deterministic. Reuses the CRDT-replay
/// harness (<c>CreateReplayLeaf</c>, <c>ReplayDeltaBytes</c>, <c>BuildReplayRegistry</c>,
/// <c>TreeForMode</c>, <c>ReplayThroughStrippingEncoder</c>) and the TTL helpers
/// (<c>FutureTicks</c> / <c>PastTicks</c> / <c>CompactTombstonesAsync</c>) defined
/// on the sibling partials of this fixture.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly LatticeMergeMode[] TtlSeamModes =
    {
        LatticeMergeMode.OrSet,
        LatticeMergeMode.PnCounter,
        LatticeMergeMode.VersionVector,
        LatticeMergeMode.MvRegister,
        LatticeMergeMode.OrFlag,
        LatticeMergeMode.RwFlag,
        LatticeMergeMode.Sequence,
        LatticeMergeMode.OrMap,
    };

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_with_future_expiry_persists_ExpiresAtTicks_and_stays_visible(LatticeMergeMode mode)
    {
        var registry = BuildReplayRegistry();
        var grain = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-fut-" + mode);
        var expiry = FutureTicks(TimeSpan.FromHours(1));

        await grain.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), expiry);

        Assert.That(grain.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(expiry));
        Assert.That(await grain.GetAsync("k"), Is.Not.Null, "a future-dated CRDT entry must still read back");
    }

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_with_past_expiry_is_read_hidden(LatticeMergeMode mode)
    {
        var registry = BuildReplayRegistry();
        var grain = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-past-" + mode);

        await grain.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), PastTicks(TimeSpan.FromMinutes(1)));

        Assert.That(await grain.GetAsync("k"), Is.Null, "an expired CRDT entry must read back as null");
    }

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_with_zero_expiry_is_durable(LatticeMergeMode mode)
    {
        var registry = BuildReplayRegistry();
        var grain = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-durable-" + mode);

        await grain.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), 0L);

        Assert.That(grain.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(0L), "a no-TTL CRDT write must be durable");
        Assert.That(await grain.GetAsync("k"), Is.Not.Null);
    }

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_expiry_join_is_order_independent(LatticeMergeMode mode)
    {
        var near = FutureTicks(TimeSpan.FromHours(1));
        var far = FutureTicks(TimeSpan.FromHours(2));
        var registry = BuildReplayRegistry();

        // Order A: far then near.
        var a = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-ord-a-" + mode);
        await a.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), far);
        await a.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 2), near);

        // Order B: near then far.
        var b = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-ord-b-" + mode);
        await b.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), near);
        await b.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 2), far);

        Assert.That(a.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(far), "max-ticks join must survive in order A");
        Assert.That(b.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(far), "max-ticks join must survive in order B");
        Assert.That(a.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(b.EntriesForTest["k"].ExpiresAtTicks),
            "the surviving expiry must be independent of apply order");
    }

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_no_ttl_write_leaves_existing_expiry_unchanged(LatticeMergeMode mode)
    {
        var far = FutureTicks(TimeSpan.FromHours(2));
        var registry = BuildReplayRegistry();
        var grain = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-refresh-" + mode);

        await grain.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), far);
        await grain.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 2), 0L);

        Assert.That(grain.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(far),
            "a durable (no-TTL) CRDT write must not clear an existing expiry (v1 refresh-only semantics)");
    }

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_expired_entry_is_reaped_by_compaction(LatticeMergeMode mode)
    {
        var registry = BuildReplayRegistry();
        var grain = CreateReplayLeaf(registry, TreeForMode(mode), out _, commitLog: null, replicaId: "leaf-ttl-reap-" + mode);

        await grain.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), PastTicks(TimeSpan.FromHours(1)));

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromMinutes(1));

        Assert.That(removed, Is.GreaterThanOrEqualTo(1), "an expired CRDT entry must be reaped by tombstone compaction");
        Assert.That(grain.EntriesForTest.ContainsKey("k"), Is.False);
    }

    [Test]
    [TestCaseSource(nameof(TtlSeamModes))]
    public async Task CrdtApply_expiry_survives_wal_replay_at_running_max(LatticeMergeMode mode)
    {
        var near = FutureTicks(TimeSpan.FromHours(1));
        var far = FutureTicks(TimeSpan.FromHours(2));
        var registry = BuildReplayRegistry();
        var treeId = TreeForMode(mode);
        var commitLog = new FakeCommitLogWriter();
        var foreground = CreateReplayLeaf(registry, treeId, out _, commitLog, replicaId: "leaf-ttl-rp-fg-" + mode);

        // Two TTL'd folds into the same key: the running max (far) must be the
        // surviving expiry, and every WAL record carries the cumulative max.
        await foreground.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 1), far);
        await foreground.ApplyCrdtDeltaAsync("k", mode, ReplayDeltaBytes(mode, 2), near);
        var fgExpiry = foreground.EntriesForTest["k"].ExpiresAtTicks;
        Assert.That(fgExpiry, Is.EqualTo(far), "foreground must fold to the running-max expiry");

        var replay = CreateReplayLeaf(registry, treeId, out _, commitLog: null, replicaId: "leaf-ttl-rp-" + mode);
        ReplayThroughStrippingEncoder(replay, commitLog.Appended);

        Assert.That(replay.EntriesForTest["k"].ExpiresAtTicks, Is.EqualTo(fgExpiry),
            "cold-rebuild replay must reconstruct the foreground cumulative-max expiry");
    }
}
