using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// In-process, cluster-free coverage for <see cref="SnapshotLeafGrain"/> driven
/// through the lazy-persist seed-then-open path: <see cref="SnapshotLeafGrain.SeedAsync"/>
/// materialises the shard baseline verbatim in memory, then
/// <see cref="SnapshotLeafGrain.OpenAsync"/> attaches the read-time ownership
/// filter via its pre-seeded branch (no WAL replay, no durable reload). The three
/// range readers are then asserted deterministically over the seeded rows,
/// including tombstone / expired / predicate / range-bound / reverse / limit
/// filtering. Guard clauses on both entry points are exercised directly.
/// </summary>
[TestFixture]
public sealed class SnapshotLeafGrainSeedReadTests
{
    private const string TreeId = "snap-leaf-tree";
    private const int ShardIndex = 0;
    private static readonly Guid Token = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly long[] Offsets = [5L];

    private static SnapshotLeafGrain CreateGrain() => CreateGrain(out _);

    private static SnapshotLeafGrain CreateGrain(out IGrainFactory grainFactory)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("snapshot-leaf", $"{TreeId}/{ShardIndex}"));
        context.ActivationServices.GetService(typeof(CrdtShapeRegistry)).Returns(new CrdtShapeRegistry());
        context.ActivationServices.GetService(typeof(ILatticeEnvelopeCodec)).Returns(null);

        grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        return new SnapshotLeafGrain(
            context, grainFactory, optionsMonitor, NullLogger<SnapshotLeafGrain>.Instance);
    }

    private static HybridLogicalClock Clock(long wall) => new() { WallClockTicks = wall, Counter = 0 };

    private static LeafSnapshotRow Row(string key, byte[] value) =>
        new(key, new LwwValue<byte[]> { Value = value, Timestamp = Clock(100), IsTombstone = false });

    private static LeafSnapshotRow Tombstone(string key) =>
        new(key, new LwwValue<byte[]> { Value = null, Timestamp = Clock(100), IsTombstone = true });

    private static LeafSnapshotRow Expired(string key, byte[] value) =>
        new(key, new LwwValue<byte[]> { Value = value, Timestamp = Clock(100), ExpiresAtTicks = 1 });

    private static SnapshotShardBaseline Baseline(params LeafSnapshotRow[] rows) => new()
    {
        Rows = rows,
        CapturedHeadPerPartition = Offsets,
        CapturedAtTicks = 1000,
        RowBytes = 0,
    };

    private static async Task<SnapshotLeafGrain> SeededOpenGrain(params LeafSnapshotRow[] rows)
    {
        var grain = CreateGrain();
        await grain.SeedAsync(TreeId, ShardIndex, Baseline(rows), Token, CancellationToken.None);
        await grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        return grain;
    }

    // ------------------------------------------------------------------
    // SeedAsync guard clauses.
    // ------------------------------------------------------------------

    [Test]
    public void SeedAsync_null_tree_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.SeedAsync(null!, ShardIndex, Baseline(Row("a", [1])), Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void SeedAsync_null_baseline_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.SeedAsync(TreeId, ShardIndex, null!, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void SeedAsync_negative_shard_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.SeedAsync(TreeId, -1, Baseline(Row("a", [1])), Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void SeedAsync_empty_token_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.SeedAsync(TreeId, ShardIndex, Baseline(Row("a", [1])), Guid.Empty, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SeedAsync_empty_captured_head_throws()
    {
        var grain = CreateGrain();
        var baseline = new SnapshotShardBaseline { Rows = [Row("a", [1])], CapturedHeadPerPartition = [] };
        Assert.That(async () => await grain.SeedAsync(TreeId, ShardIndex, baseline, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SeedAsync_idempotent_reseed_same_coordinate_is_noop()
    {
        var grain = CreateGrain();
        var baseline = Baseline(Row("a", [1]));
        await grain.SeedAsync(TreeId, ShardIndex, baseline, Token, CancellationToken.None);

        // Re-seeding the same coordinate against an already-materialised activation returns without throwing.
        await grain.SeedAsync(TreeId, ShardIndex, baseline, Token, CancellationToken.None);

        await grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);
        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EquivalentTo(new[] { "a" }));
    }

    // ------------------------------------------------------------------
    // OpenAsync guard clauses.
    // ------------------------------------------------------------------

    [Test]
    public void OpenAsync_null_tree_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.OpenAsync(null!, ShardIndex, Offsets, null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void OpenAsync_null_offsets_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.OpenAsync(TreeId, ShardIndex, null!, null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void OpenAsync_negative_shard_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.OpenAsync(TreeId, -1, Offsets, null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void OpenAsync_empty_offsets_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.OpenAsync(TreeId, ShardIndex, [], null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void OpenAsync_negative_offset_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.OpenAsync(TreeId, ShardIndex, [-1L], null, 0, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void OpenAsync_owned_slots_without_shard_count_throws()
    {
        var grain = CreateGrain();
        Assert.That(async () => await grain.OpenAsync(TreeId, ShardIndex, Offsets, new[] { 0 }, 0, Token, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task OpenAsync_idempotent_reopen_same_coordinate_is_noop()
    {
        var grain = await SeededOpenGrain(Row("a", [1]));

        // A second OpenAsync with the identical coordinate is a no-op (persisted baseline TTL touch is skipped).
        await grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EquivalentTo(new[] { "a" }));
    }

    // ------------------------------------------------------------------
    // Range readers over the seeded baseline.
    // ------------------------------------------------------------------

    [Test]
    public async Task GetKeysAsync_returns_live_keys_in_order()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Row("b", [2]), Row("c", [3]));
        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task GetKeysAsync_skips_tombstones_and_expired()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Tombstone("b"), Expired("c", [3]), Row("d", [4]));
        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "d" }));
    }

    [Test]
    public async Task GetKeysAsync_honours_range_bounds()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Row("b", [2]), Row("c", [3]), Row("d", [4]));
        var keys = await grain.GetKeysAsync(startInclusive: "b", endExclusive: "d");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetKeysAsync_honours_after_and_before_bounds()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Row("b", [2]), Row("c", [3]), Row("d", [4]));
        var keys = await grain.GetKeysAsync(afterExclusive: "a", beforeExclusive: "d");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetKeysAsync_limit_zero_returns_empty()
    {
        var grain = await SeededOpenGrain(Row("a", [1]));
        var keys = await grain.GetKeysAsync(limit: 0);
        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task GetKeysAsync_forward_limit_truncates_head()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Row("b", [2]), Row("c", [3]));
        var keys = await grain.GetKeysAsync(limit: 2);
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetKeysAsync_reverse_limit_keeps_largest()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Row("b", [2]), Row("c", [3]));
        var keys = await grain.GetKeysAsync(limit: 2, reverse: true);
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetEntriesAsync_returns_key_value_pairs()
    {
        var grain = await SeededOpenGrain(Row("a", [1, 2]), Tombstone("b"), Row("c", [3]));
        var entries = await grain.GetEntriesAsync();
        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "c" }));
        Assert.That(entries[0].Value, Is.EqualTo(new byte[] { 1, 2 }));
    }

    [Test]
    public async Task GetEntriesAsync_limit_zero_returns_empty()
    {
        var grain = await SeededOpenGrain(Row("a", [1]));
        var entries = await grain.GetEntriesAsync(limit: 0);
        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task GetRawEntriesAsync_preserves_lww_envelope()
    {
        var grain = await SeededOpenGrain(Row("a", [1]), Tombstone("b"), Row("c", [3]));
        var raw = await grain.GetRawEntriesAsync();
        Assert.That(raw.Select(e => e.Key), Is.EqualTo(new[] { "a", "c" }));
    }

    [Test]
    public async Task GetRawEntriesAsync_limit_zero_returns_empty()
    {
        var grain = await SeededOpenGrain(Row("a", [1]));
        var raw = await grain.GetRawEntriesAsync(limit: 0);
        Assert.That(raw, Is.Empty);
    }

    // ------------------------------------------------------------------
    // Durable-baseline persist / reload.
    // ------------------------------------------------------------------

    [Test]
    public async Task EnsurePersistedAsync_flushes_seed_baseline_to_storage()
    {
        var grain = CreateGrain(out var grainFactory);
        var storage = Substitute.For<ISnapshotBaselineStorageGrain>();
        grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(Arg.Any<string>()).Returns(storage);

        await grain.SeedAsync(TreeId, ShardIndex, Baseline(Row("a", [1])), Token, CancellationToken.None);
        await grain.EnsurePersistedAsync(CancellationToken.None);

        await storage.Received(1).SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());

        // A second flush is a no-op now that the baseline is durable.
        await grain.EnsurePersistedAsync(CancellationToken.None);
        await storage.Received(1).SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task EnsurePersistedAsync_noop_when_never_seeded()
    {
        var grain = CreateGrain(out var grainFactory);
        var storage = Substitute.For<ISnapshotBaselineStorageGrain>();
        grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(Arg.Any<string>()).Returns(storage);

        // A leaf with an empty baseline token has nothing durable to flush.
        await grain.EnsurePersistedAsync(CancellationToken.None);

        await storage.DidNotReceive().SaveAsync(Arg.Any<SnapshotShardBaseline>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task OpenAsync_reload_path_seeds_from_durable_baseline()
    {
        var grain = CreateGrain(out var grainFactory);
        var storage = Substitute.For<ISnapshotBaselineStorageGrain>();
        grainFactory.GetGrain<ISnapshotBaselineStorageGrain>(Arg.Any<string>()).Returns(storage);
        storage.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Baseline(Row("a", [1]), Row("b", [2])));

        // Fresh activation (not pre-seeded): OpenAsync with a non-empty token reloads the durable baseline.
        await grain.OpenAsync(TreeId, ShardIndex, Offsets, null, 0, Token, CancellationToken.None);

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
        await storage.Received(1).LoadAsync(Arg.Any<CancellationToken>());
    }
}
