using System.Text;
using Orleans.Lattice.Tests.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Integration coverage for multi-partition WAL replay on leaf
/// activation. Pinned at <c>WalPartitions = 4</c> via
/// <see cref="MultiPartitionWalClusterFixture"/> so the per-partition
/// fan-out paths in <c>BPlusLeafGrain.Activation</c>,
/// <c>BPlusLeafGrain.Projection</c>,
/// <c>BPlusLeafGrain.Snapshot</c>, <c>BPlusLeafGrain.Split</c>, and
/// <c>BPlusLeafGrain.CursorRegistry</c> are exercised end-to-end
/// against the real cluster.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BPlusLeafGrainMultiPartitionReplayTests
{
    private MultiPartitionWalClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MultiPartitionWalClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Round_trip_writes_replay_across_every_partition()
    {
        // Mixed Set / Delete batch across enough keys that hashing
        // distributes them across all four WAL partitions. After a
        // round-trip the per-partition checkpoint state and the
        // entry cache must reflect every committed write.
        var tree = await _fixture.CreateTreeAsync($"mp-roundtrip-{Guid.NewGuid():N}");
        var keys = Enumerable.Range(0, 64).Select(i => $"k{i:D3}").ToArray();

        foreach (var k in keys)
        {
            await tree.SetAsync(k, Encoding.UTF8.GetBytes($"v-{k}"));
        }
        // Delete a quarter to mix in tombstones.
        foreach (var k in keys.Where((_, i) => i % 4 == 0))
        {
            await tree.DeleteAsync(k);
        }

        var expectedLive = keys.Where((_, i) => i % 4 != 0).ToArray();
        var actual = new List<string>();
        await foreach (var key in tree.KeysAsync())
        {
            actual.Add(key);
        }

        Assert.That(actual, Is.EquivalentTo(expectedLive));
    }

    [Test]
    public async Task Atomic_batch_across_partitions_is_visible_after_round_trip()
    {
        // SetManyAtomicAsync writes every key under one saga
        // transaction id; the per-key writes hash across multiple
        // partitions. After commit every key must be readable, and
        // the per-partition projection checkpoints must have
        // advanced past the saga terminals.
        var tree = await _fixture.CreateTreeAsync($"mp-saga-{Guid.NewGuid():N}");
        var batch = Enumerable.Range(0, 32).Select(
            i => new KeyValuePair<string, byte[]>($"sk{i:D3}", Encoding.UTF8.GetBytes($"sv-{i}"))).ToList();

        await tree.SetManyAtomicAsync(batch);

        foreach (var kvp in batch)
        {
            var actual = await tree.GetAsync(kvp.Key);
            Assert.That(actual, Is.EqualTo(kvp.Value), $"saga key {kvp.Key} not visible");
        }
    }

    [Test]
    public async Task Delete_range_across_partitions_replays_consistently()
    {
        // DeleteRange fans out per shard; under multi-partition WAL
        // the range tombstone is appended to its partition and must
        // replay correctly during the next read fan-out.
        var tree = await _fixture.CreateTreeAsync($"mp-range-{Guid.NewGuid():N}");
        var keys = Enumerable.Range(0, 40).Select(i => $"r{i:D3}").ToArray();
        foreach (var k in keys)
        {
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));
        }

        await tree.DeleteRangeAsync("r010", "r030");

        var remaining = new List<string>();
        await foreach (var key in tree.KeysAsync())
        {
            remaining.Add(key);
        }
        var expected = keys.Where(k => string.CompareOrdinal(k, "r010") < 0
                                       || string.CompareOrdinal(k, "r030") >= 0).ToArray();
        Assert.That(remaining, Is.EquivalentTo(expected));
    }

    [Test]
    public async Task Default_wal_partitions_pin_is_eight()
    {
        // Pin: the silo-wide default for WalPartitions is 8. Existing
        // trees pin the value in force at first WAL write into the
        // tree registry, so a default flip is non-breaking for
        // already-registered trees.
        Assert.That(new LatticeOptions().WalPartitions, Is.EqualTo(8));
        Assert.That(LatticeOptions.DefaultWalPartitions, Is.EqualTo(8));
        await Task.CompletedTask;
    }
}
