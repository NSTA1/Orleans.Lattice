using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="ILattice.GetLeafProjectionDigestForRangeAsync"/>.
/// Exercises the public-surface routing plus the shard-root range fold and the
/// leaf range fold against real grains: a full-range probe must be byte-identical
/// to the whole-shard digest, complementary sub-ranges must partition the entry
/// count, and the input-validation contract must match
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeRangeDigestIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix)
        => await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}");

    [Test]
    public async Task RangeDigest_full_range_is_byte_identical_to_whole_shard_digest()
    {
        var tree = await NewTreeAsync("range-full");
        for (var i = 0; i < 60; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            var whole = await tree.GetLeafProjectionDigestAsync(s);
            var fullRange = await tree.GetLeafProjectionDigestForRangeAsync(s, null, null);

            Assert.That(fullRange.Hash, Is.EqualTo(whole.Hash),
                $"full-range digest must equal whole-shard digest for shard {s}");
            Assert.That(fullRange.EntryCount, Is.EqualTo(whole.EntryCount));
            Assert.That(fullRange.CheckpointOffset, Is.EqualTo(whole.CheckpointOffset));
            Assert.That(fullRange.Version, Is.EqualTo(whole.Version));
        }
    }

    [Test]
    public async Task RangeDigest_empty_shard_full_range_equals_whole_shard_digest()
    {
        var tree = await NewTreeAsync("range-empty");

        var whole = await tree.GetLeafProjectionDigestAsync(0);
        var fullRange = await tree.GetLeafProjectionDigestForRangeAsync(0, null, null);

        Assert.That(fullRange.EntryCount, Is.Zero);
        Assert.That(fullRange.Hash, Is.EqualTo(whole.Hash));
    }

    [Test]
    public async Task RangeDigest_complementary_subranges_partition_full_entry_count()
    {
        var tree = await NewTreeAsync("range-split");
        for (var i = 0; i < 60; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            var full = await tree.GetLeafProjectionDigestForRangeAsync(s, null, null);
            if (full.EntryCount == 0)
            {
                continue;
            }

            // "k030" splits the populated key space; [null, k030) + [k030, null)
            // must cover exactly the same entries as [null, null).
            var head = await tree.GetLeafProjectionDigestForRangeAsync(s, null, "k030");
            var tail = await tree.GetLeafProjectionDigestForRangeAsync(s, "k030", null);

            Assert.That(head.EntryCount + tail.EntryCount, Is.EqualTo(full.EntryCount),
                $"complementary sub-ranges must partition the entry count for shard {s}");
        }
    }

    [Test]
    public async Task RangeDigest_subrange_with_no_keys_yields_zero_count()
    {
        var tree = await NewTreeAsync("range-gap");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // Keys are all "k0xx"; a range entirely below them selects nothing on
        // every shard.
        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            var snap = await tree.GetLeafProjectionDigestForRangeAsync(s, "a", "b");
            Assert.That(snap.EntryCount, Is.Zero);
        }
    }

    [Test]
    public async Task RangeDigest_is_stable_across_repeated_calls()
    {
        var tree = await NewTreeAsync("range-stable");
        for (var i = 0; i < 30; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var d1 = await tree.GetLeafProjectionDigestForRangeAsync(0, null, "k015");
        var d2 = await tree.GetLeafProjectionDigestForRangeAsync(0, null, "k015");

        Assert.That(d1.Hash, Is.EqualTo(d2.Hash));
        Assert.That(d1.EntryCount, Is.EqualTo(d2.EntryCount));
    }

    [Test]
    public async Task RangeDigest_invalid_shard_index_throws()
    {
        var tree = await NewTreeAsync("range-bad-shard");

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await tree.GetLeafProjectionDigestForRangeAsync(int.MaxValue, null, null));
    }

    [Test]
    public async Task RangeDigest_pre_cancelled_token_throws()
    {
        var tree = await NewTreeAsync("range-cancel");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await tree.GetLeafProjectionDigestForRangeAsync(0, null, null, cts.Token));
    }

    [Test]
    public async Task RangeDigest_rejects_system_tree()
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var systemTree = grainFactory.GetGrain<ILattice>("_lattice_replog_test");

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            async () => await systemTree.GetLeafProjectionDigestForRangeAsync(0, null, null));
    }
}
