using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="ILattice.GetLeafProjectionDigestAsync"/>.
/// Verifies the public-surface forwarder validates inputs, dispatches per-shard,
/// and produces stable digests for identical state.
/// </summary>
[TestFixture]
[Category("Integration")]
public class DigestIntegrationTests
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
    public async Task GetLeafProjectionDigestAsync_empty_tree_returns_zero_entry_count()
    {
        var tree = await NewTreeAsync("digest-empty");

        var digest = await tree.GetLeafProjectionDigestAsync(0);

        Assert.That(digest.EntryCount, Is.Zero);
        Assert.That(digest.CheckpointOffset, Is.Zero);
        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_populated_shard_reports_nonzero_entries()
    {
        var tree = await NewTreeAsync("digest-pop");
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var totalEntries = 0L;
        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            var digest = await tree.GetLeafProjectionDigestAsync(s);
            totalEntries += digest.EntryCount;
            Assert.That(digest.Hash.Length, Is.EqualTo(16));
        }

        Assert.That(totalEntries, Is.EqualTo(50));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_invalid_shard_index_throws()
    {
        var tree = await NewTreeAsync("digest-bad-shard");

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await tree.GetLeafProjectionDigestAsync(int.MaxValue));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_negative_shard_index_throws()
    {
        var tree = await NewTreeAsync("digest-neg-shard");

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await tree.GetLeafProjectionDigestAsync(-1));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_is_stable_across_repeated_calls()
    {
        var tree = await NewTreeAsync("digest-stable");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var d1 = await tree.GetLeafProjectionDigestAsync(0);
        var d2 = await tree.GetLeafProjectionDigestAsync(0);

        Assert.That(d1.Hash, Is.EqualTo(d2.Hash));
        Assert.That(d1.EntryCount, Is.EqualTo(d2.EntryCount));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_changes_after_mutation()
    {
        var tree = await NewTreeAsync("digest-mut");
        await tree.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        // Find which shard k0 lives on and snapshot its digest.
        byte[]? before = null;
        var shardIndex = 0;
        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            var d = await tree.GetLeafProjectionDigestAsync(s);
            if (d.EntryCount > 0)
            {
                before = d.Hash;
                shardIndex = s;
                break;
            }
        }
        Assert.That(before, Is.Not.Null);

        await tree.SetAsync("k0", Encoding.UTF8.GetBytes("v0-updated"));

        var after = await tree.GetLeafProjectionDigestAsync(shardIndex);
        Assert.That(after.Hash, Is.Not.EqualTo(before));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_pre_cancelled_token_throws()
    {
        var tree = await NewTreeAsync("digest-cancel");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await tree.GetLeafProjectionDigestAsync(0, cts.Token));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_rejects_system_tree()
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var systemTree = grainFactory.GetGrain<ILattice>("_lattice_replog_test");

        Assert.ThrowsAsync<LatticeReservedTreeNamespaceException>(
            async () => await systemTree.GetLeafProjectionDigestAsync(0));
    }
}
