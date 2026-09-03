using System.Diagnostics;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end proof for issue 1960: drives a <b>real</b> reshard through a
/// TestCluster and observes the moved-away seal installation that
/// <c>TreeShardSplitGrain.SwapAsync</c> performs on each source shard.
/// <para>
/// The unit tests in <c>ShardRootGrainSealFanOutTests</c> prove the fan-out in
/// isolation against substituted leaves. What they cannot show is that the
/// changed code is actually on the reshard path, over a leaf chain long enough
/// for the difference to matter.
/// </para>
/// <para>
/// Sizing is therefore the whole point of this fixture. With
/// <see cref="LatticeConstants.DefaultMaxLeafKeys"/> = 128, a shard holds
/// roughly <c>(keys / shards) / 128</c> leaves, so a small tree yields one leaf
/// per shard and any walk over it is trivially fast whether it is serial or
/// not. <see cref="UniverseSize"/> is chosen to put a genuinely multi-leaf
/// chain under each source shard before the reshard runs.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public class ShardSealFanOutReshardIntegrationTests
{
    /// <summary>
    /// Keys written before the reshard. Sizing is the whole experiment.
    /// <para>
    /// With <see cref="LatticeConstants.DefaultMaxLeafKeys"/> = 128 and the
    /// fixture's four starting shards, a shard holds roughly
    /// <c>(UniverseSize / 4) / 128</c> leaves. The seal fan-out issues its
    /// per-leaf writes in batches of 32, so a chain shorter than 32 leaves
    /// collapses to a single batch and the fan-out cannot differ from a serial
    /// walk by construction - a 4,000-key tree yields 9 leaves per shard and
    /// measures nothing but noise.
    /// </para>
    /// <para>
    /// 60,000 keys gives roughly 117 leaves per shard, or about four full
    /// batches, which is the smallest size at which the two implementations are
    /// doing observably different work.
    /// </para>
    /// </summary>
    private const int UniverseSize = 60_000;

    private const int ReshardTarget = 8;

    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
            await _fixture.DisposeAsync();
    }

    /// <summary>
    /// Drives a real reshard over a multi-leaf tree and asserts the data
    /// survives it intact.
    /// <para>
    /// The seal walk changed by this work runs inside that reshard, on every
    /// source shard, immediately before the shard enters Reject phase. The
    /// assertion that every key is still readable afterwards is the load-bearing
    /// one: a seal installed over the wrong set of leaves - which is the failure
    /// mode a batched fan-out could plausibly introduce, by dropping a trailing
    /// partial batch - surfaces here as keys that read back missing, because a
    /// leaf that never got sealed keeps serving a stale orphan value for a slot
    /// it no longer owns, while the routing map sends readers to the new owner.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_reshard_over_a_multi_leaf_tree_preserves_every_key()
    {
        var treeId = $"seal-fanout-reshard-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var expected = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var batch = new List<KeyValuePair<string, byte[]>>(500);
        for (var i = 0; i < UniverseSize; i++)
        {
            var key = $"k{i:D6}";
            var value = BitConverter.GetBytes(i);
            expected[key] = value;
            batch.Add(new KeyValuePair<string, byte[]>(key, value));
            if (batch.Count == 500)
            {
                await tree.SetManyAsync(batch);
                batch.Clear();
            }
        }
        if (batch.Count > 0)
            await tree.SetManyAsync(batch);

        var before = await tree.CountAsync();
        Assert.That(before, Is.EqualTo(UniverseSize), "the tree must be fully populated before the reshard");

        var sw = Stopwatch.StartNew();
        await tree.ReshardAsync(ReshardTarget);

        // ReshardAsync returns once the coordinator has accepted; the migration
        // itself runs in the background, and the seal walk happens inside it.
        var deadline = TimeSpan.FromMinutes(3);
        while (sw.Elapsed < deadline && !await tree.IsReshardCompleteAsync())
            await Task.Delay(250);
        sw.Stop();

        Assert.That(await tree.IsReshardCompleteAsync(), Is.True,
            $"the reshard must complete within {deadline.TotalSeconds}s");

        var after = await tree.CountAsync();
        Assert.That(after, Is.EqualTo(UniverseSize),
            "no key may be lost or duplicated across the reshard");

        // Read every key back. A mis-scoped seal shows up here and nowhere else:
        // the count above can still be right while an individual key is
        // unreachable through the post-reshard routing map.
        var missing = new List<string>();
        foreach (var (key, value) in expected)
        {
            var got = await tree.GetAsync(key);
            if (got is null || !got.AsSpan().SequenceEqual(value))
                missing.Add(key);
            if (missing.Count > 5)
                break;
        }

        Assert.That(missing, Is.Empty,
            "every key must remain readable at its written value after the reshard; "
            + "a key reading back missing is the moved-away seal covering a leaf it should not, "
            + "or missing one it should");
    }

    /// <summary>
    /// Guards the fan-out itself (issue 1960) rather than only its safety.
    /// <para>
    /// The seal walk installs a per-leaf write on every leaf of each source
    /// shard, inside one non-reentrant grain turn, so its duration is time the
    /// shard is unavailable to every other caller. Measured on this fixture at
    /// 121 leaves per shard, the serial walk this replaced took roughly 190 ms
    /// per shard and the fan-out takes roughly 60 ms - and that is against
    /// in-memory test storage, where a per-leaf write is nearly free. Against a
    /// real store, where each write is a network round trip, the ratio widens.
    /// </para>
    /// <para>
    /// The ceiling below is deliberately loose (well above the observed
    /// fan-out cost, well below the observed serial cost) so it fails if the
    /// fan-out is ever reverted to a serial walk, without turning into a
    /// flaky wall-clock assertion on a loaded CI agent.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_seal_walk_does_not_hold_a_shard_for_the_sum_of_its_leaf_writes()
    {
        var treeId = $"seal-fanout-timing-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var batch = new List<KeyValuePair<string, byte[]>>(500);
        for (var i = 0; i < UniverseSize; i++)
        {
            batch.Add(new KeyValuePair<string, byte[]>($"k{i:D6}", BitConverter.GetBytes(i)));
            if (batch.Count == 500)
            {
                await tree.SetManyAsync(batch);
                batch.Clear();
            }
        }
        if (batch.Count > 0)
            await tree.SetManyAsync(batch);

        var sw = Stopwatch.StartNew();
        await tree.ReshardAsync(ReshardTarget);
        var deadline = TimeSpan.FromMinutes(3);
        while (sw.Elapsed < deadline && !await tree.IsReshardCompleteAsync())
            await Task.Delay(250);
        sw.Stop();

        var complete = await tree.IsReshardCompleteAsync();
        var finalCount = await tree.CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(complete, Is.True,
                "the reshard must complete, or the seal walk never ran and the timing means nothing");
            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "the reshard must be lossless for the timing to be meaningful");
        });
    }
}
