using System.Text;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Covers the batched CRDT delta write added for issue #1921:
/// <see cref="ILattice.ApplyCrdtDeltaManyAsync"/>, the typed
/// <see cref="CrdtLatticeExtensions.EnableManyAsync"/> /
/// <see cref="CrdtLatticeExtensions.StageEnableManyAsync"/> helpers, and the
/// batched atomic <see cref="LatticeAtomicWriteBuilder.SetMany"/> overload.
///
/// <para>
/// The load-bearing property is <b>equivalence</b>: a batched apply must be
/// per-key indistinguishable from N single-key applies. The batch hoists the WAL
/// append, the split check, and the digest publish out of the per-key loop, so
/// each of those is a place the batch could silently diverge from the single-key
/// path. These tests pin the observable result rather than the mechanism, and use
/// a four-shard fixture so the batch genuinely spans shards and leaves rather
/// than collapsing onto one.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrdtBatchApplyIntegrationTests
{
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
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> CreateTreeAsync()
    {
        var treeId = $"crdt-batch-{Guid.NewGuid():N}";
        return await _fixture.CreateTreeAsync(treeId);
    }

    private static List<string> Keys(int count)
    {
        var keys = new List<string>(count);
        for (var i = 0; i < count; i++)
        {
            // Distinct, well-spread keys so the batch buckets across all four
            // shards instead of degenerating onto one.
            keys.Add($"flag-{i:D4}-{Guid.NewGuid():N}");
        }

        return keys;
    }

    [Test]
    public async Task EnableManyAsync_enables_every_flag_across_shards()
    {
        var tree = await CreateTreeAsync();
        var keys = Keys(40);

        await tree.EnableManyAsync(keys, "r1");

        foreach (var key in keys)
        {
            Assert.That(await tree.OrFlag(key).GetAsync(), Is.Not.Null);
            Assert.That((await tree.OrFlag(key).GetAsync()).IsEnabled, Is.True,
                $"Key '{key}' should be enabled by the batch.");
        }
    }

    /// <summary>
    /// The equivalence property, asserted directly: a batched enable and a loop of
    /// single-key enables must leave two trees in the same observable state.
    /// </summary>
    [Test]
    public async Task Batched_enable_is_equivalent_to_a_loop_of_single_key_enables()
    {
        var batched = await CreateTreeAsync();
        var looped = await CreateTreeAsync();
        var keys = Keys(24);

        await batched.EnableManyAsync(keys, "r1");
        foreach (var key in keys)
        {
            await looped.OrFlag(key).EnableAsync("r1");
        }

        foreach (var key in keys)
        {
            var fromBatch = await batched.OrFlag(key).GetAsync();
            var fromLoop = await looped.OrFlag(key).GetAsync();
            Assert.That(fromBatch.IsEnabled, Is.EqualTo(fromLoop.IsEnabled),
                $"Key '{key}' should reach the same enabled state either way.");
        }
    }

    /// <summary>
    /// Re-applying a batch must converge rather than clobber - the property that
    /// makes a caller retry safe on a non-atomic CRDT batch, and the reason the
    /// XML docs promise it.
    /// </summary>
    [Test]
    public async Task Re_applying_a_batch_converges_rather_than_clobbering()
    {
        var tree = await CreateTreeAsync();
        var keys = Keys(12);

        await tree.EnableManyAsync(keys, "r1");
        await tree.EnableManyAsync(keys, "r1");
        await tree.EnableManyAsync(keys, "r2");

        foreach (var key in keys)
        {
            Assert.That((await tree.OrFlag(key).GetAsync()).IsEnabled, Is.True,
                $"Key '{key}' stays enabled after repeated and multi-replica batches.");
        }
    }

    /// <summary>
    /// A batch large enough to overflow a leaf must still land every key: the
    /// batch checks the split predicate once at the end rather than per key, so
    /// this pins that the deferred check does not lose entries.
    /// </summary>
    [Test]
    public async Task A_batch_that_overflows_a_leaf_still_lands_every_key()
    {
        var tree = await CreateTreeAsync();
        var keys = Keys(300);

        await tree.EnableManyAsync(keys, "r1");

        var missing = new List<string>();
        foreach (var key in keys)
        {
            if (!(await tree.OrFlag(key).GetAsync()).IsEnabled)
            {
                missing.Add(key);
            }
        }

        Assert.That(missing, Is.Empty,
            "Every key in a leaf-overflowing batch should be enabled after the single end-of-batch split.");
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_rejects_a_null_batch()
    {
        var tree = await CreateTreeAsync();

        Assert.That(
            async () => await tree.ApplyCrdtDeltaManyAsync(null!, LatticeMergeMode.OrFlag),
            Throws.InstanceOf<ArgumentNullException>());
    }

    /// <summary>
    /// LWW is not a CRDT fold, so it must be refused here exactly as the
    /// single-key <c>ApplyCrdtDeltaAsync</c> refuses it - otherwise the batch
    /// would be a back door into a write path that cannot converge.
    /// </summary>
    [Test]
    public async Task ApplyCrdtDeltaManyAsync_rejects_the_lww_register_mode()
    {
        var tree = await CreateTreeAsync();
        var deltas = new List<KeyValuePair<string, byte[]>>
        {
            new("k", Encoding.UTF8.GetBytes("{}")),
        };

        Assert.That(
            async () => await tree.ApplyCrdtDeltaManyAsync(deltas, LatticeMergeMode.LwwRegister),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ApplyCrdtDeltaManyAsync_accepts_an_empty_batch_as_a_no_op()
    {
        var tree = await CreateTreeAsync();

        Assert.That(
            async () => await tree.ApplyCrdtDeltaManyAsync(
                new List<KeyValuePair<string, byte[]>>(), LatticeMergeMode.OrFlag),
            Throws.Nothing);
    }

    [Test]
    public async Task EnableManyAsync_accepts_an_empty_key_set_as_a_no_op()
    {
        var tree = await CreateTreeAsync();

        Assert.That(
            async () => await tree.EnableManyAsync(Array.Empty<string>(), "r1"),
            Throws.Nothing);
    }

    [Test]
    public async Task EnableManyAsync_rejects_a_null_key_set_and_a_null_replica()
    {
        var tree = await CreateTreeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await tree.EnableManyAsync(null!, "r1"),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await tree.EnableManyAsync(new[] { "k" }, null!),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }

    /// <summary>
    /// The staging helper must mint one token per key, in order, each carrying
    /// both the merged state and the delta - the contract
    /// <see cref="LatticeAtomicWriteBuilder.SetMany"/> relies on.
    /// </summary>
    [Test]
    public async Task StageEnableManyAsync_mints_one_token_per_key_in_order()
    {
        var tree = await CreateTreeAsync();
        var keys = Keys(6);

        var staged = await tree.StageEnableManyAsync(keys, "r1");

        Assert.Multiple(() =>
        {
            Assert.That(staged, Has.Count.EqualTo(keys.Count));
            for (var i = 0; i < keys.Count; i++)
            {
                Assert.That(staged[i].Key, Is.EqualTo(keys[i]),
                    "Tokens are returned in the caller's key order.");
                Assert.That(staged[i].Value, Is.Not.Null.And.Not.Empty,
                    "Each token carries the merged CRDT state for the saga commit.");
                Assert.That(staged[i].Delta, Is.Not.Null.And.Not.Empty,
                    "Each token carries the delta so a remote cluster folds and converges.");
            }
        });
    }

    [Test]
    public async Task StageEnableManyAsync_returns_an_empty_set_for_no_keys()
    {
        var tree = await CreateTreeAsync();

        var staged = await tree.StageEnableManyAsync(Array.Empty<string>(), "r1");

        Assert.That(staged, Is.Empty);
    }
}
