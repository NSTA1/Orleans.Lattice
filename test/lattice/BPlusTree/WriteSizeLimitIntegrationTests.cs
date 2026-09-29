using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

// Regression coverage for the optional write-size bounds
// (LatticeOptions.MaxKeyLength / MaxValueSizeBytes). A client must not be able
// to drive unbounded heap growth by writing pathologically large keys or
// values; the public ILattice write surface rejects an oversized write with
// ArgumentException before any shard work, while a within-bound write still
// succeeds.
[TestFixture]
[Category("Integration")]
public class WriteSizeLimitIntegrationTests
{
    private WriteSizeLimitClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new WriteSizeLimitClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static string OverlongKey() =>
        new('k', WriteSizeLimitClusterFixture.MaxKeyLength + 1);

    private static byte[] OversizedValue() =>
        new byte[WriteSizeLimitClusterFixture.MaxValueSizeBytes + 1];

    private static byte[] SmallValue() => Encoding.UTF8.GetBytes("v");

    [Test]
    public void SetAsync_rejects_oversized_key()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-set-key");
        Assert.That(
            async () => await tree.SetAsync(OverlongKey(), SmallValue()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetAsync_rejects_oversized_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-set-value");
        Assert.That(
            async () => await tree.SetAsync("k", OversizedValue()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetAsync_accepts_within_bound_write()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-set-ok");
        await tree.SetAsync("k", SmallValue());
        var read = await tree.GetAsync("k");
        Assert.That(read, Is.EqualTo(SmallValue()));
    }

    [Test]
    public void SetAsync_ttl_rejects_oversized_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-ttl-value");
        Assert.That(
            async () => await tree.SetAsync("k", OversizedValue(), TimeSpan.FromMinutes(5)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetIfVersionAsync_rejects_oversized_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-cas-value");
        Assert.That(
            async () => await tree.SetIfVersionAsync("k", OversizedValue(), HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetOrSetAsync_rejects_oversized_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-getorset-value");
        Assert.That(
            async () => await tree.GetOrSetAsync("k", OversizedValue()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetManyAsync_rejects_oversized_entry()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-setmany-value");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("ok", SmallValue()),
            new("bad", OversizedValue()),
        };
        Assert.That(
            async () => await tree.SetManyAsync(entries),
            Throws.InstanceOf<ArgumentException>());
    }

    private sealed record Scored(int Score);

    private static LatticePredicateNode AnyScore() =>
        LatticePredicatePushdown.Compile<Scored>(
            s => s.Score >= 0, JsonLatticeSerializer<Scored>.Default);

    private static byte[] ScoredJson(int score) => Encoding.UTF8.GetBytes($"{{\"Score\":{score}}}");

    // Regression: the conditional batch skipped the write-size bounds that
    // SetManyAsync enforces, so adding a predicate bypassed them. The keys are
    // seeded with a matching value first, so without the bound check the
    // oversized write would be admitted by the predicate and land.
    [Test]
    public async Task SetManyWherePredicateAsync_rejects_oversized_value()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-where-value");
        await tree.SetAsync("ok", ScoredJson(1));
        await tree.SetAsync("bad", ScoredJson(1));
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("ok", ScoredJson(2)),
            new("bad", OversizedValue()),
        };

        Assert.That(
            async () => await tree.SetManyWherePredicateAsync(entries, AnyScore()),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(await tree.GetAsync("bad"), Is.EqualTo(ScoredJson(1)),
            "the rejected batch must not have written the oversized value");
    }

    [Test]
    public void SetManyWherePredicateAsync_rejects_oversized_key()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-where-key");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new(OverlongKey(), ScoredJson(2)),
        };

        Assert.That(
            async () => await tree.SetManyWherePredicateAsync(entries, AnyScore()),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetManyWherePredicateAsync_accepts_within_bound_write()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("wsl-where-ok");
        await tree.SetAsync("k", ScoredJson(1));

        var written = await tree.SetManyWherePredicateAsync(
            new List<KeyValuePair<string, byte[]>> { new("k", ScoredJson(2)) }, AnyScore());

        Assert.That(written, Is.EqualTo(new[] { "k" }));
        Assert.That(await tree.GetAsync("k"), Is.EqualTo(ScoredJson(2)));
    }

    // Regression: the atomic batch writes skipped the entry-time write-size
    // bounds. An oversized leg was only caught once the saga was already
    // running, so the call failed with InvalidOperationException after the
    // saga had been started and rolled back, instead of the ArgumentException
    // every other write raises before any shard work.
    private static IEnumerable<TestCaseData> AtomicWrites()
    {
        yield return new TestCaseData(
            (Func<ILattice, List<KeyValuePair<string, byte[]>>, Task>)((t, e) => t.SetManyAtomicAsync(e)))
            .SetArgDisplayNames("SetManyAtomicAsync(entries)");
        yield return new TestCaseData(
            (Func<ILattice, List<KeyValuePair<string, byte[]>>, Task>)((t, e) => t.SetManyAtomicAsync(e, "op-id")))
            .SetArgDisplayNames("SetManyAtomicAsync(entries, operationId)");
        yield return new TestCaseData(
            (Func<ILattice, List<KeyValuePair<string, byte[]>>, Task>)((t, e) => t.SetManyAtomicAsync(e, Array.Empty<string>(), "op-mixed")))
            .SetArgDisplayNames("SetManyAtomicAsync(upserts, deletes, operationId)");
        yield return new TestCaseData(
            (Func<ILattice, List<KeyValuePair<string, byte[]>>, Task>)((t, e) => t.SetManyAtomicWhereAsync(e, AnyScore())))
            .SetArgDisplayNames("SetManyAtomicWhereAsync(entries, predicate)");
        yield return new TestCaseData(
            (Func<ILattice, List<KeyValuePair<string, byte[]>>, Task>)((t, e) => t.SetManyAtomicWhereAsync(e, AnyScore(), "op-where")))
            .SetArgDisplayNames("SetManyAtomicWhereAsync(entries, predicate, operationId)");
    }

    [TestCaseSource(nameof(AtomicWrites))]
    public async Task Atomic_batch_rejects_oversized_value_before_the_saga_starts(
        Func<ILattice, List<KeyValuePair<string, byte[]>>, Task> write)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"wsl-atomic-value-{Guid.NewGuid():N}");
        await tree.SetAsync("ok", ScoredJson(1));
        await tree.SetAsync("bad", ScoredJson(1));
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("ok", ScoredJson(2)),
            new("bad", OversizedValue()),
        };

        Assert.That(async () => await write(tree, entries), Throws.InstanceOf<ArgumentException>());
        Assert.That(await tree.GetAsync("ok"), Is.EqualTo(ScoredJson(1)),
            "the rejected batch must not have written any leg");
        Assert.That(await tree.GetAsync("bad"), Is.EqualTo(ScoredJson(1)),
            "the rejected batch must not have written the oversized value");
    }

    [TestCaseSource(nameof(AtomicWrites))]
    public void Atomic_batch_rejects_oversized_key_before_the_saga_starts(
        Func<ILattice, List<KeyValuePair<string, byte[]>>, Task> write)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"wsl-atomic-key-{Guid.NewGuid():N}");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new(OverlongKey(), ScoredJson(2)),
        };

        Assert.That(async () => await write(tree, entries), Throws.InstanceOf<ArgumentException>());
    }

    [TestCaseSource(nameof(AtomicWrites))]
    public async Task Atomic_batch_accepts_within_bound_write(
        Func<ILattice, List<KeyValuePair<string, byte[]>>, Task> write)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>($"wsl-atomic-ok-{Guid.NewGuid():N}");
        await tree.SetAsync("k", ScoredJson(1));

        await write(tree, new List<KeyValuePair<string, byte[]>> { new("k", ScoredJson(2)) });

        Assert.That(await tree.GetAsync("k"), Is.EqualTo(ScoredJson(2)));
    }

    // Regression: a cross-tree atomic write skipped the entry-time write-size
    // bounds. Its sub-sagas caught an oversized leg only once they were running,
    // so the commit failed with InvalidOperationException after the prepare had
    // been staged and rolled back, instead of the ArgumentException every other
    // write raises before anything is staged.
    [Test]
    public async Task Cross_tree_batch_rejects_oversized_value_before_any_tree_is_staged()
    {
        var (first, second) = CrossTreePair("wsl-xtree-value");
        await first.Lattice.SetAsync("ok", ScoredJson(1));
        await second.Lattice.SetAsync("bad", ScoredJson(1));

        Assert.That(
            async () => await _cluster.GrainFactory.SetManyAtomicAsync(
                [
                    new LatticeTreeBatch(first.TreeId, [new("ok", ScoredJson(2))]),
                    new LatticeTreeBatch(second.TreeId, [new("bad", OversizedValue())]),
                ],
                $"op-xtree-value-{Guid.NewGuid():N}"),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(await first.Lattice.GetAsync("ok"), Is.EqualTo(ScoredJson(1)),
            "the rejected cross-tree write must not have written any tree's leg");
        Assert.That(await second.Lattice.GetAsync("bad"), Is.EqualTo(ScoredJson(1)),
            "the rejected cross-tree write must not have written the oversized value");
    }

    [Test]
    public void Cross_tree_batch_rejects_oversized_key_before_any_tree_is_staged()
    {
        var (first, second) = CrossTreePair("wsl-xtree-key");

        Assert.That(
            async () => await _cluster.GrainFactory.SetManyAtomicAsync(
                [
                    new LatticeTreeBatch(first.TreeId, [new("ok", ScoredJson(2))]),
                    new LatticeTreeBatch(second.TreeId, [new(OverlongKey(), ScoredJson(2))]),
                ],
                $"op-xtree-key-{Guid.NewGuid():N}"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task Cross_tree_batch_accepts_within_bound_write()
    {
        var (first, second) = CrossTreePair("wsl-xtree-ok");

        var outcome = await _cluster.GrainFactory.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(first.TreeId, [new("a", ScoredJson(2))]),
                new LatticeTreeBatch(second.TreeId, [new("b", ScoredJson(3))]),
            ],
            $"op-xtree-ok-{Guid.NewGuid():N}");

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(await first.Lattice.GetAsync("a"), Is.EqualTo(ScoredJson(2)));
        Assert.That(await second.Lattice.GetAsync("b"), Is.EqualTo(ScoredJson(3)));
    }

    private ((string TreeId, ILattice Lattice) First, (string TreeId, ILattice Lattice) Second) CrossTreePair(string prefix)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var first = $"{prefix}-a-{suffix}";
        var second = $"{prefix}-b-{suffix}";
        return (
            (first, _cluster.GrainFactory.GetGrain<ILattice>(first)),
            (second, _cluster.GrainFactory.GetGrain<ILattice>(second)));
    }
}
