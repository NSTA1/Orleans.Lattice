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
}
