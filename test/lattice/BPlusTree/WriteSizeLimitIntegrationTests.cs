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
}
