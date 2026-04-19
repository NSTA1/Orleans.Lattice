using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class AtomicWriteIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static List<KeyValuePair<string, byte[]>> Entries(params (string k, string v)[] pairs) =>
        pairs.Select(p => new KeyValuePair<string, byte[]>(p.k, Bytes(p.v))).ToList();

    [Test]
    public async Task SetManyAtomicAsync_commits_all_entries()
    {
        var treeId = $"atomic-commit-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await tree.SetManyAtomicAsync(Entries(("a", "A"), ("b", "B"), ("c", "C")));

        Assert.That(await tree.GetAsync("a"), Is.EqualTo(Bytes("A")));
        Assert.That(await tree.GetAsync("b"), Is.EqualTo(Bytes("B")));
        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Bytes("C")));
    }

    [Test]
    public async Task SetManyAtomicAsync_empty_batch_is_noop()
    {
        var treeId = $"atomic-empty-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await tree.SetManyAtomicAsync(new List<KeyValuePair<string, byte[]>>());

        Assert.That(await tree.CountAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task SetManyAtomicAsync_overwrites_existing_entries()
    {
        var treeId = $"atomic-over-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("a", Bytes("old-A"));

        await tree.SetManyAtomicAsync(Entries(("a", "new-A"), ("b", "new-B")));

        Assert.That(await tree.GetAsync("a"), Is.EqualTo(Bytes("new-A")));
        Assert.That(await tree.GetAsync("b"), Is.EqualTo(Bytes("new-B")));
    }

    [Test]
    public void SetManyAtomicAsync_throws_on_duplicate_keys()
    {
        var treeId = $"atomic-dup-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        Assert.ThrowsAsync<ArgumentException>(() =>
            tree.SetManyAtomicAsync(Entries(("a", "A"), ("a", "B"))));
    }

    [Test]
    public void SetManyAtomicAsync_throws_on_null_entries()
    {
        var treeId = $"atomic-null-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        Assert.ThrowsAsync<ArgumentNullException>(() => tree.SetManyAtomicAsync(null!));
    }

    [Test]
    public async Task SetManyAtomicAsync_is_independent_across_invocations()
    {
        // Each call uses a fresh saga grain (distinct operation id), so
        // concurrent atomic writes to different keys do not serialize.
        var treeId = $"atomic-concurrent-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var t1 = tree.SetManyAtomicAsync(Entries(("a", "A"), ("b", "B")));
        var t2 = tree.SetManyAtomicAsync(Entries(("c", "C"), ("d", "D")));
        await Task.WhenAll(t1, t2);

        Assert.That(await tree.CountAsync(), Is.EqualTo(4));
    }

    [Test]
    public async Task SetManyAtomicAsync_typed_extension_round_trips()
    {
        var treeId = $"atomic-typed-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        await tree.SetManyAtomicAsync(new List<KeyValuePair<string, int>>
        {
            new("x", 42),
            new("y", 7),
        });

        Assert.That(await tree.GetAsync<int>("x"), Is.EqualTo(42));
        Assert.That(await tree.GetAsync<int>("y"), Is.EqualTo(7));
    }
}
