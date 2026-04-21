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

    // --- Caller-supplied idempotency key (G-011) ---

    [Test]
    public async Task SetManyAtomicAsync_with_operationId_is_idempotent()
    {
        var treeId = $"idem-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("v1")),
            new("b", Encoding.UTF8.GetBytes("v2")),
        };
        var operationId = $"op-{Guid.NewGuid():N}";

        // First call commits.
        await tree.SetManyAtomicAsync(entries, operationId);

        // Second call with same operationId and same key set must be a no-op
        // (observing the completed saga's outcome) and must not throw.
        await tree.SetManyAtomicAsync(entries, operationId);

        Assert.That(await tree.GetAsync("a"), Is.EqualTo(Encoding.UTF8.GetBytes("v1")));
        Assert.That(await tree.GetAsync("b"), Is.EqualTo(Encoding.UTF8.GetBytes("v2")));
    }

    [Test]
    public async Task SetManyAtomicAsync_with_operationId_rejects_mismatched_key_set()
    {
        var treeId = $"mismatch-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var firstBatch = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", Encoding.UTF8.GetBytes("v1")),
            new("k2", Encoding.UTF8.GetBytes("v2")),
        };
        var operationId = $"op-{Guid.NewGuid():N}";
        await tree.SetManyAtomicAsync(firstBatch, operationId);

        var wrongBatch = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", Encoding.UTF8.GetBytes("v1")),
            new("k3", Encoding.UTF8.GetBytes("v3")),
        };

        Assert.That(
            async () => await tree.SetManyAtomicAsync(wrongBatch, operationId),
            Throws.InvalidOperationException.With.Message.Contains("different key set"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void SetManyAtomicAsync_rejects_null_empty_or_whitespace_operationId(string? operationId)
    {
        var treeId = $"invalid-opid-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k", Encoding.UTF8.GetBytes("v")),
        };

        Assert.That(
            async () => await tree.SetManyAtomicAsync(entries, operationId!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetManyAtomicAsync_rejects_operationId_containing_slash()
    {
        var treeId = $"slash-opid-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k", Encoding.UTF8.GetBytes("v")),
        };

        Assert.That(
            async () => await tree.SetManyAtomicAsync(entries, "op/with/slash"),
            Throws.ArgumentException.With.Message.Contains("'/'"));
    }

    [Test]
    public async Task SetManyAtomicAsync_with_operationId_allows_reordered_keys_and_different_values()
    {
        var treeId = $"reorder-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var operationId = $"op-{Guid.NewGuid():N}";

        var first = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
        };
        await tree.SetManyAtomicAsync(first, operationId);

        // Retry with the same keys (reordered) and different values. The
        // fingerprint hashes the sorted key set only, so this is an
        // idempotent retry that resolves to the already-completed saga.
        var retry = new List<KeyValuePair<string, byte[]>>
        {
            new("c", Encoding.UTF8.GetBytes("DIFFERENT")),
            new("a", Encoding.UTF8.GetBytes("DIFFERENT")),
            new("b", Encoding.UTF8.GetBytes("DIFFERENT")),
        };
        await tree.SetManyAtomicAsync(retry, operationId);

        // Original values still hold because the second call re-attached to
        // the completed saga rather than applying the retry's payload.
        Assert.That(await tree.GetAsync("a"), Is.EqualTo(Encoding.UTF8.GetBytes("1")));
        Assert.That(await tree.GetAsync("b"), Is.EqualTo(Encoding.UTF8.GetBytes("2")));
        Assert.That(await tree.GetAsync("c"), Is.EqualTo(Encoding.UTF8.GetBytes("3")));
    }

    [Test]
    public async Task SetManyAtomicAsync_T_with_operationId_is_idempotent()
    {
        var treeId = $"typed-idem-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var entries = new List<KeyValuePair<string, int>>
        {
            new("x", 42),
            new("y", 7),
        };
        var operationId = $"op-{Guid.NewGuid():N}";

        await tree.SetManyAtomicAsync(entries, operationId);
        await tree.SetManyAtomicAsync(entries, operationId);

        Assert.That(await tree.GetAsync<int>("x"), Is.EqualTo(42));
        Assert.That(await tree.GetAsync<int>("y"), Is.EqualTo(7));
    }
}
