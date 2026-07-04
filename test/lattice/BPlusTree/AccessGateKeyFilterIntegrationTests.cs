using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the read-path access-gate key-filter seam
/// (issue #977). A test <see cref="ILatticeAccessGate"/> returns a
/// <see cref="LatticeAccessDecision.Filtered"/> decision scoped to the tree
/// under test; the range-read surfaces (<c>KeysAsync</c> / <c>EntriesAsync</c>),
/// the multi-key point read (<c>GetManyAsync</c>), and <c>CountAsync</c> must
/// admit only the authorized keys, prune unauthorized keys/values server-side,
/// keep the null (allow-all) path unchanged, and bypass filtering entirely under
/// a system-origin scope.
/// </summary>
[TestFixture]
[Category("Integration")]
public class AccessGateKeyFilterIntegrationTests
{
    private AccessGateKeyFilterClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AccessGateKeyFilterClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [TearDown]
    public void TearDown() => ConfigurableAccessGate.Reset();

    // Each key's value is its own UTF-8 bytes so an unauthorized value leak is
    // directly observable by decoding the returned bytes.
    private static byte[] Val(string key) => Encoding.UTF8.GetBytes(key);

    private static async Task SeedAsync(ILattice tree, params string[] keys)
    {
        foreach (var k in keys)
            await tree.SetAsync(k, Val(k));
    }

    private static async Task<List<string>> CollectAsync(IAsyncEnumerable<string> source)
    {
        var list = new List<string>();
        await foreach (var k in source)
            list.Add(k);
        return list;
    }

    private static async Task<List<KeyValuePair<string, byte[]>>> CollectAsync(
        IAsyncEnumerable<KeyValuePair<string, byte[]>> source)
    {
        var list = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in source)
            list.Add(e);
        return list;
    }

    // Scopes a "keep only keys starting with 'user/a'" filter to the given tree;
    // every other tree (and any internal system tree) keeps the allow-all default
    // so unrelated activity is never disturbed.
    private static void FilterUserAToTree(string treeId) =>
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId
                ? LatticeAccessDecision.Filtered(static k => k.StartsWith("user/a", StringComparison.Ordinal))
                : LatticeAccessDecision.Allow();

    [Test]
    public async Task KeysAsync_with_key_filter_returns_only_authorized_keys()
    {
        const string treeId = "agf-keys";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        var keys = await CollectAsync(tree.KeysAsync());

        Assert.That(keys, Is.EquivalentTo(new[] { "user/alice", "user/amy" }));
    }

    [Test]
    public async Task EntriesAsync_with_key_filter_returns_only_authorized_entries_and_never_leaks_value()
    {
        const string treeId = "agf-entries";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        var entries = await CollectAsync(tree.EntriesAsync());

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.Key), Is.EquivalentTo(new[] { "user/alice", "user/amy" }));
            // No unauthorized key's value crossed the boundary.
            Assert.That(entries.Any(e => e.Key == "user/bob" || e.Key == "user/carol"), Is.False);
            // Authorized values are intact (identity-encoded).
            foreach (var e in entries)
                Assert.That(Encoding.UTF8.GetString(e.Value), Is.EqualTo(e.Key));
        });
    }

    [Test]
    public async Task GetManyAsync_omits_unauthorized_keys()
    {
        const string treeId = "agf-getmany";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        var result = await tree.GetManyAsync(
            new List<string> { "user/alice", "user/amy", "user/bob", "user/carol" });

        Assert.Multiple(() =>
        {
            Assert.That(result.Keys, Is.EquivalentTo(new[] { "user/alice", "user/amy" }));
            Assert.That(result.ContainsKey("user/bob"), Is.False);
            Assert.That(result.ContainsKey("user/carol"), Is.False);
        });
    }

    [Test]
    public async Task CountAsync_reflects_only_authorized_keys()
    {
        const string treeId = "agf-count";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        var count = await tree.CountAsync();

        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task CountAsync_range_reflects_only_authorized_keys()
    {
        const string treeId = "agf-count-range";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        // Range covers all four keys; the filter still restricts the count to the
        // authorized subset.
        var count = await tree.CountAsync("user/", "user/z");

        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task KeysAsync_with_plain_allow_returns_everything()
    {
        const string treeId = "agf-allow";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob", "user/carol");
        // Default decision is a plain Allow() with a null KeyFilter, exercising
        // the zero-per-key-cost hot path.

        var keys = await CollectAsync(tree.KeysAsync());
        var count = await tree.CountAsync();
        var many = await tree.GetManyAsync(new List<string> { "user/alice", "user/bob", "user/carol" });

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "user/alice", "user/bob", "user/carol" }));
            Assert.That(count, Is.EqualTo(3));
            Assert.That(many.Keys, Is.EquivalentTo(new[] { "user/alice", "user/bob", "user/carol" }));
        });
    }

    [Test]
    public async Task KeysAsync_with_reject_all_filter_returns_empty()
    {
        const string treeId = "agf-reject";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");
        // A deny-whole-scan is expressed through the KeyFilter construct, not a
        // separate deny code path.
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId
                ? LatticeAccessDecision.Filtered(static _ => false)
                : LatticeAccessDecision.Allow();

        var keys = await CollectAsync(tree.KeysAsync());
        var count = await tree.CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.Empty);
            Assert.That(count, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task KeysAsync_under_system_origin_scope_bypasses_filter()
    {
        const string treeId = "agf-sysorigin";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/amy", "user/bob", "user/carol");
        FilterUserAToTree(treeId);

        // A system-origin turn must skip the gate entirely: the RequestContext
        // marker flows to the grain on the enumeration call.
        List<string> keys;
        int count;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            keys = await CollectAsync(tree.KeysAsync());
            count = await tree.CountAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.EquivalentTo(new[] { "user/alice", "user/amy", "user/bob", "user/carol" }));
            Assert.That(count, Is.EqualTo(4));
        });
    }
}
