using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
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
/// <para>
/// The same fixture also pins the <b>cross-tree source authorization</b> of
/// <c>MergeAsync</c>: a merge reads the whole of a caller-supplied source tree,
/// so the gate is consulted for that source and not only for the destination.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class AccessGateKeyFilterIntegrationTests
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

        var entries = await CollectAsync(tree.ScanEntriesAsync());

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

    // ---- Cross-tree source authorization (MergeAsync) --------------------
    // A merge copies the whole of a caller-supplied *source* tree into the
    // calling tree, where it becomes readable under the calling tree's own read
    // policy. Authorizing only the destination would let a caller holding Admin
    // on a tree it owns siphon any other tree in the cluster into one it can
    // read, so the gate must govern the source as well - and, because a merge
    // copies the source in its entirety, a partial-coverage (filtered) allow on
    // the source must be refused rather than silently narrowed.

    private static async Task PollUntilAsync(Func<Task<bool>> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await predicate())
                return;
            await Task.Delay(100);
        }

        Assert.Fail($"Condition was not met within {timeout}.");
    }

    [Test]
    public async Task MergeAsync_denies_when_the_caller_cannot_read_the_source_tree()
    {
        const string sourceId = "agf-merge-denied-src";
        const string targetId = "agf-merge-denied-dst";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceId);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetId);
        await SeedAsync(source, "secret/one", "secret/two");

        // The attacker shape: Admin on the destination it owns, no read on the
        // source it covets.
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == sourceId
                ? LatticeAccessDecision.Deny("caller may not read the merge source")
                : LatticeAccessDecision.Allow();

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => target.MergeAsync(sourceId));

        // Nothing was copied: the refusal happens before the merge coordinator
        // is engaged, so no source entry ever reaches the destination.
        ConfigurableAccessGate.Reset();
        Assert.Multiple(async () =>
        {
            Assert.That(await target.GetAsync("secret/one"), Is.Null);
            Assert.That(await target.GetAsync("secret/two"), Is.Null);
        });
    }

    [Test]
    public async Task MergeAsync_denies_a_partial_coverage_allow_on_the_source_tree()
    {
        const string sourceId = "agf-merge-filtered-src";
        const string targetId = "agf-merge-filtered-dst";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceId);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetId);
        await SeedAsync(source, "user/alice", "user/bob");

        // A filtered allow authorizes only part of the source. A merge cannot
        // copy a key subset without silently diverging from the source, so this
        // is refused rather than narrowed (fail-closed).
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == sourceId
                ? LatticeAccessDecision.Filtered(static k => k.StartsWith("user/a", StringComparison.Ordinal))
                : LatticeAccessDecision.Allow();

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => target.MergeAsync(sourceId));

        ConfigurableAccessGate.Reset();
        Assert.That(await target.GetAsync("user/alice"), Is.Null,
            "a partial-coverage allow copies nothing at all");
    }

    [Test]
    public async Task MergeAsync_succeeds_when_the_caller_may_read_the_whole_source_tree()
    {
        const string sourceId = "agf-merge-allowed-src";
        const string targetId = "agf-merge-allowed-dst";
        var source = _cluster.GrainFactory.GetGrain<ILattice>(sourceId);
        var target = _cluster.GrainFactory.GetGrain<ILattice>(targetId);
        await SeedAsync(source, "shared/one");

        // A uniform allow over the source is the authorized shape: the merge
        // proceeds exactly as before the source-side gate was added.
        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        await target.MergeAsync(sourceId);
        // Drive the drain deterministically rather than waiting on the
        // coordinator's reminder, which does not tick inside the test cluster.
        await _cluster.GrainFactory.GetGrain<ITreeMergeGrain>(targetId).RunMergePassAsync();
        await PollUntilAsync(async () => await target.IsMergeCompleteAsync(), TimeSpan.FromSeconds(20));

        Assert.That(await target.GetAsync("shared/one"), Is.Not.Null,
            "an authorized merge still copies the source");
    }
}
