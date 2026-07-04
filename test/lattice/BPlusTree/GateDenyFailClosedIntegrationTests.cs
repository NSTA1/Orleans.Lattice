using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Adversarial fail-closed coverage for the write / mutate path of the
/// <see cref="ILattice"/> facade access gate (issue #1103). The read path's
/// key-filter enforcement is covered elsewhere; this suite drives a
/// <see cref="LatticeAccessDecision.Deny(string)"/> decision for every mutating
/// operation class and asserts each is refused with
/// <see cref="LatticeAuthorizationDeniedException"/> and leaves no partial state
/// (fail-closed). Reads for the tree stay allowed so the absence of a written
/// value is directly observable.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class GateDenyFailClosedIntegrationTests
{
    private AccessGateKeyFilterClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    // Every operation except a pure point / range read is a mutation this suite denies.
    private const LatticeOperation MutatingMask =
        LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.Admin;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AccessGateKeyFilterClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public void TearDown() => ConfigurableAccessGate.Reset();

    private static byte[] Val(string key) => Encoding.UTF8.GetBytes(key);

    // Deny every mutating op on the given tree; leave reads allowed so fail-closed
    // (no partial write) is observable by reading the key back.
    private static void DenyMutationsOn(string treeId) =>
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId && (req.Operation & MutatingMask) != 0
                ? LatticeAccessDecision.Deny("adversarial fail-closed probe")
                : LatticeAccessDecision.Allow();

    private ILattice Tree(string treeId) => _cluster.GrainFactory.GetGrain<ILattice>(treeId);

    [Test]
    public async Task SetAsync_is_refused_fail_closed_when_denied()
    {
        const string treeId = "deny-set";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.SetAsync("k", Val("k")));

        // Fail-closed: nothing was written.
        Assert.That(await tree.GetAsync("k"), Is.Null);
    }

    [Test]
    public void SetAsync_with_ttl_is_refused_when_denied()
    {
        const string treeId = "deny-set-ttl";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.SetAsync("k", Val("k"), TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void DeleteAsync_is_refused_when_denied()
    {
        const string treeId = "deny-delete";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.DeleteAsync("k"));
    }

    [Test]
    public void DeleteRangeAsync_is_refused_when_denied()
    {
        const string treeId = "deny-range-delete";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.DeleteRangeAsync("a", "z"));
    }

    [Test]
    public void ApplyCrdtDeltaAsync_is_refused_when_denied()
    {
        const string treeId = "deny-crdt";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.ApplyCrdtDeltaAsync("k", LatticeMergeMode.LwwRegister, Val("k")));
    }

    [Test]
    public void SetManyAsync_batch_is_refused_when_denied()
    {
        const string treeId = "deny-setmany";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        var entries = new List<KeyValuePair<string, byte[]>> { new("a", Val("a")), new("b", Val("b")) };
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.SetManyAsync(entries));
    }

    [Test]
    public async Task SetManyAtomicAsync_is_refused_fail_closed_when_denied()
    {
        const string treeId = "deny-atomic";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        var entries = new List<KeyValuePair<string, byte[]>> { new("a", Val("a")), new("b", Val("b")) };
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.SetManyAtomicAsync(entries));

        // Fail-closed: no leg of the atomic write landed.
        Assert.That(await tree.GetAsync("a"), Is.Null);
        Assert.That(await tree.GetAsync("b"), Is.Null);
    }

    [Test]
    public void SetManyAtomicAsync_with_operation_id_is_refused_when_denied()
    {
        const string treeId = "deny-atomic-opid";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        var entries = new List<KeyValuePair<string, byte[]>> { new("a", Val("a")) };
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.SetManyAtomicAsync(entries, "op-1"));
    }

    [Test]
    public void SetManyAtomicAsync_mixed_upsert_and_delete_is_refused_when_denied()
    {
        const string treeId = "deny-atomic-mixed";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        var upserts = new List<KeyValuePair<string, byte[]>> { new("a", Val("a")) };
        var deletes = new List<string> { "b" };
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.SetManyAtomicAsync(upserts, deletes, "op-2"));
    }

    [Test]
    public void BulkLoadAsync_is_refused_when_denied()
    {
        const string treeId = "deny-bulk";
        var tree = Tree(treeId);
        DenyMutationsOn(treeId);

        var entries = new List<KeyValuePair<string, byte[]>> { new("a", Val("a")), new("b", Val("b")) };
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.BulkLoadAsync(entries));
    }
}
