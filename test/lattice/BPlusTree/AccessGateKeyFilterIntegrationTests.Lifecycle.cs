using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Access-gate regression coverage for the <b>lifecycle-status, projection, and
/// read-disclosing-write</b> verbs on the <see cref="ILattice"/> facade grain
/// (issue #1733), the follow-on to the metadata sweep in #1721 / #1722.
/// <para>
/// Three distinct gaps are pinned here:
/// </para>
/// <list type="number">
/// <item><description>
/// <see cref="ILattice.GetOrSetAsync"/> enforced only
/// <see cref="LatticeOperation.Write"/> but <em>returns the pre-existing stored
/// value</em> on the read-hit path. <see cref="LatticeOperation"/> grants are
/// independent per-bit, so a Write-without-Read rule is an expressible policy and
/// such a subject could use the verb as a read oracle to recover plaintext it is
/// explicitly denied.
/// </description></item>
/// <item><description>
/// <see cref="ILattice.GetMaterialiserLagAsync"/> performed <b>no gate call at
/// all</b>, unlike both of its siblings in the same file, while fanning out across
/// every physical shard - disclosing tree existence and shard topology and forcing
/// shard activation for an unauthorized caller.
/// </description></item>
/// <item><description>
/// The four lifecycle-status verbs (<see cref="ILattice.IsMergeCompleteAsync"/>,
/// <see cref="ILattice.IsSnapshotCompleteAsync"/>,
/// <see cref="ILattice.IsResizeCompleteAsync"/>,
/// <see cref="ILattice.IsReshardCompleteAsync"/>) performed no gate call, though
/// every verb that <em>initiates</em> the corresponding operation enforces Admin or
/// TreeLifecycle.
/// </description></item>
/// </list>
/// <para>
/// The final fixture in this file locks in the load-bearing caveat that comes with
/// gap 3: <see cref="HotShardMonitorGrain"/> polls all four status verbs on a timer
/// with no caller identity, so gating them without granting the monitor system
/// origin would have made a deny-by-default tree silently stop auto-splitting.
/// </para>
/// </summary>
public partial class AccessGateKeyFilterIntegrationTests
{
    /// <summary>
    /// Allows every operation on the tree except <see cref="LatticeOperation.Read"/>,
    /// which is denied. This is precisely the Write-without-Read grant that makes
    /// <see cref="ILattice.GetOrSetAsync"/>'s read-hit return path a disclosure.
    /// </summary>
    private static void AllowWriteDenyReadOn(string treeId) =>
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId && req.Operation == LatticeOperation.Read
                ? LatticeAccessDecision.Deny("subject holds Write but not Read on this tree")
                : LatticeAccessDecision.Allow();

    // ---- Finding 1: GetOrSetAsync read disclosure ------------------------

    [Test]
    public async Task GetOrSetAsync_does_not_disclose_the_stored_value_to_a_write_only_caller()
    {
        // The crux of finding 1. The key already holds a value; a subject with
        // Write but not Read must not be able to read it back through GetOrSet.
        const string treeId = "agf-getorset-writeonly";
        const string key = "secret/one";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, key);

        AllowWriteDenyReadOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => tree.GetOrSetAsync(key, Encoding.UTF8.GetBytes("attacker-supplied")),
            "GetOrSet discloses the stored value on a read hit, so it must require Read as well as Write");

        // The denial must also have prevented any mutation.
        ConfigurableAccessGate.Reset();
        var stored = await tree.GetAsync(key);
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo(key),
            "a denied GetOrSet must not have written the attacker's value either");
    }

    [Test]
    public async Task GetOrSetAsync_denies_a_write_only_caller_even_when_the_key_is_absent()
    {
        // Read is enforced unconditionally rather than lazily on the read-hit
        // path. Were it deferred until `existing` is known to be non-null, the
        // allow/deny outcome would itself become a key-existence oracle for a
        // Write-only subject: absent -> success, present -> denial. This test is
        // what forces the up-front placement.
        const string treeId = "agf-getorset-absent";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "unrelated/key");

        AllowWriteDenyReadOn(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            () => tree.GetOrSetAsync("never/written", Encoding.UTF8.GetBytes("v")),
            "the denial must not depend on whether the key exists, or it becomes an existence oracle");
    }

    [Test]
    public async Task GetOrSetAsync_consults_the_gate_for_a_point_read_of_the_key()
    {
        const string treeId = "agf-getorset-observed";
        const string key = "obs/one";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var seen = RecordRequestsFor(treeId);
        await tree.GetOrSetAsync(key, Encoding.UTF8.GetBytes("v"));

        var reads = seen.Where(r => r.Operation == LatticeOperation.Read).ToList();
        Assert.That(reads, Is.Not.Empty, "GetOrSet must consult the gate for Read, not only Write");
        Assert.Multiple(() =>
        {
            Assert.That(seen.Any(r => r.Operation == LatticeOperation.Write),
                Is.True, "the existing Write enforcement must be retained");
            foreach (var read in reads)
            {
                Assert.That(read.TreeId, Is.EqualTo(treeId));
                Assert.That(read.Key, Is.EqualTo(key), "GetOrSet reads a single key, so the Read request is a point read");
            }
        });
    }

    [Test]
    public async Task GetOrSetAsync_still_serves_a_caller_holding_both_write_and_read()
    {
        const string treeId = "agf-getorset-allowed";
        const string key = "ok/one";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        var first = await tree.GetOrSetAsync(key, Encoding.UTF8.GetBytes("first"));
        var second = await tree.GetOrSetAsync(key, Encoding.UTF8.GetBytes("second"));

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Null, "the insert path returns null when no value was present");
            Assert.That(Encoding.UTF8.GetString(second!), Is.EqualTo("first"),
                "the read-hit path returns the pre-existing value to an authorized caller");
        });
    }

    // ---- Finding 2: GetMaterialiserLagAsync ------------------------------

    [Test]
    public async Task GetMaterialiserLagAsync_consults_the_gate_with_a_whole_tree_read()
    {
        const string treeId = "agf-lag-observed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        var seen = RecordRequestsFor(treeId);
        await tree.GetMaterialiserLagAsync();

        AssertWholeTreeReadObserved(seen, treeId, nameof(ILattice.GetMaterialiserLagAsync));
    }

    [Test]
    public async Task GetMaterialiserLagAsync_is_denied_when_the_caller_may_not_read_the_tree()
    {
        const string treeId = "agf-lag-denied";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.GetMaterialiserLagAsync());
    }

    [Test]
    public async Task GetMaterialiserLagAsync_still_serves_a_caller_authorized_over_the_whole_tree()
    {
        const string treeId = "agf-lag-allowed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        Assert.That(await tree.GetMaterialiserLagAsync(), Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public async Task GetMaterialiserLagAsync_under_a_system_origin_scope_bypasses_the_gate()
    {
        const string treeId = "agf-lag-sysorigin";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(await tree.GetMaterialiserLagAsync(), Is.GreaterThanOrEqualTo(0));
        }
    }

    // ---- Finding 3: the four lifecycle-status verbs ----------------------

    /// <summary>
    /// The four observe-only lifecycle-status verbs, each paired with the name it
    /// reports under so a failure names the offending verb.
    /// </summary>
    private static IEnumerable<TestCaseData> LifecycleStatusVerbs()
    {
        yield return new TestCaseData(
            new Func<ILattice, Task<bool>>(t => t.IsMergeCompleteAsync()))
            .SetName("IsMergeCompleteAsync");
        yield return new TestCaseData(
            new Func<ILattice, Task<bool>>(t => t.IsSnapshotCompleteAsync()))
            .SetName("IsSnapshotCompleteAsync");
        yield return new TestCaseData(
            new Func<ILattice, Task<bool>>(t => t.IsResizeCompleteAsync()))
            .SetName("IsResizeCompleteAsync");
        yield return new TestCaseData(
            new Func<ILattice, Task<bool>>(t => t.IsReshardCompleteAsync()))
            .SetName("IsReshardCompleteAsync");
    }

    [TestCaseSource(nameof(LifecycleStatusVerbs))]
    public async Task Lifecycle_status_verb_consults_the_gate_with_a_whole_tree_read(
        Func<ILattice, Task<bool>> verb)
    {
        var treeId = $"agf-lifecycle-observed-{TestContext.CurrentContext.Test.Name}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        var seen = RecordRequestsFor(treeId);
        await verb(tree);

        AssertWholeTreeReadObserved(seen, treeId, TestContext.CurrentContext.Test.Name);
    }

    [TestCaseSource(nameof(LifecycleStatusVerbs))]
    public async Task Lifecycle_status_verb_is_denied_when_the_caller_may_not_read_the_tree(
        Func<ILattice, Task<bool>> verb)
    {
        var treeId = $"agf-lifecycle-denied-{TestContext.CurrentContext.Test.Name}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => verb(tree));
    }

    [TestCaseSource(nameof(LifecycleStatusVerbs))]
    public async Task Lifecycle_status_verb_still_serves_a_caller_authorized_over_the_whole_tree(
        Func<ILattice, Task<bool>> verb)
    {
        var treeId = $"agf-lifecycle-allowed-{TestContext.CurrentContext.Test.Name}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        Assert.That(await verb(tree), Is.True, "an idle tree reports its lifecycle operation complete");
    }

    [TestCaseSource(nameof(LifecycleStatusVerbs))]
    public async Task Lifecycle_status_verb_under_a_system_origin_scope_bypasses_the_gate(
        Func<ILattice, Task<bool>> verb)
    {
        var treeId = $"agf-lifecycle-sysorigin-{TestContext.CurrentContext.Test.Name}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(await verb(tree), Is.True);
        }
    }
}
