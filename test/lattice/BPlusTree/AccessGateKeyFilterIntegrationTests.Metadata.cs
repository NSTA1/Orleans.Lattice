using System.Collections.Concurrent;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Access-gate regression coverage for the <b>metadata and existence</b> verbs on
/// the <see cref="ILattice"/> facade grain (issue #1721). Before this suite,
/// <see cref="ILattice.DiagnoseAsync"/>, <see cref="ILattice.GetStorageUsageAsync"/>,
/// <see cref="ILattice.TreeExistsAsync"/>, and
/// <see cref="ILattice.GetHistoryRetentionAsync"/> performed <b>no gate call at
/// all</b>, so any caller able to address the grain could read a tree's
/// volumetrics and probe its existence under <c>DefaultEffect = Deny</c> with zero
/// grants - and, with the tenancy add-on on, could do so across tenants, because
/// tenant isolation is composed inside the gate and a verb that never calls the
/// gate never reaches it.
/// <para>
/// Each verb is pinned three ways: the gate is <em>consulted</em> at all (with the
/// correct whole-tree <see cref="LatticeOperation.Read"/> request shape), a denial
/// is honoured, and an authorized caller is still served. The two volumetric verbs
/// additionally refuse a partial-coverage (filtered) allow, since a per-shard count
/// or byte aggregate cannot be narrowed per key without still disclosing the keys
/// it counted; <see cref="ILattice.TreeExistsAsync"/> deliberately does the opposite
/// on both axes - a partial allow still sees existence, and a denial reports the
/// tree as <em>absent</em> rather than throwing, so "exists but I cannot read it"
/// stays indistinguishable from "does not exist".
/// </para>
/// </summary>
public partial class AccessGateKeyFilterIntegrationTests
{
    /// <summary>
    /// Installs a decision that denies every request naming <paramref name="treeId"/>
    /// and allows everything else, so unrelated trees and internal system trees are
    /// undisturbed. This is the shape a <c>DefaultEffect = Deny</c> cluster presents
    /// to a caller holding no grant on the tree.
    /// </summary>
    private static void DenyTree(string treeId) =>
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == treeId
                ? LatticeAccessDecision.Deny("caller holds no grant on this tree")
                : LatticeAccessDecision.Allow();

    /// <summary>
    /// Installs an allow-all decision that records every request naming
    /// <paramref name="treeId"/>, so a test can assert the gate was consulted at all
    /// and with the expected request shape. Requests for other trees are allowed and
    /// not recorded.
    /// </summary>
    private static ConcurrentQueue<LatticeAccessRequest> RecordRequestsFor(string treeId)
    {
        var seen = new ConcurrentQueue<LatticeAccessRequest>();
        ConfigurableAccessGate.Decide = req =>
        {
            if (req.TreeId == treeId)
            {
                seen.Enqueue(req);
            }

            return LatticeAccessDecision.Allow();
        };
        return seen;
    }

    /// <summary>
    /// Asserts that a whole-tree read request was observed for the tree: the exact
    /// shape the ungated verbs previously never issued.
    /// </summary>
    private static void AssertWholeTreeReadObserved(
        ConcurrentQueue<LatticeAccessRequest> seen,
        string treeId,
        string verb)
    {
        var reads = seen.Where(r => r.Operation == LatticeOperation.Read).ToList();
        Assert.That(reads, Is.Not.Empty, $"{verb} must consult the access gate");
        Assert.Multiple(() =>
        {
            foreach (var read in reads)
            {
                Assert.That(read.TreeId, Is.EqualTo(treeId));
                Assert.That(read.Key, Is.Null, $"{verb} is a whole-tree read, not a point read");
                Assert.That(read.RangeStart, Is.Null);
                Assert.That(read.RangeEnd, Is.Null);
            }
        });
    }

    // ---- DiagnoseAsync ---------------------------------------------------

    [Test]
    public async Task DiagnoseAsync_consults_the_gate_with_a_whole_tree_read()
    {
        const string treeId = "agf-diagnose-observed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        var seen = RecordRequestsFor(treeId);
        await tree.DiagnoseAsync();

        AssertWholeTreeReadObserved(seen, treeId, nameof(ILattice.DiagnoseAsync));
    }

    [Test]
    public async Task DiagnoseAsync_is_denied_when_the_caller_may_not_read_the_tree()
    {
        const string treeId = "agf-diagnose-denied";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        DenyTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.DiagnoseAsync());
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.DiagnoseAsync(deep: true));
    }

    [Test]
    public async Task DiagnoseAsync_refuses_a_partial_coverage_allow_rather_than_narrowing()
    {
        const string treeId = "agf-diagnose-filtered";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        // A prefix-granted caller may read part of the tree, but the report
        // aggregates counts over every key, so it is refused rather than served
        // with figures covering keys the caller may not read.
        FilterUserAToTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.DiagnoseAsync());
    }

    [Test]
    public async Task DiagnoseAsync_still_serves_a_caller_authorized_over_the_whole_tree()
    {
        const string treeId = "agf-diagnose-allowed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        var report = await tree.DiagnoseAsync();

        Assert.That(report.TotalLiveKeys, Is.EqualTo(2),
            "an authorized caller still receives the full report");
    }

    [Test]
    public async Task DiagnoseAsync_under_a_system_origin_scope_bypasses_the_gate()
    {
        const string treeId = "agf-diagnose-sysorigin";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        // Internal machinery must never self-deny: the system-origin marker flows
        // to the grain on the call and short-circuits enforcement.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var report = await tree.DiagnoseAsync();
            Assert.That(report.TotalLiveKeys, Is.EqualTo(1));
        }
    }

    // ---- GetStorageUsageAsync --------------------------------------------

    [Test]
    public async Task GetStorageUsageAsync_consults_the_gate_with_a_whole_tree_read()
    {
        const string treeId = "agf-usage-observed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        var seen = RecordRequestsFor(treeId);
        await tree.GetStorageUsageAsync();

        AssertWholeTreeReadObserved(seen, treeId, nameof(ILattice.GetStorageUsageAsync));
    }

    [Test]
    public async Task GetStorageUsageAsync_is_denied_when_the_caller_may_not_read_the_tree()
    {
        const string treeId = "agf-usage-denied";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        DenyTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.GetStorageUsageAsync());
    }

    [Test]
    public async Task GetStorageUsageAsync_refuses_a_partial_coverage_allow_rather_than_narrowing()
    {
        const string treeId = "agf-usage-filtered";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        FilterUserAToTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.GetStorageUsageAsync());
    }

    [Test]
    public async Task GetStorageUsageAsync_still_serves_a_caller_authorized_over_the_whole_tree()
    {
        const string treeId = "agf-usage-allowed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        var usage = await tree.GetStorageUsageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(usage.TreeId, Is.EqualTo(treeId));
            Assert.That(usage.LiveKeys, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task GetStorageUsageAsync_under_a_system_origin_scope_bypasses_the_gate()
    {
        const string treeId = "agf-usage-sysorigin";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var usage = await tree.GetStorageUsageAsync();
            Assert.That(usage.LiveKeys, Is.EqualTo(1));
        }
    }

    // ---- TreeExistsAsync -------------------------------------------------

    [Test]
    public async Task TreeExistsAsync_consults_the_gate_with_a_whole_tree_read()
    {
        const string treeId = "agf-exists-observed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        var seen = RecordRequestsFor(treeId);
        await tree.TreeExistsAsync();

        AssertWholeTreeReadObserved(seen, treeId, nameof(ILattice.TreeExistsAsync));
    }

    [Test]
    public async Task TreeExistsAsync_reports_a_denied_tree_as_absent_instead_of_throwing()
    {
        const string treeId = "agf-exists-denied";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        // The tree demonstrably exists for an authorized caller.
        Assert.That(await tree.TreeExistsAsync(), Is.True);

        DenyTree(treeId);

        Assert.That(await tree.TreeExistsAsync(), Is.False,
            "a denied caller must not be able to distinguish 'exists but I cannot read it' from 'does not exist'");
    }

    [Test]
    public async Task TreeExistsAsync_still_reports_existence_for_a_partial_coverage_allow()
    {
        const string treeId = "agf-exists-filtered";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice", "user/bob");

        // A prefix-granted caller may read part of the tree, so its existence is
        // not a secret from them: the per-key surfaces still prune the keys they
        // may not observe. Hiding it here would break every consumer that probes
        // existence before a filtered read.
        FilterUserAToTree(treeId);

        Assert.That(await tree.TreeExistsAsync(), Is.True);
    }

    [Test]
    public async Task TreeExistsAsync_under_a_system_origin_scope_bypasses_the_gate()
    {
        const string treeId = "agf-exists-sysorigin";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            Assert.That(await tree.TreeExistsAsync(), Is.True);
        }
    }

    [Test]
    public async Task TreeExistsAsync_still_reports_an_unwritten_tree_as_absent_when_authorized()
    {
        const string treeId = "agf-exists-never-written";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();

        Assert.That(await tree.TreeExistsAsync(), Is.False,
            "gating must not change the answer for a tree that genuinely does not exist");
    }

    // ---- GetHistoryRetentionAsync ----------------------------------------

    [Test]
    public async Task GetHistoryRetentionAsync_consults_the_gate_with_a_whole_tree_read()
    {
        const string treeId = "agf-retention-observed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        var seen = RecordRequestsFor(treeId);
        await tree.GetHistoryRetentionAsync();

        AssertWholeTreeReadObserved(seen, treeId, nameof(ILattice.GetHistoryRetentionAsync));
    }

    [Test]
    public async Task GetHistoryRetentionAsync_is_denied_when_the_caller_may_not_read_the_tree()
    {
        const string treeId = "agf-retention-denied";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => tree.GetHistoryRetentionAsync());
    }

    [Test]
    public async Task GetHistoryRetentionAsync_still_serves_a_caller_authorized_over_the_whole_tree()
    {
        const string treeId = "agf-retention-allowed";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        ConfigurableAccessGate.Decide = static _ => LatticeAccessDecision.Allow();
        await tree.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, TimeSpan.FromHours(3));

        var settings = await tree.GetHistoryRetentionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(settings.Mode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(settings.Window, Is.EqualTo(TimeSpan.FromHours(3)));
        });
    }

    [Test]
    public async Task GetHistoryRetentionAsync_under_a_system_origin_scope_bypasses_the_gate()
    {
        const string treeId = "agf-retention-sysorigin";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await SeedAsync(tree, "user/alice");

        DenyTree(treeId);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var settings = await tree.GetHistoryRetentionAsync();
            Assert.That(settings.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        }
    }

    // ---- Cross-tree isolation -------------------------------------------

    [Test]
    public async Task A_grant_on_one_tree_does_not_disclose_another_trees_metadata()
    {
        const string ownedId = "agf-iso-owned";
        const string victimId = "agf-iso-victim";
        var owned = _cluster.GrainFactory.GetGrain<ILattice>(ownedId);
        var victim = _cluster.GrainFactory.GetGrain<ILattice>(victimId);
        await SeedAsync(owned, "mine/one");
        await SeedAsync(victim, "secret/one", "secret/two");

        // The attack shape from the report: the caller holds a grant on a tree it
        // owns and merely *names* another tree it does not. Tenant isolation is
        // composed inside the gate, so a verb that reaches the gate at all is also
        // the verb that reaches the tenant enforcer.
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == ownedId
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not your tree");

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => victim.DiagnoseAsync());
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => victim.GetStorageUsageAsync());
            Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => victim.GetHistoryRetentionAsync());
        });

        Assert.That(await victim.TreeExistsAsync(), Is.False,
            "the existence oracle must not confirm another tree's presence either");

        // The caller's own tree is entirely unaffected.
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == ownedId
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not your tree");
        Assert.That(await owned.TreeExistsAsync(), Is.True);
        Assert.That((await owned.DiagnoseAsync()).TotalLiveKeys, Is.EqualTo(1));
    }
}
