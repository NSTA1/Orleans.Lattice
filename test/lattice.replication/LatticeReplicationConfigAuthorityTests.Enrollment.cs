using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Reconciliation tests for the two replication enrollment sources the authority
/// reports over: the runtime config OR-Map and the static
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> deployment map.
/// <para>
/// Regression coverage for the defect where the config report described only the
/// runtime OR-Map, so an estate enrolled purely through deployment
/// configuration - the reference architecture's own standard path - reported no
/// trees at all while demonstrably replicating.
/// </para>
/// </summary>
public sealed partial class LatticeReplicationConfigAuthorityTests
{
    private const string StaticTree = "inventory";

    private static Dictionary<string, LatticeMergeMode> StaticMap(
        params (string TreeId, LatticeMergeMode Mode)[] declarations)
    {
        var map = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
        foreach (var (treeId, mode) in declarations)
        {
            map[treeId] = mode;
        }

        return map;
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_reports_a_statically_declared_tree_with_no_runtime_entry()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap((Tree, LatticeMergeMode.LwwRegister)));

        var statuses = await authority.GetAllTreeStatusesAsync();

        Assert.That(statuses.Keys, Is.EquivalentTo(new[] { Tree }));
        var status = statuses[Tree];
        Assert.Multiple(() =>
        {
            Assert.That(status.Enabled, Is.True);
            Assert.That(status.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(status.Ambiguous, Is.False);
            Assert.That(status.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.Static));
        });
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_is_empty_when_neither_source_declares_a_tree()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);

        Assert.That(await authority.GetAllTreeStatusesAsync(), Is.Empty);
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_reports_a_runtime_only_tree_as_runtime_sourced()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        var status = (await authority.GetAllTreeStatusesAsync())[Tree];

        Assert.Multiple(() =>
        {
            Assert.That(status.Enabled, Is.True);
            Assert.That(status.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(status.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.Runtime));
        });
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_unions_both_sources_without_duplicating_a_shared_tree()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap(
                (Tree, LatticeMergeMode.OrSet),
                (StaticTree, LatticeMergeMode.LwwRegister)));
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        var statuses = await authority.GetAllTreeStatusesAsync();

        Assert.That(statuses.Keys, Is.EquivalentTo(new[] { Tree, StaticTree }));
        Assert.Multiple(() =>
        {
            Assert.That(
                statuses[Tree].Source,
                Is.EqualTo(LatticeReplicationEnrollmentSource.RuntimeAndStatic));
            Assert.That(
                statuses[StaticTree].Source,
                Is.EqualTo(LatticeReplicationEnrollmentSource.Static));
        });
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_prefers_the_runtime_mode_over_a_divergent_static_declaration()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap((Tree, LatticeMergeMode.LwwRegister)));
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);

        var status = (await authority.GetAllTreeStatusesAsync())[Tree];

        Assert.Multiple(() =>
        {
            Assert.That(status.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(status.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.RuntimeAndStatic));
        });
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_reports_a_runtime_disabled_tree_as_still_enrolled_when_statically_declared()
    {
        // The static map is a floor: the commit-path resolver falls back to it
        // when the runtime entry yields no enabled unambiguous mode, so the tree
        // keeps shipping and the report must not claim otherwise.
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap((Tree, LatticeMergeMode.LwwRegister)));
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);
        await authority.DisableReplicationAsync(Tree);

        var status = (await authority.GetAllTreeStatusesAsync())[Tree];

        Assert.Multiple(() =>
        {
            Assert.That(status.Enabled, Is.True);
            Assert.That(status.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(status.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.Static));
        });
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_reports_a_runtime_disabled_tree_as_disabled_when_not_statically_declared()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(store);
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.OrSet);
        await authority.DisableReplicationAsync(Tree);

        var status = (await authority.GetAllTreeStatusesAsync())[Tree];

        Assert.Multiple(() =>
        {
            Assert.That(status.Enabled, Is.False);
            Assert.That(status.Mode, Is.EqualTo(LatticeMergeMode.OrSet), "the fixed mode is retained for re-enable");
            Assert.That(status.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.Runtime));
        });
    }

    [Test]
    public async Task GetAllTreeStatusesAsync_keeps_ambiguity_fail_closed_over_a_static_declaration()
    {
        var store = new InMemoryConfigStore();
        store.Seed(Tree, ReplicationConfigSnapshotTestHelpers.AmbiguousEnabled());
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap((Tree, LatticeMergeMode.LwwRegister)));

        var status = (await authority.GetAllTreeStatusesAsync())[Tree];

        Assert.Multiple(() =>
        {
            Assert.That(status.Ambiguous, Is.True);
            Assert.That(status.Mode, Is.Null, "a static declaration must never resolve a divergent runtime mode");
            Assert.That(status.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.RuntimeAndStatic));
        });
    }

    [Test]
    public async Task GetTreeStatusAsync_reports_a_statically_declared_tree_with_no_runtime_entry()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap((Tree, LatticeMergeMode.OrMap)));

        var status = await authority.GetTreeStatusAsync(Tree);

        Assert.That(status, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(status!.Value.Enabled, Is.True);
            Assert.That(status.Value.Mode, Is.EqualTo(LatticeMergeMode.OrMap));
            Assert.That(status.Value.Source, Is.EqualTo(LatticeReplicationEnrollmentSource.Static));
        });
    }

    [Test]
    public async Task GetTreeStatusAsync_returns_null_when_neither_source_declares_the_tree()
    {
        var store = new InMemoryConfigStore();
        var authority = CreateAuthority(
            store,
            staticTrees: StaticMap((StaticTree, LatticeMergeMode.OrSet)));

        Assert.That(await authority.GetTreeStatusAsync(Tree), Is.Null);
    }

    /// <summary>
    /// The load-bearing invariant: for every combination of runtime entry and
    /// static declaration, the reported status must describe exactly what the
    /// commit-path <see cref="SnapshotLatticeMergeModeResolver"/> does. A report
    /// that diverges from the resolver is precisely the defect this
    /// reconciliation fixes, so the two are asserted against each other rather
    /// than against a hand-written expectation.
    /// </summary>
    [Test]
    public async Task GetAllTreeStatusesAsync_agrees_with_the_commit_path_resolver_for_every_source_combination()
    {
        var store = new InMemoryConfigStore();
        store.Seed("runtime-enabled", ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.OrSet));
        store.Seed("runtime-disabled", ReplicationConfigSnapshotTestHelpers.DisabledWithMode(LatticeMergeMode.OrSet));
        store.Seed("runtime-ambiguous", ReplicationConfigSnapshotTestHelpers.AmbiguousEnabled());
        store.Seed("both-enabled", ReplicationConfigSnapshotTestHelpers.Enabled(LatticeMergeMode.OrSet));
        store.Seed("both-disabled", ReplicationConfigSnapshotTestHelpers.DisabledWithMode(LatticeMergeMode.OrSet));
        store.Seed("both-ambiguous", ReplicationConfigSnapshotTestHelpers.AmbiguousEnabled());

        var staticTrees = StaticMap(
            ("static-only", LatticeMergeMode.LwwRegister),
            ("both-enabled", LatticeMergeMode.LwwRegister),
            ("both-disabled", LatticeMergeMode.LwwRegister),
            ("both-ambiguous", LatticeMergeMode.LwwRegister));

        var authority = CreateAuthority(store, staticTrees: staticTrees);
        var resolver = ResolverOver(store, staticTrees);
        var statuses = await authority.GetAllTreeStatusesAsync();

        Assert.That(
            statuses.Keys,
            Is.EquivalentTo(new[]
            {
                "runtime-enabled", "runtime-disabled", "runtime-ambiguous",
                "both-enabled", "both-disabled", "both-ambiguous", "static-only",
            }),
            "every tree either source declares must appear in the report");

        Assert.Multiple(() =>
        {
            foreach (var (treeId, status) in statuses)
            {
                // The mode the report says is in force, or null when the report
                // says the tree is not currently shipping.
                var reported = status.Enabled && !status.Ambiguous ? status.Mode : null;
                Assert.That(
                    reported,
                    Is.EqualTo(resolver.Resolve(treeId)),
                    $"the report for '{treeId}' must match what the commit path resolves");
            }
        });
    }
}
