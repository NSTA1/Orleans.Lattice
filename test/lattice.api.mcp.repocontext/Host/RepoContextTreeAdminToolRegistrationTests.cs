using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Regression guard for the container's operator reach. The orphaned-leaf remedy
/// shipped with no way to invoke it on a running deployment: this container maps
/// only health and metrics routes and publishes only its MCP port, so a verb that
/// is not advertised as an MCP tool is unreachable from outside no matter how
/// completely it is implemented underneath. These tests assert the container
/// registers the tree-administration tool group, that the read-only orphaned-leaf
/// audit is advertised by it, that the mutating repair is now advertised too - the
/// tree-lifecycle opt-in is on and the seeded grant carries the matching
/// capability, so the repair is invocable rather than advertised and refused - and
/// that the rest of the surface that opt-in contributes is exactly the named set
/// below rather than whatever the library happens to add next.
/// </summary>
[TestFixture]
[FastInProcessHostFixture("Builds the host's service provider in-process and never starts the silo, so there is no storage and no cluster. Measured at 42 ms for all 9 tests including the one-time host build.")]
public sealed class RepoContextTreeAdminToolRegistrationTests
{
    private const string AuditToolName = "lattice_treeadmin_orphaned_leaves_audit";
    private const string RepairToolName = "lattice_treeadmin_orphaned_leaves_repair";

    /// <summary>
    /// Every tool the tree-lifecycle opt-in contributes beyond the read-only
    /// lifecycle verbs, measured on this host.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The point of naming them is that the opt-in is <b>not</b> a switch for the
    /// orphaned-leaf repair. It contributes 26 mutating verbs, of which the repair is
    /// one; the other 25 include tree deletion, purge, restore, reshard, resize, and
    /// WAL placement moves. Accepting that surface is a decision about this
    /// container - single-user, one trusted local caller, every verb still
    /// re-authorized per tree by the facade's fail-closed gate and still refused for
    /// the reserved <c>_lattice_</c> namespace - and this list is what makes it a
    /// stated decision rather than a side effect.
    /// </para>
    /// <para>
    /// If a library change adds a verb to this group, this test fails and the
    /// decision is retaken deliberately. That is the intent: on this box the failure
    /// is the review.
    /// </para>
    /// </remarks>
    private static readonly string[] LifecycleOptInTools =
    [
        "lattice_treeadmin_bulk_load_append",
        "lattice_treeadmin_bulk_load_begin",
        "lattice_treeadmin_bulk_load_commit",
        "lattice_treeadmin_compaction_trigger",
        "lattice_treeadmin_orphaned_leaves_repair",
        "lattice_treeadmin_retention_set",
        "lattice_treeadmin_tag_index_reconcile",
        "lattice_treeadmin_tree_create",
        "lattice_treeadmin_tree_delete",
        "lattice_treeadmin_tree_purge",
        "lattice_treeadmin_tree_recover",
        "lattice_treeadmin_tree_reshard",
        "lattice_treeadmin_tree_resize",
        "lattice_treeadmin_tree_resize_undo",
        "lattice_treeadmin_tree_restore",
        "lattice_treeadmin_tree_restore_revert",
        "lattice_treeadmin_tree_restore_set",
        "lattice_treeadmin_tree_set_alias",
        "lattice_treeadmin_tree_set_config",
        "lattice_treeadmin_tree_snapshot",
        "lattice_treeadmin_view_create",
        "lattice_treeadmin_view_drop",
        "lattice_treeadmin_view_rebuild",
        "lattice_treeadmin_view_reconcile",
        "lattice_treeadmin_wal_move_execute",
        "lattice_treeadmin_wal_move_reclaim",
    ];

    private string _root = null!;
    private WebApplication _app = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-treeadmin-tools-" + Guid.NewGuid().ToString("N"));
        var config = RepoContextHostConfiguration.FromConfiguration(
            new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [RepoContextHostConfiguration.DataRootKey] = _root,
                })
                .Build());

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseTestServer();
        _app = RepoContextHostBuilder.Build(builder, config);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _app?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root))
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup; a background handle may briefly hold a file.
            }
        }
    }

    private IReadOnlyList<string> TreeAdminToolNames()
        => _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .SelectMany(g => g.Tools)
            .Select(t => t.ProtocolTool.Name)
            .ToList();

    /// <summary>
    /// Pins the <b>whole</b> advertised surface, not only the treeadmin slice, so the
    /// before/after cost of the lifecycle opt-in stays a measured number on this
    /// container rather than an estimate.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Counted here are the tools the registered groups contribute <b>and</b> that the
    /// discovery core's per-tool filter admits under the seeded grant, which is what a
    /// client sees in <c>tools/list</c> apart from the two always-advertised meta
    /// tools (<c>lattice_capabilities</c>, <c>lattice_list_regions</c>) that no group
    /// contributes. So the client-visible total is this number plus two.
    /// </para>
    /// <para>
    /// Before the lifecycle opt-in the groups contributed 53 admitted tools (23
    /// repository-context, 30 tree-administration), i.e. 55 including the meta pair,
    /// which matches the count observed from a branch-built image. The opt-in adds the
    /// 26 verbs named above and nothing else.
    /// </para>
    /// </remarks>
    [Test]
    public void The_advertised_surface_is_the_measured_size()
    {
        var admitted = _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .SelectMany(group => group.Tools.Select(tool => (group, tool)))
            .Where(x =>
                (RepoContextGrant.Operations
                    & LatticeApiMcpGroupCapabilityMap.RequiredOperations(x.group.Group)) != LatticeOperation.None
                && (RepoContextGrant.Operations
                    & x.group.RequiredOperationsFor(x.tool.ProtocolTool.Name)) != LatticeOperation.None)
            .Select(x => x.tool.ProtocolTool.Name)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.Unique, "A duplicate tool name would make the count meaningless.");
            Assert.That(
                admitted, Has.Count.EqualTo(79),
                "53 before the lifecycle opt-in plus the 26 verbs it contributes. Add the two meta tools for the "
                + "81 a client sees, against 55 before.");
        });
    }

    [Test]
    public void The_container_registers_the_tree_admin_tool_group()
    {
        Assert.That(
            _app.Services.GetServices<ILatticeApiMcpToolGroup>().OfType<TreeAdminToolGroup>().ToList(),
            Has.Count.EqualTo(1),
            "Without this group the whole-tree operator verbs have no invocation path on this container.");
    }

    [Test]
    public void The_orphaned_leaf_audit_is_advertised()
    {
        Assert.That(TreeAdminToolNames(), Does.Contain(AuditToolName),
            "The audit is the documented first step and must be reachable over this container's MCP listener.");
    }

    [Test]
    public void The_orphaned_leaf_audit_is_annotated_read_only()
    {
        var tool = _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .SelectMany(g => g.Tools)
            .Single(t => t.ProtocolTool.Name == AuditToolName);

        Assert.Multiple(() =>
        {
            Assert.That(tool.ProtocolTool.Annotations?.ReadOnlyHint, Is.True);
            Assert.That(tool.ProtocolTool.Annotations?.DestructiveHint, Is.False);
        });
    }

    [Test]
    public void The_orphaned_leaf_repair_is_advertised()
    {
        Assert.That(TreeAdminToolNames(), Does.Contain(RepairToolName),
            "The tree-lifecycle opt-in is on and the seeded local-agent grant carries TreeLifecycle, so the "
            + "repair must be reachable over this container's single MCP listener.");
    }

    [Test]
    public void The_orphaned_leaf_repair_is_annotated_destructive()
    {
        var tool = _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>()
            .SelectMany(g => g.Tools)
            .Single(t => t.ProtocolTool.Name == RepairToolName);

        Assert.Multiple(() =>
        {
            Assert.That(tool.ProtocolTool.Annotations?.ReadOnlyHint, Is.False);
            Assert.That(tool.ProtocolTool.Annotations?.DestructiveHint, Is.True);
        });
    }

    /// <summary>
    /// The advertised repair must also pass the discovery core's per-tool filter
    /// under the seeded grant, because that filter is what puts advertisement and
    /// invocation in lock-step.
    /// </summary>
    /// <remarks>
    /// <c>LatticeApiMcpSessionConfigurator</c> builds one tool collection and serves
    /// both <c>tools/list</c> and <c>tools/call</c> from it, so a tool withheld by the
    /// filter is unreachable at invocation too. Contributing the tool is therefore
    /// necessary but not sufficient; this asserts the second half.
    /// </remarks>
    [Test]
    public void The_seeded_grant_admits_the_repair_through_the_discovery_filter()
    {
        ILatticeApiMcpToolGroup group = _app.Services.GetServices<ILatticeApiMcpToolGroup>()
            .OfType<TreeAdminToolGroup>().Single();

        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextGrant.Operations
                    & LatticeApiMcpGroupCapabilityMap.RequiredOperations(group.Group),
                Is.Not.EqualTo(LatticeOperation.None),
                "The group itself must be discoverable under the seeded grant.");
            Assert.That(
                RepoContextGrant.Operations & group.RequiredOperationsFor(RepairToolName),
                Is.Not.EqualTo(LatticeOperation.None),
                "A tool the per-tool filter withholds is unreachable at tools/call as well as tools/list.");
        });
    }

    /// <summary>
    /// Pins the whole surface the tree-lifecycle opt-in contributes, not only the
    /// repair, so widening it stays a stated decision.
    /// </summary>
    [Test]
    public void The_lifecycle_opt_in_contributes_exactly_the_named_mutating_verbs()
    {
        var withOptIn = TreeAdminToolNames().ToHashSet(StringComparer.Ordinal);
        var withoutOptIn = new TreeAdminToolGroup(_app.Services, Options.Create(new LatticeApiMcpOptions()))
            .Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

        Assert.That(
            withOptIn.Except(withoutOptIn).OrderBy(n => n, StringComparer.Ordinal),
            Is.EqualTo(LifecycleOptInTools.OrderBy(n => n, StringComparer.Ordinal)),
            "The opt-in is not a switch for the repair alone; when the set it contributes changes, the "
            + "decision to accept that surface on this container has to be retaken rather than inherited.");
    }

    /// <summary>
    /// The schema-control opt-in stays off, so no tool it would contribute is
    /// advertised. The seeded grant carries no <c>SchemaAdmin</c>, so every such tool
    /// could only ever be refused.
    /// </summary>
    [Test]
    public void The_schema_control_mutating_verbs_stay_unadvertised()
        => Assert.That(
            TreeAdminToolNames(),
            Has.None.StartsWith("lattice_treeadmin_schema_set")
                .And.None.StartsWith("lattice_treeadmin_schema_clear")
                .And.None.StartsWith("lattice_treeadmin_schema_advance")
                .And.None.StartsWith("lattice_treeadmin_schema_migrate")
                .And.None.EqualTo("lattice_treeadmin_schema_remediate"),
            "Advertising a call that can only be denied is worse than not advertising it.");
}
