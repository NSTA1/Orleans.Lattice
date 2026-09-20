using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Regression guard for the orphaned-leaf operator path. The core library shipped
/// <c>InspectOrphanedLeavesAsync</c> and <c>RepairOrphanedLeavesAsync</c> with no
/// caller anywhere outside its own package, so the documented remedy for a wedged
/// tree could not actually be invoked on a running deployment. These tests pin the
/// two tree-administration tools that make it reachable: that the audit is always
/// registered, read-only and needs no opt-in, so it cannot silently regress to
/// unreachable again; and that the repair is absent until the lifecycle opt-in is
/// set and is annotated destructive when it is. Deterministic - no cluster, no
/// invocation.
/// </summary>
[TestFixture]
public sealed class TreeAdminOrphanedLeafToolTests
{
    private const string AuditToolName = "lattice_treeadmin_orphaned_leaves_audit";
    private const string RepairToolName = "lattice_treeadmin_orphaned_leaves_repair";

    private static TreeAdminToolGroup CreateGroup(bool enableLifecycle)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeSchemaControl>());
        services.AddSingleton(Substitute.For<ILatticeTreeAdmin>());
        var provider = services.BuildServiceProvider();
        var options = Options.Create(
            new LatticeApiMcpOptions
            {
                EnableTreeAdminSchemaControlTools = false,
                EnableTreeAdminLifecycleTools = enableLifecycle,
            });
        return new TreeAdminToolGroup(provider, options);
    }

    private static McpServerTool Tool(TreeAdminToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Audit_tool_is_registered_without_any_opt_in()
    {
        var group = CreateGroup(enableLifecycle: false);

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Does.Contain(AuditToolName),
            "The orphaned-leaf audit is the documented first step before the repair, so it must be "
            + "freely callable and must not sit behind the lifecycle opt-in.");
    }

    [Test]
    public void Audit_tool_is_annotated_read_only_and_non_destructive()
    {
        var group = CreateGroup(enableLifecycle: false);

        var annotations = Tool(group, AuditToolName).ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations?.ReadOnlyHint, Is.True, "The audit changes nothing and must advertise that.");
            Assert.That(annotations?.DestructiveHint, Is.False, "The audit must not be advertised as destructive.");
        });
    }

    [Test]
    public void Audit_tool_takes_a_tree_selector_in_its_input_schema()
    {
        var group = CreateGroup(enableLifecycle: false);

        var schema = Tool(group, AuditToolName).ProtocolTool.InputSchema;

        Assert.Multiple(() =>
        {
            Assert.That(schema.ValueKind, Is.EqualTo(JsonValueKind.Object));
            Assert.That(
                schema.TryGetProperty("properties", out var properties) && properties.TryGetProperty("treeId", out _),
                Is.True,
                "The facade resolves per tree, so the caller must be able to name one.");
            Assert.That(
                schema.TryGetProperty("properties", out var props) && props.TryGetProperty("treeAdmin", out _),
                Is.False,
                "The facade itself is bound from services and must never appear in the input schema.");
        });
    }

    [Test]
    public void Repair_tool_is_absent_until_the_lifecycle_opt_in()
    {
        var group = CreateGroup(enableLifecycle: false);

        Assert.That(
            group.Tools.Select(t => t.ProtocolTool.Name),
            Does.Not.Contain(RepairToolName),
            "The repair is an irreversible structural change and must be offered only when the host opted writes in.");
    }

    [Test]
    public void Repair_tool_appears_when_the_lifecycle_opt_in_is_enabled()
    {
        var group = CreateGroup(enableLifecycle: true);

        Assert.That(group.Tools.Select(t => t.ProtocolTool.Name), Does.Contain(RepairToolName));
    }

    [Test]
    public void Repair_tool_is_annotated_destructive()
    {
        var group = CreateGroup(enableLifecycle: true);

        var annotations = Tool(group, RepairToolName).ProtocolTool.Annotations;

        Assert.Multiple(() =>
        {
            Assert.That(annotations?.DestructiveHint, Is.True);
            Assert.That(annotations?.ReadOnlyHint, Is.False);
        });
    }

    [Test]
    public void The_two_verbs_stay_separate_rather_than_collapsing_into_a_dry_run_flag()
    {
        var group = CreateGroup(enableLifecycle: true);

        var auditSchema = Tool(group, AuditToolName).ProtocolTool.InputSchema;
        var repairSchema = Tool(group, RepairToolName).ProtocolTool.InputSchema;

        Assert.Multiple(() =>
        {
            Assert.That(
                auditSchema.TryGetProperty("properties", out var auditProps) && auditProps.TryGetProperty("dryRun", out _),
                Is.False,
                "A dryRun flag defaulting the wrong way on a fail-closed repair is exactly what the two-verb split avoids.");
            Assert.That(
                repairSchema.TryGetProperty("properties", out var repairProps) && repairProps.TryGetProperty("dryRun", out _),
                Is.False,
                "A dryRun flag defaulting the wrong way on a fail-closed repair is exactly what the two-verb split avoids.");
        });
    }
}
