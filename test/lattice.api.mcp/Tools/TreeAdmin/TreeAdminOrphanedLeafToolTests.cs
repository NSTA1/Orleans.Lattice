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

    /// <summary>
    /// Both verbs return one bounded batch (issue 3302), so the resume token has
    /// to be offerable by the caller. A tool that reports a resume position but
    /// does not accept one back strands every pass on its first batch, and an
    /// operator has no way to finish the repair at all.
    /// </summary>
    [Test]
    public void Both_tools_accept_a_resume_token_so_a_bounded_pass_can_be_driven_to_completion()
    {
        var group = CreateGroup(enableLifecycle: true);

        Assert.Multiple(() =>
        {
            foreach (var name in new[] { AuditToolName, RepairToolName })
            {
                var schema = Tool(group, name).ProtocolTool.InputSchema;
                Assert.That(
                    schema.TryGetProperty("properties", out var props) && props.TryGetProperty("resumeFrom", out _),
                    Is.True,
                    $"{name} must accept the previous batch's resume token.");
                Assert.That(
                    schema.TryGetProperty("required", out var required)
                        && required.EnumerateArray().Any(e => e.GetString() == "resumeFrom"),
                    Is.False,
                    $"{name} must be callable without a token to start a new pass.");
            }
        });
    }

    /// <summary>
    /// <b>Requirement 3 of issue 3302.</b> A timeout on the repair is not
    /// distinguishable at the call site from a failure, and in the field the
    /// grain went on to complete all 236 repairs after the client gave up. The
    /// obvious operator response - retry - starts a second pass over a chain the
    /// first may still be mutating. The description is the only place an agent or
    /// operator learns otherwise, so it must state the safe loop and must warn
    /// that the return value is not authoritative when a timeout is seen.
    /// <para>
    /// Asserted on the description text rather than on prose in a doc comment
    /// because the description is what actually reaches the caller: a doc comment
    /// deleted here changes nothing the operator sees, and this line going red is
    /// the only signal that the warning has gone.
    /// </para>
    /// </summary>
    [Test]
    public void Repair_tool_description_states_the_safe_loop_and_warns_the_return_is_not_authoritative()
    {
        var group = CreateGroup(enableLifecycle: true);

        var description = Tool(group, RepairToolName).ProtocolTool.Description ?? string.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain("re-audit").IgnoreCase,
                "the safe loop ends in a re-audit, and that is the step an operator skips");
            Assert.That(description, Does.Contain("timeout").IgnoreCase,
                "the failure mode has to be named to be recognised");
            Assert.That(description, Does.Contain("not authoritative").IgnoreCase,
                "the return value being untrustworthy after a timeout is the whole warning");
            Assert.That(description, Does.Contain("complete=").IgnoreCase,
                "an operator must be told which field says the pass finished");
            Assert.That(description, Does.Contain("resume_from").Or.Contain("resumeFrom"),
                "a bounded pass is undrivable unless the token is named");
        });
    }

    /// <summary>
    /// The audit's completeness flag carries a second, easily-missed claim: until
    /// the pass is complete, an empty finding list is not the clean bill of health
    /// the description otherwise promises. Saying only the first half would leave
    /// an operator reading a partial batch as a cleared tree.
    /// <para>
    /// The qualifier itself is asserted, not merely the words around it. An
    /// earlier revision of this test checked only that "clean bill of health"
    /// and "complete=true" both appeared somewhere in the description, and a
    /// perturbation that deleted the qualifying clause outright - leaving a flat
    /// "an empty finding list is a clean bill of health" - kept both phrases and
    /// passed. The assertion has to bind them together or it is satisfied by the
    /// exact sentence it exists to forbid.
    /// </para>
    /// </summary>
    [Test]
    public void Audit_tool_description_qualifies_the_clean_verdict_by_completeness()
    {
        var group = CreateGroup(enableLifecycle: false);

        var description = Tool(group, AuditToolName).ProtocolTool.Description ?? string.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain("bounded batch").IgnoreCase);
            Assert.That(
                description,
                Does.Match(@"clean bill of health[^.]*ONLY when complete=true").IgnoreCase,
                "the clean verdict must be conditioned on completeness in the same sentence that offers it");
            Assert.That(
                description,
                Does.Contain("partial batch").IgnoreCase,
                "an operator must be told what a partial batch's empty finding list does mean, not only what it does not");
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
