using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Schema.Components;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Tests.Plugins;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// The compact reflow, asserted at render level on the three Schema concerns
/// that ship a <c>LatticeAdaptiveTable</c>: the policy's rule table, the
/// compliance breakdown by reason, and the strict-mode dead-letter queue
/// (issue #1782).
/// </summary>
/// <remarks>
/// Every render is driven by a scripted <see cref="StubSchemaDomain"/> supplied
/// up front, so no test here depends on a clock, an ordering, or a background
/// task.
/// </remarks>
[TestFixture]
public sealed class SchemaCompactReflowRenderTests
{
    private static readonly DateTimeOffset When = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    // ---- Policy: the rule table -------------------------------------------

    private const string PolicySurface = "SchemaPolicyTab";

    [Test]
    public async Task The_policy_rule_table_renders_a_table_at_expanded()
    {
        var html = await RenderPolicyAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, PolicySurface);
    }

    [Test]
    public async Task The_policy_rule_table_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderPolicyAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, PolicySurface);
    }

    [Test]
    public async Task The_policy_rule_table_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderPolicyAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderPolicyAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "#", PolicySurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Kind", PolicySurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Detail", PolicySurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "Encoding", PolicySurface);

            // The detail is the whole content of a rule: a card that kept the
            // kind and dropped this would say a rule exists without saying
            // what it enforces.
            AdaptiveReflowAssert.CardShowsField(compact, "Detail", "max 4096 bytes", PolicySurface);

            // The ordinal opts out of the card by declaration.
            AdaptiveReflowAssert.CardOmitsField(compact, "#", PolicySurface);
        });
    }

    [Test]
    public async Task The_policy_edit_control_survives_the_reflow()
    {
        var expanded = await RenderPolicyAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderPolicyAsync(LatticeBreakpoint.Compact);

        // The rule table is read-only, so the control a narrow-viewport
        // operator would be stranded without is the surface's own editor entry
        // point rather than a row action.
        AdaptiveReflowAssert.ControlSurvivesTheReflow(
            expanded, compact, ">Edit policy</button>", PolicySurface);
    }

    private static Task<string> RenderPolicyAsync(LatticeBreakpoint breakpoint)
    {
        var domain = new StubSchemaDomain
        {
            Policy = SchemaReadView<LatticeSchemaPolicy>.Succeeded(
                new LatticeSchemaPolicy([LatticeSchemaRule.MaxLength(4096)], strictIngest: true)),
        };

        return SchemaRenderHarness.RenderAsync<SchemaPolicyTab>(
            SchemaRenderHarness.Session(domain),
            breakpoint);
    }

    // ---- Compliance: the breakdown by reason -------------------------------

    private const string ComplianceSurface = "SchemaComplianceSection";

    [Test]
    public async Task The_compliance_breakdown_renders_a_table_at_expanded()
    {
        var html = await RenderComplianceAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, ComplianceSurface);
    }

    [Test]
    public async Task The_compliance_breakdown_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderComplianceAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, ComplianceSurface);
    }

    [Test]
    public async Task The_compliance_breakdown_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderComplianceAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderComplianceAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Reason", ComplianceSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Count", ComplianceSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "max byte length", ComplianceSurface);

            // A breakdown that kept the reasons and lost the counts would say
            // which rules failed without saying how badly.
            AdaptiveReflowAssert.CardShowsField(compact, "Count", "3", ComplianceSurface);
        });
    }

    [Test]
    public async Task The_compliance_scan_control_survives_the_reflow()
    {
        var expanded = await RenderComplianceAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderComplianceAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ControlSurvivesTheReflow(
            expanded, compact, ">Scan compliance</button>", ComplianceSurface);
    }

    private static Task<string> RenderComplianceAsync(LatticeBreakpoint breakpoint)
    {
        var domain = new StubSchemaDomain
        {
            Compliance = SchemaReadView<LatticeSchemaComplianceReport>.Succeeded(
                new LatticeSchemaComplianceReport
                {
                    TreeId = SchemaRenderHarness.TreeId,
                    HasPolicy = true,
                    CompliantCount = 17,
                    NonCompliantCount = 3,
                    ScannedCount = 20,
                    RuleBreakdown =
                    [
                        new LatticeSchemaComplianceRuleCount { Reason = "max byte length", Count = 3 },
                    ],
                }),
        };

        // The audit is loaded on an explicit action, so the state under test is
        // only reachable by running it.
        return SchemaRenderHarness.RenderAsync<SchemaComplianceSection>(
            SchemaRenderHarness.Session(domain),
            breakpoint,
            afterFirstRender: section => section.ScanAsync());
    }

    // ---- Dead letters: the strict-mode ingest queue -------------------------

    private const string DeadLettersSurface = "SchemaDeadLettersTab";

    [Test]
    public async Task The_schema_dead_letter_queue_renders_a_table_at_expanded()
    {
        var html = await RenderDeadLettersAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, DeadLettersSurface);
    }

    [Test]
    public async Task The_schema_dead_letter_queue_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderDeadLettersAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, DeadLettersSurface);
    }

    [Test]
    public async Task The_schema_dead_letter_queue_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderDeadLettersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderDeadLettersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Key", DeadLettersSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Reason", DeadLettersSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Source", DeadLettersSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Bytes", DeadLettersSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "When (UTC)", DeadLettersSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "orders/7", DeadLettersSurface);
            AdaptiveReflowAssert.CardShowsField(
                compact, "Reason", "max byte length", DeadLettersSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Source", "Replication", DeadLettersSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Bytes", "9001", DeadLettersSurface);
            AdaptiveReflowAssert.CardShowsField(
                compact, "When (UTC)", "2026-03-04 05:06:07Z", DeadLettersSurface);
        });
    }

    [Test]
    public async Task The_schema_dead_letter_load_control_survives_the_reflow()
    {
        var expanded = await RenderDeadLettersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderDeadLettersAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ControlSurvivesTheReflow(
            expanded, compact, ">Load dead letters</button>", DeadLettersSurface);
    }

    private static Task<string> RenderDeadLettersAsync(LatticeBreakpoint breakpoint)
    {
        var view = new SchemaDeadLetterView
        {
            Status = SchemaOperationStatus.Succeeded,
            Count = 1,
            Entries =
            [
                new LatticeSchemaDeadLetterEntry(
                    "orders/7",
                    [1, 2, 3],
                    9001,
                    "max byte length",
                    LatticeSchemaDeadLetterSource.Replication,
                    When),
            ],
        };

        var session = SchemaRenderHarness.Session(new StubSchemaDomain { DeadLetters = view });

        // The queue survives a visit to another concern by living on the
        // session, which is also what lets a test start from a loaded page.
        session.DeadLetters = new SchemaDeadLetterPage(SchemaRenderHarness.TreeId, view);

        return SchemaRenderHarness.RenderAsync<SchemaDeadLettersTab>(session, breakpoint);
    }
}
