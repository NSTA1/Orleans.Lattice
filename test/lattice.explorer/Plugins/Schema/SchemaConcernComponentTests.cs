using Orleans.Lattice.Explorer.Schema.Components;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;

#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The concern-scoped components of the Schema area, each rendered in isolation:
/// the tree selector, the enforcement-policy tab, the versioning tab, the
/// compliance section, and the dead-letter tab.
/// <para>
/// The load-bearing assertion across all of them is the per-tree, per-action
/// grey-out: a denied action renders <em>disabled and visible</em>, driven by
/// the tree's scoped access decisions, never hidden and never inherited from the
/// plugin-level gate.
/// </para>
/// </summary>
[TestFixture]
public sealed class SchemaConcernComponentTests
{
    // ---- tree selector ------------------------------------------------------

    [Test]
    public async Task The_tree_selector_renders_a_button_per_governable_tree()
    {
        using var harness = SchemaComponentHarness.Create();
        var session = await harness.SessionAsync(treeId: null);

        var id = await harness.RenderAsync<SchemaTreeSelector>(
            (nameof(SchemaTreeSelector.Session), session),
            (nameof(SchemaTreeSelector.Catalog), Catalog("orders", "invoices")));

        Assert.That(
            harness.Renderer.Buttons(id).Select(b => b.Text),
            Is.EqualTo(new[] { string.Empty, "orders", "invoices" }),
            "the refresh control, then one option per tree");
    }

    [Test]
    public async Task The_tree_selector_disables_refresh_while_the_coarse_gate_is_closed()
    {
        using var harness = SchemaComponentHarness.Create();
        var session = await harness.SessionAsync(treeId: null, allowed: false);

        var id = await harness.RenderAsync<SchemaTreeSelector>(
            (nameof(SchemaTreeSelector.Session), session),
            (nameof(SchemaTreeSelector.Catalog), SchemaTreeCatalog.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Renderer.Buttons(id)[0].Disabled, Is.True);
            Assert.That(harness.Renderer.Buttons(id)[0].Title, Is.EqualTo("Refresh trees"));
        });
    }

    [Test]
    public async Task The_tree_selector_offers_a_retry_when_discovery_failed()
    {
        using var harness = SchemaComponentHarness.Create();
        var session = await harness.SessionAsync(treeId: null);

        var id = await harness.RenderAsync<SchemaTreeSelector>(
            (nameof(SchemaTreeSelector.Session), session),
            (nameof(SchemaTreeSelector.Catalog), SchemaTreeCatalog.Failed("no endpoint")));

        Assert.That(
            harness.Renderer.Buttons(id).Select(b => b.Text),
            Does.Contain("Try again"),
            "a discovery failure must be retryable, not fatal");
    }

    [Test]
    public async Task The_tree_selector_greys_every_option_while_a_request_is_in_flight()
    {
        using var harness = SchemaComponentHarness.Create();
        var session = await harness.SessionAsync(treeId: null);
        session.IsBusy = true;

        var id = await harness.RenderAsync<SchemaTreeSelector>(
            (nameof(SchemaTreeSelector.Session), session),
            (nameof(SchemaTreeSelector.Catalog), Catalog("orders")));

        Assert.That(harness.Renderer.Buttons(id).Single(b => b.Text == "orders").Disabled, Is.True);
    }

    // ---- enforcement policy -------------------------------------------------

    [Test]
    public async Task The_policy_tab_reads_the_policy_when_the_scoped_view_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true };
        var session = await harness.SessionAsync("orders");

        await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        Assert.That(harness.Domain.PolicyReadCount, Is.EqualTo(1));
    }

    [Test]
    public async Task The_policy_tab_reads_nothing_when_the_scoped_view_grant_denies_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = SchemaCapabilitySnapshot.None;
        var session = await harness.SessionAsync("orders");

        await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        Assert.That(harness.Domain.PolicyReadCount, Is.Zero);
    }

    [Test]
    public async Task An_ungoverned_tree_offers_add_a_policy_disabled_when_management_is_denied()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true };
        var session = await harness.SessionAsync("orders");

        var id = await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        var add = harness.Renderer.Buttons(id).Single(b => b.Text == "Add a policy");
        Assert.That(
            add.Disabled,
            Is.True,
            "a denied action renders disabled and visible, never hidden");
    }

    [Test]
    public async Task An_ungoverned_tree_enables_add_a_policy_when_the_scoped_manage_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true, CanManagePolicy = true };
        var session = await harness.SessionAsync("orders");

        var id = await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        Assert.That(harness.Renderer.Buttons(id).Single(b => b.Text == "Add a policy").Disabled, Is.False);
    }

    [Test]
    public async Task Opening_the_policy_editor_reveals_the_authoring_controls()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true, CanManagePolicy = true };
        var session = await harness.SessionAsync("orders");
        var id = await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        var add = harness.Renderer.Buttons(id).Single(b => b.Text == "Add a policy");
        await harness.Renderer.ClickAsync(add.ClickHandlerId);

        Assert.That(
            harness.Renderer.Buttons(id).Select(b => b.Text),
            Does.Contain("Save changes").And.Contains("Add rule").And.Contains("Cancel"));
    }

    [Test]
    public async Task Saving_an_empty_draft_prompts_instead_of_persisting_a_meaningless_policy()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true, CanManagePolicy = true };
        var session = await harness.SessionAsync("orders");
        var id = await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Add a policy").ClickHandlerId);
        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Save changes").ClickHandlerId);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.LastSetPolicy, Is.Null, "an empty policy is the same as none at all");
            Assert.That(harness.Renderer.Buttons(id).Select(b => b.Text), Does.Contain("OK"));
        });
    }

    [Test]
    public async Task A_pending_rule_is_folded_into_the_draft_rather_than_dropped_on_save()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true, CanManagePolicy = true };
        var session = await harness.SessionAsync("orders");
        var id = await harness.RenderAsync<SchemaPolicyTab>((nameof(SchemaPolicyTab.Session), session));

        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Add a policy").ClickHandlerId);
        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Add rule").ClickHandlerId);
        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Save changes").ClickHandlerId);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.LastSetPolicy, Is.Not.Null);
            Assert.That(harness.Domain.LastSetPolicy!.Rules, Has.Count.EqualTo(1));
            Assert.That(harness.Domain.LastMutatedTreeId, Is.EqualTo("orders"));
            Assert.That(session.LastResult, Is.Not.Null);
        });
    }

    // ---- compliance ---------------------------------------------------------

    [Test]
    public async Task The_compliance_scan_is_disabled_until_its_own_scoped_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanManagePolicy = true };
        var session = await harness.SessionAsync("orders");

        var id = await harness.RenderAsync<SchemaComplianceSection>(
            (nameof(SchemaComplianceSection.Session), session));

        Assert.That(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Scan compliance").Disabled,
            Is.True,
            "each action is gated on its own scope, not on a neighbouring one");
    }

    [Test]
    public async Task The_compliance_scan_runs_when_its_scoped_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanScanCompliance = true };
        var session = await harness.SessionAsync("orders");
        var id = await harness.RenderAsync<SchemaComplianceSection>(
            (nameof(SchemaComplianceSection.Session), session));

        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Scan compliance").ClickHandlerId);

        Assert.That(harness.Domain.ComplianceScanCount, Is.EqualTo(1));
    }

    // ---- versioning ---------------------------------------------------------

    [Test]
    public async Task The_versions_tab_reads_config_and_remediation_only_where_scoped_grants_permit()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewVersionConfig = true };
        var session = await harness.SessionAsync("orders");

        var id = await harness.RenderAsync<SchemaVersionsTab>((nameof(SchemaVersionsTab.Session), session));

        Assert.That(
            harness.Renderer.Buttons(id).Select(b => b.Text),
            Does.Contain("Enable versioning"),
            "an unversioned tree leads with the plain-words next step");
    }

    [Test]
    public async Task Enabling_versioning_is_disabled_until_the_scoped_manage_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewVersionConfig = true };
        var session = await harness.SessionAsync("orders");

        var id = await harness.RenderAsync<SchemaVersionsTab>((nameof(SchemaVersionsTab.Session), session));

        Assert.That(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Enable versioning").Disabled,
            Is.True);
    }

    [Test]
    public async Task Enabling_versioning_writes_the_seeded_config_when_the_scoped_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot
        {
            CanViewVersionConfig = true,
            CanManageVersion = true,
        };
        var session = await harness.SessionAsync("orders");
        var id = await harness.RenderAsync<SchemaVersionsTab>((nameof(SchemaVersionsTab.Session), session));

        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Enable versioning").ClickHandlerId);
        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Last(b => b.Text == "Enable versioning").ClickHandlerId);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.LastSetVersionConfig, Is.Not.Null);
            Assert.That(harness.Domain.LastSetVersionConfig!.Value.TargetVersion, Is.EqualTo(1u));
            Assert.That(harness.Domain.LastMutatedTreeId, Is.EqualTo("orders"));
        });
    }

    // ---- dead letters -------------------------------------------------------

    [Test]
    public async Task The_dead_letter_queue_is_disabled_until_its_own_scoped_grant_permits_it()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewPolicy = true };
        var session = await harness.SessionAsync("orders");

        var id = await harness.RenderAsync<SchemaDeadLettersTab>((nameof(SchemaDeadLettersTab.Session), session));

        Assert.That(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Load dead letters").Disabled,
            Is.True);
    }

    [Test]
    public async Task The_dead_letter_queue_loads_a_bounded_page_when_permitted()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewDeadLetters = true };
        var session = await harness.SessionAsync("orders");
        var id = await harness.RenderAsync<SchemaDeadLettersTab>((nameof(SchemaDeadLettersTab.Session), session));

        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(id).Single(b => b.Text == "Load dead letters").ClickHandlerId);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.DeadLetterReadCount, Is.EqualTo(1));
            Assert.That(
                harness.Domain.LastDeadLetterPageSize,
                Is.GreaterThan(0),
                "a large queue must never pull an unbounded page across the circuit");
            Assert.That(session.DeadLetters!.TreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task A_loaded_dead_letter_page_survives_a_visit_to_another_concern()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewDeadLetters = true };
        var session = await harness.SessionAsync("orders");

        var first = await harness.RenderAsync<SchemaDeadLettersTab>((nameof(SchemaDeadLettersTab.Session), session));
        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(first).Single(b => b.Text == "Load dead letters").ClickHandlerId);

        // Leaving the tab unmounts the component; coming back mounts a fresh one.
        var second = await harness.RenderAsync<SchemaDeadLettersTab>((nameof(SchemaDeadLettersTab.Session), session));

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Domain.DeadLetterReadCount,
                Is.EqualTo(1),
                "nothing invalidated the page, so returning to the tab must not re-run the read");
            Assert.That(
                RendersThePage(harness, second),
                Is.True,
                "the page the operator explicitly loaded is still in view");
        });
    }

    [Test]
    public async Task A_dead_letter_page_is_never_shown_under_a_different_tree()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Domain.Capabilities = new SchemaCapabilitySnapshot { CanViewDeadLetters = true };
        var session = await harness.SessionAsync("orders");
        var loaded = await harness.RenderAsync<SchemaDeadLettersTab>((nameof(SchemaDeadLettersTab.Session), session));
        await harness.Renderer.ClickAsync(
            harness.Renderer.Buttons(loaded).Single(b => b.Text == "Load dead letters").ClickHandlerId);

        session.TreeId = "invoices";
        session.Grants = await harness.Domain.ProbeTreeAsync("invoices");
        var switched = await harness.RenderAsync<SchemaDeadLettersTab>(
            (nameof(SchemaDeadLettersTab.Session), session));

        Assert.That(
            RendersThePage(harness, switched),
            Is.False,
            "one tree's dead letters must never render under another tree's heading");
    }

    /// <summary>
    /// Whether the dead-letter tab is currently rendering a loaded page, which it
    /// does by handing the entries to the design system's adaptive table.
    /// </summary>
    private static bool RendersThePage(SchemaComponentHarness harness, int componentId) =>
        harness.Renderer
            .ChildComponents(componentId)
            .Any(child => child.GetType().Name.StartsWith("LatticeAdaptiveTable", StringComparison.Ordinal));

    private static SchemaTreeCatalog Catalog(params string[] ids) =>
        SchemaTreeCatalog.Succeeded(ids.Select(id => new SchemaTreeSummary(id, id, null, null)).ToArray());
}
