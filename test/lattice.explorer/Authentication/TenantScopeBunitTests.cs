using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;
using Orleans.Lattice.Explorer.Tests.Tenancy;
using Orleans.Lattice.Explorer.UI.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

/// <summary>
/// The shell's tenant scope control: its shape follows how many tenants the
/// caller can actually reach, it never demands an id from memory, and every
/// outcome reaches a live region.
/// </summary>
/// <remarks>
/// A pure component test over stub seams - no cluster, host or channel - so it
/// carries no slow category. Assertions read the parsed DOM rather than raw
/// markup, and every branch is selected by explicit stub state rather than by
/// timing, ordering or a wall clock.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantScopeBunitTests : BunitContext
{
    private readonly ExplorerTenantContext _context = new();
    private readonly ExplorerTenantScopeNotices _notices = new();
    private readonly FakeUiPreferenceStore _store = new();

    /// <summary>
    /// Registers the tenancy seams the control resolves, with the caller's
    /// operator verdict and reachable tenants stated outright.
    /// </summary>
    private ExplorerShellPreferences Configure(
        bool isOperator,
        ExplorerTenantId? activeTenant,
        params string[] accessible)
    {
        JSInterop.Mode = JSRuntimeMode.Loose;

        _context.ActiveTenant = activeTenant;

        var gate = new StubOperatorGate(isOperator);
        var view = new ExplorerTenantView(_context, gate);
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        session.Username.Returns("ada");

        var preferences = new ExplorerShellPreferences(
            _store,
            new ExplorerPreferenceCatalog(),
            new FakeExplorerPreferenceScopeProvider());

        Services.AddSingleton(session);
        Services.AddSingleton<IExplorerTenantContext>(_context);
        Services.AddSingleton<IExplorerTenantView>(view);
        Services.AddSingleton<IExplorerTenantScopeNotices>(_notices);
        Services.AddSingleton<IExplorerShellPreferences>(preferences);
        Services.AddSingleton<IExplorerAccessibleTenantSource>(
            new FakeAccessibleTenantSource(accessible));
        Services.AddSingleton<IExplorerTenantSwitcher>(
            new ExplorerTenantSwitcher(view, _context, gate, null, preferences, _notices));

        // The control resolves the identity seam optionally; an already-established
        // scope is exactly what the resolver would leave, and leaving it out keeps
        // each branch stated by this fixture rather than derived.
        return preferences;
    }

    // --- The adaptive affordance ---

    [Test]
    public void With_only_the_default_tenant_and_no_operator_nothing_renders()
    {
        // A single-tenant cluster must look exactly like a non-tenant one.
        Configure(isOperator: false, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);

        var cut = Render<TenantScope>();

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void With_no_established_tenant_and_no_operator_nothing_renders()
    {
        Configure(isOperator: false, activeTenant: null);

        var cut = Render<TenantScope>();

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void With_one_reachable_non_default_tenant_a_quiet_display_replaces_the_picker()
    {
        Configure(isOperator: false, new ExplorerTenantId(SampleTenant.TenantId), SampleTenant.TenantId);

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("select"), Is.Empty, "there is nothing to choose between");
            Assert.That(cut.FindAll("input[type=text]"), Is.Empty, "never a free-text box");
            Assert.That(cut.Markup, Does.Contain(SampleTenant.TenantId));
            Assert.That(cut.Markup, Does.Contain(ExplorerVocabulary.ActiveTenantLabel));
        });
    }

    [Test]
    public void With_several_reachable_tenants_a_drop_down_lists_exactly_those_tenants()
    {
        Configure(
            isOperator: true,
            new ExplorerTenantId(SampleTenant.TenantId),
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId,
            SampleTenant.OtherTenantId);

        var cut = Render<TenantScope>();

        var options = cut.FindAll("option").Select(static node => node.GetAttribute("value")).ToArray();
        Assert.That(
            options,
            Is.EqualTo(new[] { ExplorerTenantId.Default.Value, SampleTenant.TenantId, SampleTenant.OtherTenantId }));
    }

    [Test]
    public void The_drop_down_marks_the_current_tenant()
    {
        Configure(
            isOperator: true,
            new ExplorerTenantId(SampleTenant.TenantId),
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId);

        var cut = Render<TenantScope>();

        var selected = cut.FindAll("option")
            .Where(static node => node.HasAttribute("selected"))
            .Select(static node => node.GetAttribute("value"))
            .ToArray();

        Assert.That(selected, Is.EqualTo(new[] { SampleTenant.TenantId }));
    }

    [Test]
    public void The_drop_down_is_never_a_free_text_box()
    {
        Configure(
            isOperator: true,
            new ExplorerTenantId(SampleTenant.TenantId),
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId);

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input[type=text]"), Is.Empty);
            Assert.That(cut.FindAll("select"), Is.Not.Empty);
        });
    }

    [Test]
    public void The_drop_down_carries_an_accessible_name_and_a_description()
    {
        Configure(
            isOperator: true,
            new ExplorerTenantId(SampleTenant.TenantId),
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId);

        var cut = Render<TenantScope>();
        var select = cut.Find("select");

        Assert.Multiple(() =>
        {
            var label = cut.Find("label[for]");
            Assert.That(label.GetAttribute("for"), Is.EqualTo(select.GetAttribute("id")));
            Assert.That(select.GetAttribute("aria-describedby"), Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public void A_non_operator_is_never_offered_the_picker_even_with_several_reachable_tenants()
    {
        // Fail-closed by construction: only an operator may switch, so only an
        // operator is offered a control that switches.
        Configure(
            isOperator: false,
            new ExplorerTenantId(SampleTenant.TenantId),
            SampleTenant.TenantId,
            SampleTenant.OtherTenantId);

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("select"), Is.Empty);
            Assert.That(cut.FindAll("input[type=checkbox]"), Is.Empty, "nor the all-tenant escape");
        });
    }

    // --- Vocabulary ---

    [Test]
    public void The_default_tenant_is_explained_in_product()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[title]"), Is.Empty, "a title attribute is not an explanation");
            Assert.That(cut.FindAll(".lx-help-panel"), Is.Not.Empty);
        });
    }

    [Test]
    public void All_tenants_is_explained_rather_than_left_bare()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Markup, Does.Contain(ExplorerVocabulary.AllTenantsLabel));
            Assert.That(
                cut.Markup,
                Does.Contain(ExplorerGlossary.Get(ExplorerTermIds.AllTenants).Explanation));
        });
    }

    [Test]
    public void The_active_tenant_term_is_explained_beside_the_control()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);

        var cut = Render<TenantScope>();

        Assert.That(
            cut.Markup,
            Does.Contain(ExplorerGlossary.Get(ExplorerTermIds.ActiveTenant).Explanation));
    }

    // --- Honest feedback in a live region ---

    [Test]
    public void Both_live_regions_exist_before_there_is_anything_to_announce()
    {
        // A live region announces a change to its text, so it has to be in the DOM
        // before the outcome arrives.
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("[role=status]"), Is.Not.Empty);
            Assert.That(cut.FindAll("[role=alert]"), Is.Not.Empty);
        });
    }

    [Test]
    public void Selecting_a_reachable_tenant_switches_the_scope_and_confirms_it()
    {
        Configure(
            isOperator: true,
            ExplorerTenantId.Default,
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId);
        var cut = Render<TenantScope>();

        cut.Find("select").Change(SampleTenant.TenantId);

        Assert.Multiple(() =>
        {
            Assert.That(_context.ActiveTenant, Is.EqualTo(new ExplorerTenantId(SampleTenant.TenantId)));
            Assert.That(cut.Find("[role=status]").TextContent, Does.Contain(SampleTenant.TenantId));
        });
    }

    [Test]
    public void Selecting_a_tenant_the_caller_cannot_reach_is_refused_and_explained()
    {
        Configure(
            isOperator: true,
            ExplorerTenantId.Default,
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId);
        var cut = Render<TenantScope>();

        cut.Find("select").Change("ghost-tenant");

        Assert.Multiple(() =>
        {
            Assert.That(
                _context.ActiveTenant,
                Is.EqualTo(ExplorerTenantId.Default),
                "never scope to a tenant the caller cannot reach");
            Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("ghost-tenant"));
        });
    }

    [Test]
    public void An_abandoned_remembered_tenant_is_announced_politely()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);
        _notices.Publish(
            ExplorerTenantScopeNotice.RestoreAbandoned("It was forgotten.", ExplorerTenantId.Default));

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=status]").TextContent, Does.Contain("It was forgotten."));
            Assert.That(cut.Find("[role=alert]").TextContent.Trim(), Is.Empty);
        });
    }

    [Test]
    public void A_refusal_is_announced_assertively()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);
        _notices.Publish(ExplorerTenantScopeNotice.Refused());

        var cut = Render<TenantScope>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=alert]").TextContent.Trim(), Is.Not.Empty);
            Assert.That(cut.Find("[role=status]").TextContent.Trim(), Is.Empty);
        });
    }

    // --- The all-tenant view ---

    [Test]
    public void Toggling_all_tenants_applies_it_and_confirms()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);
        var cut = Render<TenantScope>();

        cut.Find("input[type=checkbox]").Change(true);

        Assert.Multiple(() =>
        {
            Assert.That(_context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
            Assert.That(cut.Find("[role=status]").TextContent.Trim(), Is.Not.Empty);
        });
    }

    [Test]
    public void A_remembered_all_tenant_view_is_reapplied_for_an_operator()
    {
        var preferences = Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerPreferenceKeys.AllTenantsVisible, true).GetAwaiter().GetResult();

        Render<TenantScope>();

        Assert.That(_context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }

    [Test]
    public void A_remembered_all_tenant_view_is_not_announced_as_though_it_just_happened()
    {
        var preferences = Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerPreferenceKeys.AllTenantsVisible, true).GetAwaiter().GetResult();

        var cut = Render<TenantScope>();

        Assert.That(cut.Find("[role=status]").TextContent.Trim(), Is.Empty);
    }

    [Test]
    public void A_remembered_all_tenant_view_cannot_elevate_a_non_operator()
    {
        // The remembered value is a hint, never an authority: the seam refuses it.
        var preferences = Configure(isOperator: false, new ExplorerTenantId(SampleTenant.TenantId), SampleTenant.TenantId);
        preferences.EnsureLoadedAsync().GetAwaiter().GetResult();
        preferences.SetAsync(ExplorerPreferenceKeys.AllTenantsVisible, true).GetAwaiter().GetResult();

        Render<TenantScope>();

        Assert.That(_context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
    }

    // --- Placement and allocation ---

    [Test]
    public void The_class_parameter_is_appended_so_the_chrome_can_place_it()
    {
        Configure(isOperator: true, ExplorerTenantId.Default, ExplorerTenantId.Default.Value);

        var cut = Render<TenantScope>(parameters => parameters.Add(p => p.Class, "lx-placed"));

        Assert.That(cut.Find("div").GetAttribute("class"), Does.Contain("lx-placed"));
    }

    [Test]
    public void The_reachable_list_is_not_re_read_on_every_render()
    {
        // The source may reach the cluster, so it is read per refresh, never per
        // render pass.
        Configure(
            isOperator: true,
            ExplorerTenantId.Default,
            ExplorerTenantId.Default.Value,
            SampleTenant.TenantId);
        var source = (FakeAccessibleTenantSource)Services.GetRequiredService<IExplorerAccessibleTenantSource>();
        var cut = Render<TenantScope>();
        var afterFirstRender = source.Calls;

        cut.Render();
        cut.Render();

        Assert.That(source.Calls, Is.EqualTo(afterFirstRender));
    }

    [Test]
    public void An_inactive_tenant_view_renders_nothing_at_all()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        Services.AddSingleton(session);
        Services.AddSingleton<IExplorerTenantSwitcher>(new StubTenantSwitcher(isActive: false));

        var cut = Render<TenantScope>();

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void With_no_tenancy_registered_at_all_the_control_stays_inert()
    {
        JSInterop.Mode = JSRuntimeMode.Loose;
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(true);
        Services.AddSingleton(session);

        var cut = Render<TenantScope>();

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }
}
