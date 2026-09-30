using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;
using Orleans.Lattice.Explorer.Tests.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Issue #3999: the server prerender cannot read the tenant a caller last held (it is
/// remembered in browser storage), so it resolves the tenant the way the live circuit
/// will - from the address first - and otherwise renders a neutral state. It never
/// renders, links or redirects under the tenant it would have had to guess.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutPrerenderTenantTests : ShellLayoutTestContext
{
    private const string Remembered = "globex";

    private bool _operator = true;

    [Test]
    public void A_prerender_of_a_cluster_wide_page_renders_the_neutral_state_and_never_the_default_tenant()
    {
        Prerender();
        Navigation.NavigateTo("cluster");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("main").TextContent, Does.Contain(ShellLayout.TenantPendingLabel)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("#circuit-tenant"), Is.Empty, "no page renders under the guessed tenant");
            Assert.That(cut.Markup, Does.Not.Contain("t/default"), "nothing links to the guessed tenant");
            Assert.That(cut.FindAll("button[data-lt-command=\"tenant.switch\"]"), Is.Empty, "the switcher names no tenant");
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "cluster"));
        });
    }

    [Test]
    public void A_prerender_of_home_is_not_redirected_to_the_guessed_tenant()
    {
        Prerender();

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("main").TextContent, Does.Contain(ShellLayout.TenantPendingLabel)));
        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri), "a redirect to /t/default would root the circuit at the guess");
            Assert.That(cut.FindAll("#circuit-tenant"), Is.Empty);
        });
    }

    [Test]
    public void A_prerender_at_an_address_naming_a_tenant_renders_that_tenant()
    {
        Prerender();
        Navigation.NavigateTo("t/acme/data");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("#circuit-tenant").TextContent, Is.EqualTo("acme")));
        Assert.That(cut.Find("main").TextContent, Does.Not.Contain(ShellLayout.TenantPendingLabel));
    }

    [Test]
    public void A_prerender_at_a_tenant_the_caller_cannot_switch_to_is_not_redirected_to_the_guess()
    {
        // The switch to globex is refused, and the fallback it would redirect to is the
        // tenant the prerender guessed among the two this caller can reach.
        _operator = false;
        Prerender("acme", "initech");
        Navigation.NavigateTo("t/globex/data");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("main").TextContent, Does.Contain(ShellLayout.TenantPendingLabel)));
        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/globex/data"));
            Assert.That(cut.FindAll("#circuit-tenant"), Is.Empty);
            Assert.That(cut.Markup, Does.Not.Contain("t/acme"), "nothing links to the guessed tenant");
        });
    }

    [Test]
    public void A_prerender_still_drops_the_tenant_root_a_cluster_wide_address_never_carries()
    {
        Prerender();
        Navigation.NavigateTo("t/acme/cluster");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "cluster")));
    }

    [Test]
    public void A_prerender_for_a_caller_with_one_reachable_tenant_renders_that_tenant()
    {
        // The circuit can only ever hold the one tenant, so nothing is guessed.
        Prerender("acme");
        Navigation.NavigateTo("cluster");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("#circuit-tenant").TextContent, Is.EqualTo("acme")));
    }

    [Test]
    public async Task The_live_circuit_restores_the_remembered_tenant_before_its_first_page_renders()
    {
        var store = new FakeUiPreferenceStore { HydrateOnCall = 1 };
        Services.AddSingleton<IUiPreferenceStore>(store);
        Tenancy(ExplorerTenantId.Default.Value, "acme", Remembered);
        Auth.SignIn("alice");
        SetRendererInfo(new RendererInfo("Server", isInteractive: true));
        await Services.GetRequiredService<IExplorerShellPreferences>().SetAsync(ExplorerPreferenceKeys.ActiveTenant, Remembered);
        Navigation.NavigateTo("cluster");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("#circuit-tenant").TextContent, Is.EqualTo(Remembered)));
        Assert.That(store.IsLoaded, Is.True);
    }

    [Test]
    public void A_live_circuit_that_cannot_read_the_remembered_tenant_settles_on_the_fallback_rather_than_waiting()
    {
        Services.AddSingleton<IUiPreferenceBackingStore>(new UnreachableBackingStore());
        Tenancy(ExplorerTenantId.Default.Value, "acme", Remembered);
        Auth.SignIn("alice");
        SetRendererInfo(new RendererInfo("Server", isInteractive: true));
        Navigation.NavigateTo("cluster");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("#circuit-tenant").TextContent, Is.EqualTo(ExplorerTenantId.Default.Value)));
    }

    private FakeAuthSession Auth => (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();

    private static readonly RenderFragment Page = builder =>
    {
        builder.OpenComponent<CircuitTenantPage>(0);
        builder.CloseComponent();
    };

    /// <summary>
    /// A server prerender: the renderer is not interactive and the browser storage the
    /// remembered tenant lives in cannot be read.
    /// </summary>
    private void Prerender(params string[] reachable)
    {
        Services.AddSingleton<IUiPreferenceBackingStore>(new UnreachableBackingStore());
        Tenancy(reachable.Length == 0 ? [ExplorerTenantId.Default.Value, "acme", Remembered] : reachable);
        Auth.SignIn("alice");
        SetRendererInfo(new RendererInfo("Static", isInteractive: false));
    }

    /// <summary>Core's real session preferences and tenancy, for a caller who can reach <paramref name="reachable"/>.</summary>
    private void Tenancy(params string[] reachable)
    {
        Services.AddScoped<IExplorerAccessibleTenantSource>(_ => new FakeAccessibleTenantSource(reachable));
        Services.AddScoped<IExplorerTenantOperatorGate>(_ => new StubOperatorGate(_operator));
        Services.AddExplorerSession();
        Services.AddExplorerTenantView();
        AddArea(new FakeArea("data", "Data"));
        AddArea(new FakeArea("cluster", "Cluster") { IsTenantScoped = false });
    }

    /// <summary>Shows the tenant the circuit holds while the page renders.</summary>
    private sealed class CircuitTenantPage : ComponentBase
    {
        [Inject]
        internal IExplorerTenantContext Context { get; set; } = default!;

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddAttribute(1, "id", "circuit-tenant");
            builder.AddContent(2, Context.ActiveTenant?.Value);
            builder.CloseElement();
        }
    }
}
