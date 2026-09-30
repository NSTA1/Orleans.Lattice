using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Blazor reuses a page component when only its route parameters change, so a
/// page that remembers what it read in its own fields would carry one tenant's
/// answers to another tenant's address. The layout keys the page on the caller -
/// the sign-in, the endpoint and the asserted tenant - so a tenant switch or a
/// sign-in as someone else builds a fresh page instead.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutTenantPageReuseTests : ShellLayoutTestContext
{
    [Test]
    public void A_tenant_switch_builds_a_fresh_page_and_shows_nothing_read_under_the_previous_tenant()
    {
        UseTenancy("acme", allowSwitch: true, "acme", "globex");
        AddArea(new FakeArea("data", "Data"));
        Services.AddSingleton<ILatticeActiveTenantProvider>(provider => new ViewTenant(provider.GetRequiredService<IExplorerTenantView>()));
        ((FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>()).SignIn("alice");
        TenantAnswerPage.Created = 0;
        Navigation.NavigateTo("t/acme/data");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));
        cut.WaitUntil(() => Assert.That(cut.Find("#tenant-answer").TextContent, Is.EqualTo("acme")));
        var first = cut.Find("#tenant-answer").GetAttribute("data-instance");

        Navigation.NavigateTo("t/globex/data");
        cut.Render(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("#tenant-answer").TextContent, Is.EqualTo("globex"), "nothing read under acme is shown at globex's address");
            Assert.That(cut.Find("#tenant-answer").GetAttribute("data-instance"), Is.Not.EqualTo(first), "the page is a fresh instance");
        });
    }

    [Test]
    public void An_unchanged_tenant_keeps_the_same_page()
    {
        UseTenancy("acme");
        AddArea(new FakeArea("data", "Data"));
        Services.AddSingleton<ILatticeActiveTenantProvider>(provider => new ViewTenant(provider.GetRequiredService<IExplorerTenantView>()));
        TenantAnswerPage.Created = 0;
        Navigation.NavigateTo("t/acme/data/orders");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, Page));
        cut.WaitUntil(() => Assert.That(cut.FindAll("#tenant-answer"), Has.Count.EqualTo(1)));
        var first = cut.Find("#tenant-answer").GetAttribute("data-instance");

        Navigation.NavigateTo("t/acme/data/audit");
        cut.Render(parameters => parameters.Add(layout => layout.Body, Page));

        cut.WaitUntil(() => Assert.That(cut.Find("#tenant-answer").GetAttribute("data-instance"), Is.EqualTo(first)));
    }

    [Test]
    public void A_sign_in_as_someone_else_builds_a_fresh_page_and_shows_nothing_read_for_the_previous_caller()
    {
        UseTenancy("acme");
        AddArea(new FakeArea("data", "Data"));
        Services.AddSingleton<ILatticeActiveTenantProvider>(provider => new ViewTenant(provider.GetRequiredService<IExplorerTenantView>()));
        var auth = (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();
        auth.SignIn("alice");
        IdentityAnswerPage.Created = 0;
        Navigation.NavigateTo("t/acme/data");

        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, IdentityPage));
        cut.WaitUntil(() => Assert.That(cut.Find("#identity-answer").TextContent, Is.EqualTo("alice")));
        var first = cut.Find("#identity-answer").GetAttribute("data-instance");

        // Same tenant, same address: only the sign-in changes, inside the circuit.
        cut.InvokeAsync(() => auth.SignIn("bob"));
        cut.Render(parameters => parameters.Add(layout => layout.Body, IdentityPage));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("#identity-answer").TextContent, Is.EqualTo("bob"), "nothing read for alice is shown to bob");
            Assert.That(cut.Find("#identity-answer").GetAttribute("data-instance"), Is.Not.EqualTo(first), "the page is a fresh instance");
        });
    }

    private static readonly RenderFragment IdentityPage = builder =>
    {
        builder.OpenComponent<IdentityAnswerPage>(0);
        builder.CloseComponent();
    };

    private static readonly RenderFragment Page = builder =>
    {
        builder.OpenComponent<TenantAnswerPage>(0);
        builder.CloseComponent();
    };

    /// <summary>Reads who is signed in once, as a page that remembers what it read for its caller does.</summary>
    private sealed class IdentityAnswerPage : ComponentBase
    {
        private string? _answer;
        private int _instance;

        public static int Created { get; set; }

        [Inject]
        internal IExplorerAuthSession Auth { get; set; } = default!;

        protected override void OnInitialized()
        {
            _instance = ++Created;
            _answer = Auth.Username;
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddAttribute(1, "id", "identity-answer");
            builder.AddAttribute(2, "data-instance", _instance);
            builder.AddContent(3, _answer);
            builder.CloseElement();
        }
    }

    /// <summary>Reads the tenant the circuit asserts once, as a page that remembers its answer does.</summary>
    private sealed class TenantAnswerPage : ComponentBase
    {
        private string? _answer;
        private int _instance;

        public static int Created { get; set; }

        [Inject]
        internal ShellAssertedTenant Tenant { get; set; } = default!;

        protected override void OnInitialized()
        {
            _instance = ++Created;
            _answer = Tenant.AssertedTenant;
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddAttribute(1, "id", "tenant-answer");
            builder.AddAttribute(2, "data-instance", _instance);
            builder.AddContent(3, _answer);
            builder.CloseElement();
        }
    }

    /// <summary>The asserted tenant, read from the substituted tenant view the chrome context scopes.</summary>
    private sealed class ViewTenant(IExplorerTenantView view) : ILatticeActiveTenantProvider
    {
        public string? AssertedTenant => view.ActiveTenant?.Value;
    }
}
