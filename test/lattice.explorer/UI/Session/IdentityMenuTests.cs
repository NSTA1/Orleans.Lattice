using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Session;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The identity menu: Sign in when anonymous; the display name, tenant and
/// cluster when signed in; Reset view; and each shape of Sign out.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class IdentityMenuTests : SessionTestContext
{
    [Test]
    public void An_anonymous_caller_is_offered_sign_in_which_opens_the_session_overlay()
    {
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").TextContent, Is.EqualTo("Sign in"));
            Assert.That(State.Overlay, Is.EqualTo(SessionOverlayKind.SignIn));
        });
    }

    [Test]
    public void A_signed_in_caller_sees_their_display_name_as_a_dialog_trigger()
    {
        Auth.SignIn("alice@contoso.com");

        var cut = Render<IdentityMenu>();

        var trigger = cut.Find("button");
        Assert.Multiple(() =>
        {
            Assert.That(trigger.TextContent, Is.EqualTo("alice@contoso.com"));
            Assert.That(trigger.GetAttribute("aria-haspopup"), Is.EqualTo("dialog"));
            Assert.That(trigger.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(trigger.GetAttribute("aria-label"), Does.Contain("alice@contoso.com"), "the accessible name contains the visible label");
            Assert.That(cut.FindAll("[role=dialog]"), Is.Empty);
        });
    }

    [Test]
    public void Signing_in_elsewhere_re_renders_the_menu()
    {
        var cut = Render<IdentityMenu>();

        cut.InvokeAsync(() => Auth.SignIn("alice")).GetAwaiter().GetResult();

        Assert.That(cut.Find("button").TextContent, Is.EqualTo("alice"));
    }

    [Test]
    public void Opening_the_menu_announces_it_so_the_other_header_panels_close()
    {
        // Issue #3986: header panels close each other.
        Auth.SignIn("alice");
        var opened = new List<object>();
        Services.GetRequiredService<ShellHeaderPanels>().Opened += opened.Add;
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.That(opened, Is.EqualTo(new object[] { cut.Instance }));
    }

    [Test]
    public void Another_header_panel_opening_closes_the_session_details()
    {
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();
        cut.Find("button").Click();
        Assert.That(cut.FindAll("[role=dialog]"), Has.Count.EqualTo(1));

        cut.InvokeAsync(() => Services.GetRequiredService<ShellHeaderPanels>().Opening(new object()));

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=dialog]"), Is.Empty));
        Assert.That(cut.Find("button").GetAttribute("aria-expanded"), Is.EqualTo("false"));
    }

    [Test]
    public void Opening_the_menu_shows_the_identity_the_method_and_the_cluster()
    {
        Explorer.Configured(RemoteConfiguration());
        Auth.SignIn("alice", ExplorerAuthSchemes.Entra);
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(Definitions(cut), Is.EqualTo(new Dictionary<string, string>
            {
                ["Signed in as"] = "alice",
                ["Sign-in method"] = ExplorerAuthSchemes.Entra,
                ["Cluster"] = "https://cluster.example:443",
            }));
        });
    }

    [Test]
    public void With_tenancy_off_no_tenant_row_is_shown()
    {
        Services.AddSingleton(InactiveTenantView());
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.That(Definitions(cut).Keys, Has.No.Member("Tenant"));
    }

    [Test]
    public void With_tenancy_on_the_active_tenant_is_shown()
    {
        Services.AddSingleton(ActiveTenantView(new ExplorerTenantId("contoso")));
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.That(Definitions(cut)["Tenant"], Is.EqualTo("contoso"));
    }

    [Test]
    public void A_restricted_identity_with_no_tenant_is_told_it_sees_no_tenants_trees()
    {
        // Tenancy fails closed: with no active tenant the view reveals nothing,
        // and the menu must say so rather than imply the caller sees everything.
        Services.AddSingleton(ActiveTenantView(tenant: null));
        Auth.SignIn("guest");
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.That(Definitions(cut)["Tenant"], Does.StartWith("None established"));
    }

    [Test]
    public void Reset_view_links_to_the_reset_page_relative_to_the_document_base()
    {
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.That(cut.FindAll("a").Single(link => link.TextContent == "Reset view").GetAttribute("href"), Is.EqualTo("reset"));
    }

    [Test]
    public void The_web_head_signs_out_with_a_form_post_to_its_logout_path()
    {
        UseOptions(new SessionSignInOptions { LogoutPath = "explorer/auth/logout" });
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        var form = cut.Find("form");
        Assert.Multiple(() =>
        {
            Assert.That(form.GetAttribute("method"), Is.EqualTo("post"));
            Assert.That(form.GetAttribute("action"), Is.EqualTo("explorer/auth/logout"));
            Assert.That(form.QuerySelector($"input[name={FakeAntiforgeryStateProvider.FieldName}]"), Is.Not.Null);
            Assert.That(form.QuerySelector("button[type=submit]")?.TextContent, Is.EqualTo("Sign out"));
        });
    }

    [Test]
    public void A_federated_sign_out_endpoint_wins()
    {
        Services.AddSingleton(new ExplorerSignOutOptions { FederatedSignOutPath = "/explorer-entra/signout" });
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();

        cut.Find("button").Click();

        Assert.That(cut.Find("form").GetAttribute("action"), Is.EqualTo("/explorer-entra/signout"));
    }

    [Test]
    public void An_in_circuit_sign_out_signs_out_and_offers_sign_in_again()
    {
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();
        cut.Find("button").Click();

        cut.FindAll("button").Single(button => button.TextContent == "Sign out").Click();

        Assert.Multiple(() =>
        {
            Assert.That(Auth.SignOuts, Is.EqualTo(1));
            Assert.That(cut.FindAll("[role=dialog]"), Is.Empty);
            Assert.That(cut.Find("button").TextContent, Is.EqualTo("Sign in"));
        });
    }

    [Test]
    public void Closing_the_menu_hides_it()
    {
        Auth.SignIn("alice");
        var cut = Render<IdentityMenu>();
        cut.Find("button").Click();

        cut.Find("[role=dialog]").KeyDown("Escape");

        Assert.That(cut.FindAll("[role=dialog]"), Is.Empty);
    }

    [Test]
    public void Disposing_the_menu_unsubscribes_it()
    {
        var cut = Render<IdentityMenu>();
        var subscribed = Auth.AuthenticationSubscribers;

        cut.Instance.Dispose();

        Assert.That(Auth.AuthenticationSubscribers, Is.LessThan(subscribed));
    }

    private static Dictionary<string, string> Definitions(IRenderedComponent<IdentityMenu> cut) =>
        cut.FindAll(".lt-dl__row").ToDictionary(
            row => row.QuerySelector("dt")!.TextContent,
            row => row.QuerySelector("dd")!.TextContent);

    private static IExplorerTenantView InactiveTenantView()
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(false);
        return view;
    }

    private static IExplorerTenantView ActiveTenantView(ExplorerTenantId? tenant)
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns(tenant);
        return view;
    }
}
