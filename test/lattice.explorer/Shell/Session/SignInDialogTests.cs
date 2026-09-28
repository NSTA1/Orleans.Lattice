using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The sign-in dialog: advertised-scheme discovery, every registered method the
/// endpoint accepts rendered through Core's seam, the password form's two shapes,
/// and the unsupported-scheme explanation.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SignInDialogTests : SessionTestContext
{
    [Test]
    public void An_endpoint_that_advertises_nothing_is_offered_the_password_form()
    {
        Explorer.Configured(RemoteConfiguration());

        var cut = Render<SignInDialog>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("input[name=username]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("input[type=password][name=password]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("h3"), Is.Empty, "a single method needs no heading");
            Assert.That(cut.Find(".lt-dialog__description").TextContent, Does.Contain("https://cluster.example:443"));
        });
    }

    [Test]
    public void A_discovery_failure_falls_back_to_the_password_form()
    {
        Auth.DiscoveryFailure = new InvalidOperationException("unreachable");

        var cut = Render<SignInDialog>();

        Assert.That(cut.FindAll("input[type=password]"), Has.Count.EqualTo(1));
    }

    [Test]
    public void The_web_head_posts_the_password_form_to_the_server_with_an_antiforgery_token()
    {
        UseOptions(new SessionSignInOptions { LoginPath = "explorer/auth/login" });

        var cut = Render<SignInDialog>();

        var form = cut.Find("form");
        Assert.Multiple(() =>
        {
            Assert.That(form.GetAttribute("method"), Is.EqualTo("post"));
            Assert.That(form.GetAttribute("action"), Is.EqualTo("explorer/auth/login"));
            Assert.That(form.GetAttribute("data-enhance"), Is.EqualTo("false"));
            Assert.That(
                form.QuerySelector($"input[type=hidden][name={FakeAntiforgeryStateProvider.FieldName}]")?.GetAttribute("value"),
                Is.EqualTo(FakeAntiforgeryStateProvider.Token));
            Assert.That(form.QuerySelector("input[name=username]")?.GetAttribute("autocomplete"), Is.EqualTo("username"));
            Assert.That(form.QuerySelector("input[name=password]")?.GetAttribute("autocomplete"), Is.EqualTo("current-password"));
            Assert.That(Auth.PasswordSignIns, Is.Empty, "the password never crosses the circuit");
        });
    }

    [Test]
    public void The_in_circuit_password_form_signs_in_and_closes()
    {
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        var closed = 0;
        var cut = Render<SignInDialog>(parameters => parameters.Add(p => p.OnClosed, () => closed++));

        cut.Find("input[name=username]").Input("  alice  ");
        cut.Find("input[name=password]").Input("secret");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Auth.PasswordSignIns, Is.EqualTo(new[] { ("alice", "secret") }));
            Assert.That(closed, Is.EqualTo(1));
        });
    }

    [Test]
    public void An_empty_username_is_refused_before_any_sign_in()
    {
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        var cut = Render<SignInDialog>();

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Auth.PasswordSignIns, Is.Empty);
            Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("Enter a username."));
        });
    }

    [Test]
    public void A_failed_sign_in_is_announced_and_the_dialog_stays_open()
    {
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        Auth.SignInFailure = new InvalidOperationException("The credentials were rejected.");
        var closed = 0;
        var cut = Render<SignInDialog>(parameters => parameters.Add(p => p.OnClosed, () => closed++));

        cut.Find("input[name=username]").Input("alice");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.Zero);
            Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("The credentials were rejected."));
        });
    }

    [Test]
    public void Every_advertised_method_a_registered_provider_services_is_rendered_in_the_endpoints_order()
    {
        Services.AddSingleton<IExplorerAuthMethod>(new FakeAuthMethod(ExplorerAuthSchemes.Entra));
        Services.AddSingleton<IExplorerAuthMethod>(new FakeAuthMethod(ExplorerAuthSchemes.Oidc));
        Auth.Advertisement = Advertise(
            (ExplorerAuthSchemes.Oidc, "Contoso SSO"),
            (ExplorerAuthSchemes.Entra, "Microsoft Entra ID"),
            (ExplorerAuthSchemes.Basic, string.Empty));

        var cut = Render<SignInDialog>();

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("h3").Select(heading => heading.TextContent),
                Is.EqualTo(new[] { "Contoso SSO", "Microsoft Entra ID", "Username and password" }));
            Assert.That(
                cut.FindAll("button").Select(button => button.TextContent.Trim()).Where(text => text.StartsWith("Continue", StringComparison.Ordinal)),
                Is.EqualTo(new[] { "Continue with Contoso SSO", "Continue with Microsoft Entra ID" }));
            Assert.That(cut.FindAll("input[type=password]"), Has.Count.EqualTo(1));
            foreach (var section in cut.FindAll("section"))
            {
                Assert.That(cut.Find($"#{section.GetAttribute("aria-labelledby")}").TagName, Is.EqualTo("H3"));
            }
        });
    }

    [Test]
    public void An_advertised_scheme_no_provider_services_is_left_out()
    {
        Auth.Advertisement = Advertise(("custom-mfa", "MFA"), (ExplorerAuthSchemes.Basic, string.Empty));

        var cut = Render<SignInDialog>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("button").Select(button => button.TextContent), Has.None.Contains("MFA"));
            Assert.That(cut.FindAll("input[type=password]"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Continuing_with_a_method_signs_in_with_the_advertised_scheme_and_closes()
    {
        Services.AddSingleton<IExplorerAuthMethod>(new FakeAuthMethod(ExplorerAuthSchemes.Entra));
        Auth.Advertisement = Advertise((ExplorerAuthSchemes.Entra, "Microsoft Entra ID"));
        var closed = 0;
        var cut = Render<SignInDialog>(parameters => parameters.Add(p => p.OnClosed, () => closed++));

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Continue with Microsoft Entra ID").Click();

        Assert.Multiple(() =>
        {
            Assert.That(Auth.MethodSignIns, Is.EqualTo(new[] { ExplorerAuthSchemes.Entra }));
            Assert.That(closed, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_method_that_accepts_an_alias_is_offered_through_its_own_can_handle()
    {
        // No special case: the dialog asks the method, as Core's seam intends.
        Services.AddSingleton<IExplorerAuthMethod>(new FakeAuthMethod("oidc", scheme => scheme.StartsWith("oidc-", StringComparison.Ordinal)));
        Auth.Advertisement = Advertise(("oidc-contoso", "Contoso"));

        var cut = Render<SignInDialog>();
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Continue with Contoso").Click();

        Assert.That(Auth.MethodSignIns, Is.EqualTo(new[] { "oidc-contoso" }));
    }

    [Test]
    public void An_endpoint_asking_only_for_unserviced_schemes_explains_what_to_install()
    {
        Auth.Advertisement = Advertise((ExplorerAuthSchemes.Entra, "Microsoft Entra ID"));
        var closed = 0;
        var cut = Render<SignInDialog>(parameters => parameters.Add(p => p.OnClosed, () => closed++));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("form"), Is.Empty);
            Assert.That(cut.Find("code").TextContent, Is.EqualTo(ExplorerAuthSchemes.Entra));
            Assert.That(cut.Find(".lt-dialog__body").TextContent, Does.Contain("Orleans.Lattice.Explorer.Entra"));
        });

        cut.FindAll("button").Last(button => button.TextContent.Trim() == "Close").Click();

        Assert.That(closed, Is.EqualTo(1));
    }

    [Test]
    public void A_failed_method_sign_in_is_announced()
    {
        Services.AddSingleton<IExplorerAuthMethod>(new FakeAuthMethod(ExplorerAuthSchemes.Entra));
        Auth.Advertisement = Advertise((ExplorerAuthSchemes.Entra, "Microsoft Entra ID"));
        Auth.SignInFailure = new InvalidOperationException("Consent was withdrawn.");
        var cut = Render<SignInDialog>();

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Continue with Microsoft Entra ID").Click();

        Assert.That(cut.Find("[role=alert]").TextContent, Does.Contain("Consent was withdrawn."));
    }

    [Test]
    public void Escape_closes_the_dialog()
    {
        var closed = 0;
        var cut = Render<SignInDialog>(parameters => parameters.Add(p => p.OnClosed, () => closed++));

        cut.Find("[role=dialog]").KeyDown("Escape");

        Assert.That(closed, Is.EqualTo(1));
    }

    private static ExplorerAuthSchemeAdvertisement Advertise(params (string Scheme, string DisplayName)[] schemes) => new()
    {
        Schemes = schemes.Select(scheme => new ExplorerAuthSchemeDescriptor { SchemeId = scheme.Scheme, DisplayName = scheme.DisplayName }).ToArray(),
    };
}
