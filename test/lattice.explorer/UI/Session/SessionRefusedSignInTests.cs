using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// A password the cluster refuses (issue #5415) is shown as refused, never as a
/// signed-in session with no grants: the sign-in dialog reopens saying the cluster
/// did not accept the username and password, and a session whose credential the
/// cluster refuses later is read as needing a sign-in. A signed-in caller the
/// cluster merely forbids is still shown as signed in.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SessionRefusedSignInTests : SessionTestContext
{
    private const string Endpoint = "https://cluster.example:443";

    private NavigationManager Navigation => Services.GetRequiredService<NavigationManager>();

    [Test]
    public void The_refused_notice_reopens_the_sign_in_dialog_saying_so_and_leaves_the_address()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        SetRendererInfo(new RendererInfo("Server", isInteractive: true));
        Navigation.NavigateTo("/access?signin=refused");

        var cut = Render<SessionOverlay>();

        cut.WaitForAssertion(() => Assert.Multiple(() =>
        {
            Assert.That(cut.FindComponents<SignInDialog>(), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("[role=alert]").Select(alert => alert.TextContent), Has.Some.Contains(ExplorerSignInRefusedException.PasswordRefusedMessage));
            Assert.That(Navigation.Uri, Does.Not.Contain("signin="));
            Assert.That(new Uri(Navigation.Uri).AbsolutePath, Is.EqualTo("/access"));
        }));
    }

    [Test]
    public void The_prerender_leaves_the_notice_for_the_interactive_circuit()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        SetRendererInfo(new RendererInfo("Static", isInteractive: false));
        Navigation.NavigateTo("/?signin=refused");

        var cut = Render<SessionOverlay>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindComponents<SignInDialog>(), Is.Empty);
            Assert.That(Navigation.Uri, Does.Contain("signin=refused"));
        });
    }

    [Test]
    public void A_signed_in_session_drops_the_notice_without_reopening_the_dialog()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        Auth.SignIn("alice");
        SetRendererInfo(new RendererInfo("Server", isInteractive: true));
        Navigation.NavigateTo("/?signin=refused");

        var cut = Render<SessionOverlay>();

        cut.WaitForAssertion(() => Assert.Multiple(() =>
        {
            Assert.That(cut.FindComponents<SignInDialog>(), Is.Empty);
            Assert.That(Navigation.Uri, Does.Not.Contain("signin="));
        }));
    }

    [TestCase("https://h/?signin=refused", true, "https://h/")]
    [TestCase("https://h/data?tenant=acme&signin=refused#top", true, "https://h/data?tenant=acme#top")]
    [TestCase("https://h/?signin=other", false, "https://h/?signin=other")]
    [TestCase("https://h/", false, "https://h/")]
    public void TryTakeRefusedNotice_reads_and_removes_only_the_notice(string uri, bool expected, string remaining)
    {
        var taken = SessionOverlay.TryTakeRefusedNotice(uri, out var rest);

        Assert.Multiple(() =>
        {
            Assert.That(taken, Is.EqualTo(expected));
            Assert.That(rest, Is.EqualTo(remaining));
        });
    }

    [Test]
    public void OpenSignInWithNotice_opens_the_dialog_with_the_notice_until_it_closes()
    {
        var state = State;
        var opening = new List<SessionOverlayKind>();
        state.OverlayOpening += opening.Add;

        state.OpenSignInWithNotice("Refused.");
        var open = (state.Overlay, state.SignInNotice);
        state.CloseOverlay();

        Assert.Multiple(() =>
        {
            Assert.That(open, Is.EqualTo((SessionOverlayKind.SignIn, "Refused.")));
            Assert.That(opening, Is.EqualTo(new[] { SessionOverlayKind.SignIn }));
            Assert.That(state.SignInNotice, Is.Null);
            Assert.Throws<ArgumentException>(() => state.OpenSignInWithNotice(" "));
        });
    }

    [Test]
    public void A_refused_credential_reads_sign_in_required_and_offers_sign_in_even_while_signed_in()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        Auth.SignIn("olivia");
        StateConnection.Seed(Refused());

        var cut = Render<ConnectionIndicator>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-pill").TextContent, Does.Contain("Sign-in required"));
            Assert.That(cut.FindAll("button").Select(b => b.TextContent.Trim()), Has.Member("Sign in"));
        });
    }

    [Test]
    public void A_refused_credential_is_not_presented_as_signed_in()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        Auth.SignIn("olivia");
        var cut = Render<IdentityMenu>();
        Assert.That(cut.Markup, Does.Contain("olivia"));

        cut.InvokeAsync(() => StateConnection.Move(Refused())).GetAwaiter().GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Markup, Does.Not.Contain("olivia"));
            Assert.That(cut.Find("button").TextContent.Trim(), Is.EqualTo("Sign in"));
        });
    }

    [Test]
    public void A_signed_in_caller_the_cluster_forbids_is_still_shown_as_signed_in()
    {
        Explorer.Configured(RemoteConfiguration(Endpoint));
        Auth.SignIn("dave");
        StateConnection.Seed(new LatticeConnectionStatus(LatticeConnectionState.Faulted, Endpoint, "denied", RequiresAuthentication: true));

        var menu = Render<IdentityMenu>();
        var indicator = Render<ConnectionIndicator>();

        Assert.Multiple(() =>
        {
            Assert.That(menu.Markup, Does.Contain("dave"));
            Assert.That(indicator.FindAll("button").Select(b => b.TextContent.Trim()), Has.No.Member("Sign in"));
        });
    }

    private static LatticeConnectionStatus Refused() =>
        new(LatticeConnectionState.Faulted, Endpoint, "refused", RequiresAuthentication: true) { CredentialRefused = true };
}
