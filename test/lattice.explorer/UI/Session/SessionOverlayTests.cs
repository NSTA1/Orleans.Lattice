using Bunit;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The session overlay: the first-run connection gate, the surfaces a circuit
/// asks for, and the re-authentication interstitial, shown one at a time in
/// priority order.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SessionOverlayTests : SessionTestContext
{
    [Test]
    public void First_run_shows_the_mandatory_connection_gate()
    {
        var cut = Render<SessionOverlay>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindComponents<ConnectionDialog>(), Has.Count.EqualTo(1));
            Assert.That(cut.FindComponent<ConnectionDialog>().Instance.AllowCancel, Is.False);
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.No.Member("Cancel"));
            Assert.That(cut.Find("[role=dialog] h2").TextContent, Is.EqualTo("Connect to a cluster"));
        });
    }

    [Test]
    public void It_initialises_the_connection_and_sign_in_sessions_once()
    {
        Render<SessionOverlay>();
        Render<SessionOverlay>();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Initializations, Is.EqualTo(1));
            Assert.That(Auth.Initializations, Is.EqualTo(1));
            Assert.That(State.IsInitialized, Is.True);
        });
    }

    [Test]
    public void Saving_the_first_run_gate_dismisses_it()
    {
        var cut = Render<SessionOverlay>();

        cut.Find("input.lt-input").Input("https://cluster.example:443");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Applied, Has.Count.EqualTo(1));
            Assert.That(cut.FindComponents<ConnectionDialog>(), Is.Empty);
            Assert.That(cut.FindAll("[role=dialog]"), Is.Empty);
        });
    }

    [Test]
    public void A_configured_session_shows_nothing_until_a_surface_is_asked_for()
    {
        Explorer.Configured(RemoteConfiguration());

        var cut = Render<SessionOverlay>();

        Assert.That(cut.FindAll("[role=dialog],[role=alertdialog]"), Is.Empty);
    }

    [Test]
    public async Task Asking_for_the_connection_settings_opens_them_cancellable_and_pre_filled()
    {
        Explorer.Configured(RemoteConfiguration("https://cluster.example:8443"));
        var cut = Render<SessionOverlay>();

        await cut.InvokeAsync(State.OpenConfiguration);

        var dialog = cut.FindComponent<ConnectionDialog>();
        Assert.Multiple(() =>
        {
            Assert.That(dialog.Instance.AllowCancel, Is.True);
            Assert.That(cut.Find("input.lt-input").GetAttribute("value"), Is.EqualTo("https://cluster.example:8443"));
        });

        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Cancel").Click();

        Assert.Multiple(() =>
        {
            Assert.That(State.Overlay, Is.EqualTo(SessionOverlayKind.None));
            Assert.That(cut.FindComponents<ConnectionDialog>(), Is.Empty);
        });
    }

    [Test]
    public async Task Asking_for_the_sign_in_opens_the_one_sign_in_dialog()
    {
        Explorer.Configured(RemoteConfiguration());
        var cut = Render<SessionOverlay>();

        await cut.InvokeAsync(State.OpenSignIn);

        Assert.That(cut.FindComponents<SignInDialog>(), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task An_open_sign_in_dialog_survives_a_configuration_change()
    {
        // Parity with the old shell's guard that an open sign-in dialog is not
        // torn down by an unrelated re-render.
        Explorer.Configured(RemoteConfiguration());
        var cut = Render<SessionOverlay>();
        await cut.InvokeAsync(State.OpenSignIn);

        await cut.InvokeAsync(() => Explorer.ApplyAsync(RemoteConfiguration("https://other.example:443")));

        Assert.That(cut.FindComponents<SignInDialog>(), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_sign_in_that_succeeds_in_circuit_closes_the_dialog()
    {
        UseOptions(new SessionSignInOptions { UseServerFormPost = false });
        Explorer.Configured(RemoteConfiguration());
        var cut = Render<SessionOverlay>();
        await cut.InvokeAsync(State.OpenSignIn);

        cut.Find("input[name=username]").Input("alice");
        cut.Find("input[name=password]").Input("secret");
        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(Auth.IsAuthenticated, Is.True);
            Assert.That(cut.FindComponents<SignInDialog>(), Is.Empty);
        });
    }

    [Test]
    public async Task Re_authentication_outranks_every_other_surface()
    {
        Explorer.Configured(RemoteConfiguration());
        Auth.SignIn("alice");
        var cut = Render<SessionOverlay>();
        await cut.InvokeAsync(State.OpenSignIn);

        await cut.InvokeAsync(Auth.RaiseReauthRequired);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindComponents<ReauthInterstitial>(), Has.Count.EqualTo(1));
            Assert.That(cut.FindComponents<SignInDialog>(), Is.Empty);
            Assert.That(cut.Find("[role=alertdialog] h2").TextContent, Is.EqualTo("Your session expired"));
        });
    }

    [Test]
    public async Task Re_authentication_outranks_even_an_unconfigured_session()
    {
        var cut = Render<SessionOverlay>();

        await cut.InvokeAsync(Auth.RaiseReauthRequired);

        Assert.That(cut.FindComponents<ConnectionDialog>(), Is.Empty);
    }

    [Test]
    public void Disposing_the_overlay_unsubscribes_it()
    {
        Explorer.Configured(RemoteConfiguration());
        var cut = Render<SessionOverlay>();
        var subscribed = Explorer.ConfigurationSubscribers;

        cut.Instance.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(subscribed, Is.EqualTo(1));
            Assert.That(Explorer.ConfigurationSubscribers, Is.Zero);
        });
    }

    [Test]
    public async Task The_basic_method_is_offered_by_default()
    {
        Explorer.Configured(RemoteConfiguration());
        var cut = Render<SessionOverlay>();
        await cut.InvokeAsync(State.OpenSignIn);

        Assert.That(Auth.AvailableSchemes, Does.Contain(ExplorerAuthSchemes.Basic));
        Assert.That(cut.FindAll("input[type=password]"), Has.Count.EqualTo(1));
    }
}
