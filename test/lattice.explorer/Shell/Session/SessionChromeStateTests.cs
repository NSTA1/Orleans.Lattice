using Orleans.Lattice.Explorer.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The circuit's session chrome state: one-time initialisation, the requested
/// surface, and the re-authentication latch.
/// </summary>
[TestFixture]
public sealed class SessionChromeStateTests
{
    private FakeExplorerSession _explorer = null!;
    private FakeAuthSession _auth = null!;

    [SetUp]
    public void SetUp()
    {
        _explorer = new FakeExplorerSession(new FakeStateConnection());
        _auth = new FakeAuthSession();
    }

    [Test]
    public void Construction_rejects_missing_sessions()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new SessionChromeState(null!, _auth), Throws.ArgumentNullException);
            Assert.That(() => new SessionChromeState(_explorer, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void It_starts_uninitialised_with_no_surface_and_no_latch()
    {
        using var state = new SessionChromeState(_explorer, _auth);

        Assert.Multiple(() =>
        {
            Assert.That(state.IsInitialized, Is.False);
            Assert.That(state.Overlay, Is.EqualTo(SessionOverlayKind.None));
            Assert.That(state.ReauthRequired, Is.False);
        });
    }

    [Test]
    public async Task Initialisation_runs_both_sessions_once_and_announces_it()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        var changes = 0;
        state.Changed += () => changes++;

        var first = state.EnsureInitializedAsync();
        var second = state.EnsureInitializedAsync();
        await first;

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first));
            Assert.That(_explorer.Initializations, Is.EqualTo(1));
            Assert.That(_auth.Initializations, Is.EqualTo(1));
            Assert.That(state.IsInitialized, Is.True);
            Assert.That(changes, Is.EqualTo(1));
        });
    }

    [Test]
    public void Opening_and_closing_surfaces_announces_only_real_changes()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        var changes = 0;
        state.Changed += () => changes++;

        state.OpenSignIn();
        state.OpenSignIn();
        Assert.That(state.Overlay, Is.EqualTo(SessionOverlayKind.SignIn));
        state.OpenConfiguration();
        Assert.That(state.Overlay, Is.EqualTo(SessionOverlayKind.Configuration));
        state.CloseOverlay();
        state.CloseOverlay();

        Assert.Multiple(() =>
        {
            Assert.That(state.Overlay, Is.EqualTo(SessionOverlayKind.None));
            Assert.That(changes, Is.EqualTo(3));
        });
    }

    [Test]
    public void The_reauth_latch_closes_any_surface_and_fires_once()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        var changes = 0;
        state.OpenSignIn();
        state.Changed += () => changes++;

        _auth.RaiseReauthRequired();
        _auth.RaiseReauthRequired();

        Assert.Multiple(() =>
        {
            Assert.That(state.ReauthRequired, Is.True);
            Assert.That(state.Overlay, Is.EqualTo(SessionOverlayKind.None));
            Assert.That(changes, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_fresh_sign_in_clears_the_latch()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        _auth.RaiseReauthRequired();

        _auth.SignIn("alice");

        Assert.That(state.ReauthRequired, Is.False);
    }

    [Test]
    public async Task A_sign_out_does_not_clear_the_latch()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        _auth.RaiseReauthRequired();

        await _auth.LogoutAsync();

        Assert.That(state.ReauthRequired, Is.True);
    }

    [Test]
    public void Overlay_opening_is_raised_before_each_new_surface_with_its_kind()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        var opening = new List<(SessionOverlayKind Kind, SessionOverlayKind OverlayAtTheTime)>();
        state.OverlayOpening += kind => opening.Add((kind, state.Overlay));

        state.OpenSignIn();
        state.OpenSignIn();
        state.OpenConfiguration();
        state.CloseOverlay();

        Assert.That(opening, Is.EqualTo(new[]
        {
            (SessionOverlayKind.SignIn, SessionOverlayKind.None),
            (SessionOverlayKind.Configuration, SessionOverlayKind.SignIn),
        }), "raised before the change, once per surface, and never for a close");
    }

    [Test]
    public void Overlay_opening_is_raised_once_for_the_reauth_latch()
    {
        using var state = new SessionChromeState(_explorer, _auth);
        var opening = new List<(SessionOverlayKind Kind, bool LatchedAtTheTime)>();
        state.OverlayOpening += kind => opening.Add((kind, state.ReauthRequired));

        _auth.RaiseReauthRequired();
        _auth.RaiseReauthRequired();

        Assert.That(opening, Is.EqualTo(new[] { (SessionOverlayKind.None, false) }));
    }

    [Test]
    public void Disposal_unsubscribes_from_the_sign_in_session()
    {
        var state = new SessionChromeState(_explorer, _auth);
        Assert.That(_auth.ReauthSubscribers, Is.EqualTo(1));

        state.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(_auth.ReauthSubscribers, Is.Zero);
            Assert.That(_auth.AuthenticationSubscribers, Is.Zero);
        });
    }
}
