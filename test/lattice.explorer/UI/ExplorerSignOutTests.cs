using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Authentication;

namespace Orleans.Lattice.Explorer.Tests.UI;

/// <summary>
/// Tests <see cref="ExplorerSignOut.Resolve"/>, the pure decision that shapes the
/// explorer's "Sign out" control: a configured federated sign-out path forces a
/// server form post to that endpoint (a hosted-web head must end the browser
/// session, not just drop the API credential), otherwise the control falls back
/// to the per-head <see cref="ExplorerAuthUiOptions"/>.
/// </summary>
[TestFixture]
public class ExplorerSignOutTests
{
    [Test]
    public void Resolve_nullUiOptions_throws()
    {
        Assert.That(() => ExplorerSignOut.Resolve(null!, null), Throws.ArgumentNullException);
    }

    [Test]
    public void Resolve_federatedPath_forcesFormPostToThatEndpoint()
    {
        // The desktop-shaped UI options (no server form post) must be overridden:
        // a federated sign-out is always a full server round-trip.
        var ui = new ExplorerAuthUiOptions { UseServerFormPost = false, LogoutPath = "/auth/logout" };
        var signOut = new ExplorerSignOutOptions { FederatedSignOutPath = "/explorer-entra/signout" };

        var target = ExplorerSignOut.Resolve(ui, signOut);

        Assert.Multiple(() =>
        {
            Assert.That(target.UseServerFormPost, Is.True);
            Assert.That(target.FormAction, Is.EqualTo("/explorer-entra/signout"));
        });
    }

    [Test]
    public void Resolve_federatedPathWithServerFormPost_stillTargetsFederatedEndpoint()
    {
        // The Entra web head layers on the cookie web head, so both a LogoutPath
        // and a federated path are present: the federated path must win.
        var ui = new ExplorerAuthUiOptions { UseServerFormPost = true, LogoutPath = "/auth/logout" };
        var signOut = new ExplorerSignOutOptions { FederatedSignOutPath = "/explorer-entra/signout" };

        var target = ExplorerSignOut.Resolve(ui, signOut);

        Assert.Multiple(() =>
        {
            Assert.That(target.UseServerFormPost, Is.True);
            Assert.That(target.FormAction, Is.EqualTo("/explorer-entra/signout"));
        });
    }

    [Test]
    public void Resolve_noFederatedPath_serverFormPostHead_postsToLogoutPath()
    {
        var ui = new ExplorerAuthUiOptions { UseServerFormPost = true, LogoutPath = "/app/auth/logout" };

        var target = ExplorerSignOut.Resolve(ui, signOutOptions: null);

        Assert.Multiple(() =>
        {
            Assert.That(target.UseServerFormPost, Is.True);
            Assert.That(target.FormAction, Is.EqualTo("/app/auth/logout"));
        });
    }

    [Test]
    public void Resolve_noFederatedPath_inProcessHead_usesInCircuitButton()
    {
        var ui = new ExplorerAuthUiOptions { UseServerFormPost = false, LogoutPath = "/auth/logout" };

        var target = ExplorerSignOut.Resolve(ui, signOutOptions: null);

        Assert.Multiple(() =>
        {
            Assert.That(target.UseServerFormPost, Is.False);
            Assert.That(target.FormAction, Is.Empty);
        });
    }

    [Test]
    public void Resolve_emptyFederatedPath_isIgnored()
    {
        var ui = new ExplorerAuthUiOptions { UseServerFormPost = false, LogoutPath = "/auth/logout" };
        var signOut = new ExplorerSignOutOptions { FederatedSignOutPath = string.Empty };

        var target = ExplorerSignOut.Resolve(ui, signOut);

        Assert.Multiple(() =>
        {
            Assert.That(target.UseServerFormPost, Is.False);
            Assert.That(target.FormAction, Is.Empty);
        });
    }
}
