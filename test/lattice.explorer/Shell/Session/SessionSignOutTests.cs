using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Shell.Session;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// The sign-out decision and the sign-in options it reads: a federated sign-out
/// path forces a server form post to that endpoint, otherwise the options decide.
/// Parity with the old UI's <c>ExplorerSignOutTests</c>.
/// </summary>
[TestFixture]
public sealed class SessionSignOutTests
{
    [Test]
    public void Resolve_rejects_missing_options()
    {
        Assert.That(() => SessionSignOut.Resolve(null!, null), Throws.ArgumentNullException);
    }

    [Test]
    public void A_federated_path_forces_a_form_post_to_that_endpoint()
    {
        var options = new SessionSignInOptions { UseServerFormPost = false };
        var signOut = new ExplorerSignOutOptions { FederatedSignOutPath = "/explorer-entra/signout" };

        Assert.That(SessionSignOut.Resolve(options, signOut), Is.EqualTo(new SessionSignOutTarget(true, "/explorer-entra/signout")));
    }

    [Test]
    public void A_federated_path_wins_over_a_form_posting_head()
    {
        var options = new SessionSignInOptions { UseServerFormPost = true, LogoutPath = "auth/logout" };
        var signOut = new ExplorerSignOutOptions { FederatedSignOutPath = "/explorer-entra/signout" };

        Assert.That(SessionSignOut.Resolve(options, signOut).FormAction, Is.EqualTo("/explorer-entra/signout"));
    }

    [Test]
    public void A_form_posting_head_without_a_federated_path_posts_to_its_logout_path()
    {
        var options = new SessionSignInOptions { UseServerFormPost = true, LogoutPath = "app/auth/logout" };

        Assert.That(SessionSignOut.Resolve(options, signOutOptions: null), Is.EqualTo(new SessionSignOutTarget(true, "app/auth/logout")));
    }

    [Test]
    public void An_in_circuit_head_uses_the_in_circuit_button()
    {
        var options = new SessionSignInOptions { UseServerFormPost = false };

        Assert.That(SessionSignOut.Resolve(options, signOutOptions: null), Is.EqualTo(new SessionSignOutTarget(false, string.Empty)));
    }

    [Test]
    public void An_empty_federated_path_is_ignored()
    {
        var options = new SessionSignInOptions { UseServerFormPost = false };
        var signOut = new ExplorerSignOutOptions { FederatedSignOutPath = string.Empty };

        Assert.That(SessionSignOut.Resolve(options, signOut), Is.EqualTo(new SessionSignOutTarget(false, string.Empty)));
    }

    [Test]
    public void The_default_options_post_to_relative_paths()
    {
        // Blazor Server is the only head, so the default keeps the password off
        // the circuit; relative paths resolve against the document base.
        var options = new SessionSignInOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.UseServerFormPost, Is.True);
            Assert.That(options.LoginPath, Is.EqualTo(SessionSignInOptions.DefaultLoginPath).And.EqualTo("auth/login"));
            Assert.That(options.LogoutPath, Is.EqualTo(SessionSignInOptions.DefaultLogoutPath).And.EqualTo("auth/logout"));
        });
    }
}
