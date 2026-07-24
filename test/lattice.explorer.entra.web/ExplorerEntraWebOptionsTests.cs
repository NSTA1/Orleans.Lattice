namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for <see cref="ExplorerEntraWebOptions"/> validation and defaults.
/// </summary>
[TestFixture]
public sealed class ExplorerEntraWebOptionsTests
{
    private static ExplorerEntraWebOptions Valid() => new()
    {
        TenantId = "tenant",
        ClientId = "client",
    };

    [Test]
    public void Defaults_are_the_documented_values()
    {
        var options = new ExplorerEntraWebOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.Instance, Is.EqualTo("https://login.microsoftonline.com/"));
            Assert.That(options.CallbackPath, Is.EqualTo("/signin-oidc"));
            Assert.That(options.SignedOutCallbackPath, Is.EqualTo("/signout-callback-oidc"));
            Assert.That(options.TokenCache, Is.EqualTo(ExplorerWebTokenCacheKind.InMemory));
            Assert.That(options.RequireAuthenticatedUser, Is.True);
            Assert.That(options.AutoSignIn, Is.True);
            Assert.That(options.Scopes, Is.Empty);
            Assert.That(options.ReauthChallengePath, Is.EqualTo("/explorer-entra/reauth"));
        });
    }

    [Test]
    public void Validate_passes_for_a_minimal_valid_configuration()
    {
        Assert.DoesNotThrow(() => Valid().Validate());
    }

    [Test]
    public void Validate_throws_when_tenant_id_is_missing()
    {
        var options = Valid();
        options.TenantId = "  ";

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(ExplorerEntraWebOptions.TenantId)));
    }

    [Test]
    public void Validate_throws_when_client_id_is_missing()
    {
        var options = Valid();
        options.ClientId = null;

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(ExplorerEntraWebOptions.ClientId)));
    }

    [Test]
    public void Validate_throws_when_instance_is_blank()
    {
        var options = Valid();
        options.Instance = "";

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(ExplorerEntraWebOptions.Instance)));
    }

    [Test]
    public void Validate_throws_when_callback_path_is_blank()
    {
        var options = Valid();
        options.CallbackPath = "   ";

        var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
        Assert.That(ex!.Message, Does.Contain(nameof(ExplorerEntraWebOptions.CallbackPath)));
    }
}
