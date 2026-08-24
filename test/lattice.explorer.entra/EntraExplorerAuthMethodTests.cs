using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Tests;

[TestFixture]
public class EntraExplorerAuthMethodTests
{
    private static readonly DateTimeOffset Start = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static EntraExplorerAuthMethod CreateMethod(
        FakeEntraAcquirer acquirer,
        ExplorerEntraOptions? options = null)
        => new(acquirer, new StaticOptionsMonitor<ExplorerEntraOptions>(options ?? new ExplorerEntraOptions()));

    private static ExplorerAuthChallengeContext ContextWithAdvertisedParameters(TimeProvider time)
        => new()
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            TimeProvider = time,
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.AuthorityParameter] = "https://login.microsoftonline.com/contoso",
                [ExplorerAuthSchemes.ClientIdParameter] = "client-123",
                [ExplorerAuthSchemes.AudienceParameter] = "api://state-api",
            },
        };

    [Test]
    public void SchemeId_is_entra()
    {
        var method = CreateMethod(new FakeEntraAcquirer(TimeProvider.System));
        Assert.That(method.SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
    }

    [Test]
    public void CanHandle_entra_returnsTrue_otherScheme_returnsFalse()
    {
        var method = CreateMethod(new FakeEntraAcquirer(TimeProvider.System));
        Assert.Multiple(() =>
        {
            Assert.That(method.CanHandle("entra"), Is.True);
            Assert.That(method.CanHandle("ENTRA"), Is.True);
            Assert.That(method.CanHandle("basic"), Is.False);
        });
    }

    [Test]
    public void Constructor_nullAcquirer_throws()
        => Assert.That(
            () => new EntraExplorerAuthMethod(null!, new StaticOptionsMonitor<ExplorerEntraOptions>(new())),
            Throws.ArgumentNullException);

    [Test]
    public void Constructor_nullOptions_throws()
        => Assert.That(
            () => new EntraExplorerAuthMethod(new FakeEntraAcquirer(TimeProvider.System), null!),
            Throws.ArgumentNullException);

    [Test]
    public void ChallengeAsync_nullContext_throws()
    {
        var method = CreateMethod(new FakeEntraAcquirer(TimeProvider.System));
        Assert.That(async () => await method.ChallengeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ChallengeAsync_advertisedParameters_acquiresInteractive_andReturnsBearerSignIn()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var method = CreateMethod(acquirer);

        var signIn = await method.ChallengeAsync(ContextWithAdvertisedParameters(time));

        Assert.Multiple(() =>
        {
            Assert.That(acquirer.InteractiveCount, Is.EqualTo(1));
            Assert.That(signIn.SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
            Assert.That(signIn.DisplayName, Is.EqualTo("user@contoso.com"));
            Assert.That(signIn.Authentication.HasCredentialProvider, Is.True);
            Assert.That(signIn.Authentication.HasHeaders, Is.False);
        });

        var header = await signIn.Authentication.CredentialProvider!.GetAuthorizationHeaderAsync();
        Assert.That(header, Is.EqualTo("Bearer access-1"));
    }

    [Test]
    public async Task ChallengeAsync_forwardsAdvertisedParameters_toTokenRequest()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var method = CreateMethod(acquirer);

        await method.ChallengeAsync(ContextWithAdvertisedParameters(time));

        Assert.That(acquirer.LastRequest, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(acquirer.LastRequest!.Authority, Is.EqualTo("https://login.microsoftonline.com/contoso"));
            Assert.That(acquirer.LastRequest!.ClientId, Is.EqualTo("client-123"));
            Assert.That(acquirer.LastRequest!.Scopes, Is.EqualTo(new[] { "api://state-api/.default" }));
        });
    }

    [Test]
    public async Task ChallengeAsync_staticOptions_usedWhenNoAdvertisedParameters()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var options = new ExplorerEntraOptions
        {
            Authority = "https://login.microsoftonline.com/fabrikam",
            ClientId = "static-client",
        };
        options.Scopes.Add("api://static/.default");
        var method = CreateMethod(acquirer, options);

        await method.ChallengeAsync(new ExplorerAuthChallengeContext
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            TimeProvider = time,
        });

        Assert.Multiple(() =>
        {
            Assert.That(acquirer.LastRequest!.Authority, Is.EqualTo("https://login.microsoftonline.com/fabrikam"));
            Assert.That(acquirer.LastRequest!.ClientId, Is.EqualTo("static-client"));
            Assert.That(acquirer.LastRequest!.Scopes, Is.EqualTo(new[] { "api://static/.default" }));
        });
    }

    [Test]
    public void ChallengeAsync_missingAuthority_throwsInvalidOperationException()
    {
        var method = CreateMethod(new FakeEntraAcquirer(TimeProvider.System));
        var context = new ExplorerAuthChallengeContext
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.ClientIdParameter] = "client-123",
                [ExplorerAuthSchemes.AudienceParameter] = "api://state-api",
            },
        };
        Assert.That(async () => await method.ChallengeAsync(context), Throws.InvalidOperationException);
    }

    [Test]
    public void ChallengeAsync_missingClientId_throwsInvalidOperationException()
    {
        var method = CreateMethod(new FakeEntraAcquirer(TimeProvider.System));
        var context = new ExplorerAuthChallengeContext
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.AuthorityParameter] = "https://login.microsoftonline.com/contoso",
                [ExplorerAuthSchemes.AudienceParameter] = "api://state-api",
            },
        };
        Assert.That(async () => await method.ChallengeAsync(context), Throws.InvalidOperationException);
    }

    [Test]
    public void ChallengeAsync_missingScope_throwsInvalidOperationException()
    {
        var method = CreateMethod(new FakeEntraAcquirer(TimeProvider.System));
        var context = new ExplorerAuthChallengeContext
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.AuthorityParameter] = "https://login.microsoftonline.com/contoso",
                [ExplorerAuthSchemes.ClientIdParameter] = "client-123",
            },
        };
        Assert.That(async () => await method.ChallengeAsync(context), Throws.InvalidOperationException);
    }

    [Test]
    public async Task Token_expiringWithinMargin_triggersSilentRefresh_andRebuildsHeader()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time)
        {
            SilentResult = new EntraTokenResult
            {
                AccessToken = "access-2",
                ExpiresOn = Start.AddMinutes(30),
                Username = "user@contoso.com",
            },
        };
        var method = CreateMethod(acquirer);

        var signIn = await method.ChallengeAsync(ContextWithAdvertisedParameters(time));
        var provider = signIn.Authentication.CredentialProvider!;

        // Initial token is valid; no silent acquisition yet.
        Assert.That(await provider.GetAuthorizationHeaderAsync(), Is.EqualTo("Bearer access-1"));
        Assert.That(acquirer.SilentCount, Is.EqualTo(0));

        // Cross into the proactive-refresh margin (token expires at +10m, margin 2m).
        time.Advance(TimeSpan.FromMinutes(9));

        Assert.That(await provider.GetAuthorizationHeaderAsync(), Is.EqualTo("Bearer access-2"));
        Assert.That(acquirer.SilentCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Token_silentReturnsNull_reChallengeRequired_headerBecomesNull()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time) { SilentResult = null };
        var method = CreateMethod(acquirer);

        var signIn = await method.ChallengeAsync(ContextWithAdvertisedParameters(time));
        var provider = signIn.Authentication.CredentialProvider!;

        time.Advance(TimeSpan.FromMinutes(9));

        Assert.That(await provider.GetAuthorizationHeaderAsync(), Is.Null);
        Assert.That(acquirer.SilentCount, Is.EqualTo(1));
    }

    [Test]
    public async Task SilentRenewal_bindsToSignedInAccount_soRenewalRequestCarriesUsername()
    {
        // Credential-confusion regression: the initial interactive request has
        // no username yet, but the silent-renewal request must carry the account
        // that actually signed in, so a shared MSAL cache never renews this
        // connection with a different operator's identity.
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time)
        {
            SilentResult = new EntraTokenResult
            {
                AccessToken = "access-2",
                ExpiresOn = Start.AddMinutes(30),
                Username = "user@contoso.com",
            },
        };
        var method = CreateMethod(acquirer);

        var signIn = await method.ChallengeAsync(ContextWithAdvertisedParameters(time));
        var provider = signIn.Authentication.CredentialProvider!;

        // The initial interactive acquisition carries no bound account yet.
        Assert.That(acquirer.LastRequest!.Username, Is.Null.Or.Empty);

        // Cross into the proactive-refresh margin to trigger silent renewal.
        time.Advance(TimeSpan.FromMinutes(9));
        await provider.GetAuthorizationHeaderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(acquirer.SilentCount, Is.EqualTo(1));
            Assert.That(acquirer.LastRequest!.Username, Is.EqualTo("user@contoso.com"));
        });
    }
}
