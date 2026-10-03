using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Tests;

/// <summary>
/// Regression tests for the advertised-audience admission rule. The audience is
/// fetched over an unauthenticated RPC from the very endpoint the minted token is
/// handed to, so when nothing is configured locally it is the only thing naming
/// the resource the operator's delegated token is minted for. An endpoint that
/// advertised a foreign first-party resource (Microsoft Graph, ARM, Key Vault)
/// would otherwise have the console mint a token for it and hand that token
/// straight back - a confused deputy. These tests pin the refusal, the shapes a
/// real State API deployment takes, and the two operator overrides.
/// </summary>
[TestFixture]
public sealed class EntraExplorerAuthMethodAudienceAdmissionTests
{
    private static readonly DateTimeOffset Start = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static EntraExplorerAuthMethod CreateMethod(
        FakeEntraAcquirer acquirer,
        ExplorerEntraOptions? options = null)
        => new(acquirer, new StaticOptionsMonitor<ExplorerEntraOptions>(options ?? new ExplorerEntraOptions()));

    private static ExplorerAuthChallengeContext Context(string audience, TimeProvider time, string? endpoint = null)
        => new()
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            TimeProvider = time,
            Endpoint = endpoint,
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.AuthorityParameter] = "https://login.microsoftonline.com/contoso",
                [ExplorerAuthSchemes.ClientIdParameter] = "client-123",
                [ExplorerAuthSchemes.AudienceParameter] = audience,
            },
        };

    [Test]
    [TestCase("https://graph.microsoft.com")]
    [TestCase("https://graph.microsoft.com/.default")]
    [TestCase("https://management.azure.com")]
    [TestCase("https://vault.azure.net")]
    public void ChallengeAsync_advertisedForeignFirstPartyAudience_isRefusedAndNoTokenIsMinted(string audience)
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var method = CreateMethod(acquirer);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await method.ChallengeAsync(Context(audience, time, "https://state.contoso.example")),
                Throws.InvalidOperationException.With.Message.Contains(audience));
            Assert.That(acquirer.InteractiveCount, Is.Zero, "no token may be minted for a refused audience");
        });
    }

    [Test]
    public async Task ChallengeAsync_advertisedApiResourceIdentifier_isAdmitted()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);

        await CreateMethod(acquirer).ChallengeAsync(Context("api://state-api", time));

        Assert.That(acquirer.LastRequest!.Scopes, Is.EqualTo(new[] { "api://state-api/.default" }));
    }

    [Test]
    public async Task ChallengeAsync_advertisedHttpsAudienceOnTheEndpointHost_isAdmitted()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);

        await CreateMethod(acquirer)
            .ChallengeAsync(Context("https://state.contoso.example/api", time, "https://state.contoso.example:8080"));

        Assert.That(acquirer.LastRequest!.Scopes, Is.EqualTo(new[] { "https://state.contoso.example/api/.default" }));
    }

    [Test]
    public void ChallengeAsync_advertisedHttpsAudienceOnAForeignHost_isRefused()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);

        Assert.That(
            async () => await CreateMethod(acquirer)
                .ChallengeAsync(Context("https://evil.example/api", time, "https://state.contoso.example")),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task ChallengeAsync_configuredScopes_bypassTheAdvertisedAudienceEntirely()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var options = new ExplorerEntraOptions();
        options.Scopes.Add("api://state-api/.default");

        await CreateMethod(acquirer, options).ChallengeAsync(Context("https://graph.microsoft.com", time));

        Assert.That(acquirer.LastRequest!.Scopes, Is.EqualTo(new[] { "api://state-api/.default" }));
    }

    [Test]
    public async Task ChallengeAsync_operatorAllowedAudience_isAdmitted()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var options = new ExplorerEntraOptions();
        options.AllowedAudiences.Add("https://state.contoso.example/api");

        await CreateMethod(acquirer, options).ChallengeAsync(Context("https://state.contoso.example/api", time));

        Assert.That(acquirer.LastRequest!.Scopes, Is.EqualTo(new[] { "https://state.contoso.example/api/.default" }));
    }

    [Test]
    public void ChallengeAsync_allowedAudiencesConfigured_refusesEveryOtherAudienceIncludingApiResources()
    {
        var time = new MutableTimeProvider(Start);
        var acquirer = new FakeEntraAcquirer(time);
        var options = new ExplorerEntraOptions();
        options.AllowedAudiences.Add("api://state-api");

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await CreateMethod(acquirer, options).ChallengeAsync(Context("api://other-api", time)),
                Throws.InvalidOperationException.With.Message.Contains("AllowedAudiences"));
            Assert.That(acquirer.InteractiveCount, Is.Zero);
        });
    }
}
