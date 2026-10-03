using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Regression tests for the advertised-audience admission rule in the web head.
/// The advertisement arrives over an unauthenticated RPC from the very endpoint
/// the minted token is handed to, and the auto sign-in circuit handler drives the
/// flow with no operator click, so an endpoint that advertised a foreign
/// first-party resource (Microsoft Graph, ARM, Key Vault) would have the console
/// silently mint a delegated token for it and hand that token straight back.
/// These tests pin the refusal, the shapes a real State API deployment takes, and
/// the two operator overrides.
/// </summary>
[TestFixture]
public sealed class EntraWebExplorerAuthMethodAudienceAdmissionTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static EntraWebExplorerAuthMethod CreateMethod(
        FakeWebTokenAcquirer acquirer,
        ExplorerEntraWebOptions? options = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<ExplorerEntraWebOptions>>();
        monitor.CurrentValue.Returns(options ?? new ExplorerEntraWebOptions());
        return new EntraWebExplorerAuthMethod(acquirer, monitor);
    }

    private static ExplorerAuthChallengeContext Context(string audience, TimeProvider clock, string? endpoint = null)
        => new()
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            TimeProvider = clock,
            Endpoint = endpoint,
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [ExplorerAuthSchemes.AudienceParameter] = audience,
            },
        };

    private static FakeWebTokenAcquirer Acquirer()
        => new FakeWebTokenAcquirer().EnqueueToken(new ExplorerWebToken
        {
            AccessToken = "tok",
            ExpiresOn = Start.AddHours(1),
            Username = "alice@contoso.com",
        });

    [Test]
    [TestCase("https://graph.microsoft.com")]
    [TestCase("https://graph.microsoft.com/.default")]
    [TestCase("https://management.azure.com")]
    [TestCase("https://vault.azure.net")]
    public void ChallengeAsync_advertisedForeignFirstPartyAudience_isRefusedAndNoTokenIsMinted(string audience)
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = Acquirer();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await CreateMethod(acquirer)
                    .ChallengeAsync(Context(audience, clock, "https://state.contoso.example")),
                Throws.InvalidOperationException.With.Message.Contains(audience));
            Assert.That(acquirer.CallCount, Is.Zero, "no token may be minted for a refused audience");
        });
    }

    [Test]
    public async Task ChallengeAsync_advertisedApiResourceIdentifier_isAdmitted()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = Acquirer();

        await CreateMethod(acquirer).ChallengeAsync(Context("api://state-api", clock));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "api://state-api/.default" }));
    }

    [Test]
    public async Task ChallengeAsync_advertisedHttpsAudienceOnTheEndpointHost_isAdmitted()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = Acquirer();

        await CreateMethod(acquirer)
            .ChallengeAsync(Context("https://state.contoso.example/api", clock, "https://state.contoso.example:8080"));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "https://state.contoso.example/api/.default" }));
    }

    [Test]
    public void ChallengeAsync_advertisedHttpsAudienceOnAForeignHost_isRefused()
    {
        var clock = new MutableTimeProvider(Start);

        Assert.That(
            async () => await CreateMethod(Acquirer())
                .ChallengeAsync(Context("https://evil.example/api", clock, "https://state.contoso.example")),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task ChallengeAsync_configuredScopes_bypassTheAdvertisedAudienceEntirely()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = Acquirer();
        var options = new ExplorerEntraWebOptions();
        options.Scopes.Add("api://state-api/.default");

        await CreateMethod(acquirer, options).ChallengeAsync(Context("https://graph.microsoft.com", clock));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "api://state-api/.default" }));
    }

    [Test]
    public async Task ChallengeAsync_operatorAllowedAudience_isAdmitted()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = Acquirer();
        var options = new ExplorerEntraWebOptions();
        options.AllowedAudiences.Add("https://state.contoso.example/api");

        await CreateMethod(acquirer, options).ChallengeAsync(Context("https://state.contoso.example/api", clock));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "https://state.contoso.example/api/.default" }));
    }

    [Test]
    public void ChallengeAsync_allowedAudiencesConfigured_refusesEveryOtherAudienceIncludingApiResources()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = Acquirer();
        var options = new ExplorerEntraWebOptions();
        options.AllowedAudiences.Add("api://state-api");

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await CreateMethod(acquirer, options).ChallengeAsync(Context("api://other-api", clock)),
                Throws.InvalidOperationException.With.Message.Contains("AllowedAudiences"));
            Assert.That(acquirer.CallCount, Is.Zero);
        });
    }
}
