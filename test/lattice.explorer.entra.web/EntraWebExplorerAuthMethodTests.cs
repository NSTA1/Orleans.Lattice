using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for <see cref="EntraWebExplorerAuthMethod"/>: scheme handling,
/// scope resolution from the advertised audience or static options, the initial
/// challenge, silent renewal, and the re-challenge path when the acquirer
/// signals that interactive sign-in is required. A fake acquirer and a
/// controllable clock keep every case deterministic and network-free.
/// </summary>
[TestFixture]
public sealed class EntraWebExplorerAuthMethodTests
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

    private static ExplorerAuthChallengeContext ContextWithAudience(string? audience, TimeProvider clock)
    {
        var parameters = new Dictionary<string, string>(StringComparer.Ordinal);
        if (audience is not null)
        {
            parameters[ExplorerAuthSchemes.AudienceParameter] = audience;
        }

        return new ExplorerAuthChallengeContext
        {
            SchemeId = ExplorerAuthSchemes.Entra,
            Parameters = parameters,
            TimeProvider = clock,
        };
    }

    [Test]
    public void SchemeId_is_entra()
    {
        Assert.That(CreateMethod(new FakeWebTokenAcquirer()).SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
    }

    [Test]
    public void CanHandle_matches_entra_case_insensitively()
    {
        var method = CreateMethod(new FakeWebTokenAcquirer());

        Assert.Multiple(() =>
        {
            Assert.That(method.CanHandle("entra"), Is.True);
            Assert.That(method.CanHandle("ENTRA"), Is.True);
            Assert.That(method.CanHandle("basic"), Is.False);
            Assert.That(method.CanHandle("oidc"), Is.False);
        });
    }

    [Test]
    public async Task ChallengeAsync_acquires_a_token_and_returns_a_bearer_credential()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer().EnqueueToken(new ExplorerWebToken
        {
            AccessToken = "tok1",
            ExpiresOn = Start.AddHours(1),
            Username = "alice@contoso.com",
        });
        var method = CreateMethod(acquirer);

        var signIn = await method.ChallengeAsync(ContextWithAudience("api://resource", clock));

        Assert.Multiple(async () =>
        {
            Assert.That(signIn.SchemeId, Is.EqualTo(ExplorerAuthSchemes.Entra));
            Assert.That(signIn.DisplayName, Is.EqualTo("alice@contoso.com"));
            Assert.That(signIn.Authentication.HasCredentialProvider, Is.True);
            var header = await signIn.Authentication.CredentialProvider!.GetAuthorizationHeaderAsync();
            Assert.That(header, Is.EqualTo("Bearer tok1"));
        });
    }

    [Test]
    public async Task ChallengeAsync_resolves_the_scope_from_the_advertised_audience()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer().EnqueueToken(Token("t", Start.AddHours(1)));

        await CreateMethod(acquirer).ChallengeAsync(ContextWithAudience("api://resource", clock));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "api://resource/.default" }));
    }

    [Test]
    public async Task ChallengeAsync_uses_an_audience_that_already_names_a_scope_verbatim()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer().EnqueueToken(Token("t", Start.AddHours(1)));

        await CreateMethod(acquirer).ChallengeAsync(ContextWithAudience("api://resource/.default", clock));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "api://resource/.default" }));
    }

    [Test]
    public async Task ChallengeAsync_prefers_statically_configured_scopes_over_the_audience()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer().EnqueueToken(Token("t", Start.AddHours(1)));
        var options = new ExplorerEntraWebOptions();
        options.Scopes.Add("api://custom/scope.read");

        await CreateMethod(acquirer, options).ChallengeAsync(ContextWithAudience("api://ignored", clock));

        Assert.That(acquirer.LastScopes, Is.EqualTo(new[] { "api://custom/scope.read" }));
    }

    [Test]
    public void ChallengeAsync_throws_when_no_scope_can_be_resolved()
    {
        var clock = new MutableTimeProvider(Start);
        var method = CreateMethod(new FakeWebTokenAcquirer());

        Assert.ThrowsAsync<InvalidOperationException>(
            () => method.ChallengeAsync(ContextWithAudience(audience: null, clock)));
    }

    [Test]
    public async Task ChallengeAsync_defaults_the_display_name_when_the_username_is_blank()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer().EnqueueToken(new ExplorerWebToken
        {
            AccessToken = "t",
            ExpiresOn = Start.AddHours(1),
            Username = null,
        });

        var signIn = await CreateMethod(acquirer).ChallengeAsync(ContextWithAudience("api://resource", clock));

        Assert.That(signIn.DisplayName, Is.EqualTo("Entra user"));
    }

    [Test]
    public async Task Credential_provider_renews_silently_when_the_token_is_expiring()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer()
            .EnqueueToken(Token("first", Start.AddMinutes(1)))
            .EnqueueToken(Token("second", Start.AddHours(1)));
        var signIn = await CreateMethod(acquirer).ChallengeAsync(ContextWithAudience("api://resource", clock));

        // Advance past the refresh margin so the first token is treated as expiring.
        clock.Advance(TimeSpan.FromMinutes(1));
        var header = await signIn.Authentication.CredentialProvider!.GetAuthorizationHeaderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(header, Is.EqualTo("Bearer second"));
            Assert.That(acquirer.CallCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Credential_provider_returns_null_after_a_required_reauth()
    {
        var clock = new MutableTimeProvider(Start);
        var acquirer = new FakeWebTokenAcquirer()
            .EnqueueToken(Token("first", Start.AddMinutes(1)))
            .EnqueueThrow(new ExplorerWebReauthRequiredException());
        var signIn = await CreateMethod(acquirer).ChallengeAsync(ContextWithAudience("api://resource", clock));

        clock.Advance(TimeSpan.FromMinutes(1));
        var header = await signIn.Authentication.CredentialProvider!.GetAuthorizationHeaderAsync();

        Assert.That(header, Is.Null);
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var monitor = Substitute.For<IOptionsMonitor<ExplorerEntraWebOptions>>();
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new EntraWebExplorerAuthMethod(null!, monitor));
            Assert.Throws<ArgumentNullException>(() => new EntraWebExplorerAuthMethod(new FakeWebTokenAcquirer(), null!));
        });
    }

    private static ExplorerWebToken Token(string value, DateTimeOffset expiresOn)
        => new() { AccessToken = value, ExpiresOn = expiresOn };
}
