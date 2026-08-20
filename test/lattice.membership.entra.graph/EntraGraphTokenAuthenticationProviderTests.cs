using Microsoft.Kiota.Abstractions;

namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="EntraGraphTokenAuthenticationProvider"/>: the Kiota
/// authentication provider that stamps each Graph request with the shared app-only
/// bearer token. It must consult the shared <see cref="EntraGraphTokenProvider"/>
/// (so every request rides the cached, single-flight token) and reject a
/// <c>null</c> request. No live MSAL / Graph call is made.
/// </summary>
public class EntraGraphTokenAuthenticationProviderTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static EntraGraphTokenProvider CreateTokenProvider(out FakeGraphTokenAcquirer acquirer)
    {
        var clock = new ManualTimeProvider(Start);
        acquirer = new FakeGraphTokenAcquirer(clock, TimeSpan.FromHours(1));
        return new EntraGraphTokenProvider(acquirer, clock, TimeSpan.FromMinutes(5));
    }

    [Test]
    public void Constructor_null_token_provider_throws()
    {
        Assert.That(
            () => new EntraGraphTokenAuthenticationProvider(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task AuthenticateRequestAsync_consults_token_provider_and_sets_authorization_header()
    {
        var tokenProvider = CreateTokenProvider(out var acquirer);
        var provider = new EntraGraphTokenAuthenticationProvider(tokenProvider);
        var request = new RequestInformation
        {
            HttpMethod = Method.GET,
            URI = new Uri("https://graph.microsoft.com/v1.0/users"),
        };

        await provider.AuthenticateRequestAsync(request);

        Assert.Multiple(() =>
        {
            // The shared token provider was consulted exactly once (cold cache).
            Assert.That(acquirer.CallCount, Is.EqualTo(1));
            Assert.That(request.Headers.ContainsKey("Authorization"), Is.True);
        });
    }

    [Test]
    public async Task AuthenticateRequestAsync_replaces_a_preexisting_authorization_header()
    {
        var tokenProvider = CreateTokenProvider(out _);
        var provider = new EntraGraphTokenAuthenticationProvider(tokenProvider);
        var request = new RequestInformation
        {
            HttpMethod = Method.GET,
            URI = new Uri("https://graph.microsoft.com/v1.0/users"),
        };
        request.Headers.Add("Authorization", "stale");

        await provider.AuthenticateRequestAsync(request);

        // The provider removes any prior value before adding its own, so the header
        // carries a single value rather than accumulating.
        Assert.That(request.Headers["Authorization"], Has.Count.EqualTo(1));
        Assert.That(request.Headers["Authorization"], Does.Not.Contain("stale"));
    }

    [Test]
    public void AuthenticateRequestAsync_null_request_throws()
    {
        var tokenProvider = CreateTokenProvider(out _);
        var provider = new EntraGraphTokenAuthenticationProvider(tokenProvider);

        Assert.That(
            async () => await provider.AuthenticateRequestAsync(null!),
            Throws.ArgumentNullException);
    }
}
