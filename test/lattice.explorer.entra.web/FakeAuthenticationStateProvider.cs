using System.Security.Claims;
using Microsoft.AspNetCore.Components.Authorization;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// A controllable <see cref="AuthenticationStateProvider"/> for tests: it returns
/// whatever <see cref="ClaimsPrincipal"/> it is seeded with, so the acquirer and
/// circuit handler can be driven with an authenticated or anonymous user without
/// a real OpenID Connect flow.
/// </summary>
internal sealed class FakeAuthenticationStateProvider(ClaimsPrincipal user) : AuthenticationStateProvider
{
    public override Task<AuthenticationState> GetAuthenticationStateAsync()
        => Task.FromResult(new AuthenticationState(user));

    /// <summary>An authenticated principal with the given <paramref name="name"/>.</summary>
    public static ClaimsPrincipal Authenticated(string name)
        => new(new ClaimsIdentity(new[] { new Claim(ClaimTypes.Name, name) }, authenticationType: "Test"));

    /// <summary>An anonymous (unauthenticated) principal.</summary>
    public static ClaimsPrincipal Anonymous() => new(new ClaimsIdentity());
}
