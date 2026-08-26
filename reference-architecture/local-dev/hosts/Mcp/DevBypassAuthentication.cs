using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.ReferenceArchitecture.Mcp;

/// <summary>
/// Development-only authentication handler for the local-dev dual-cluster harness.
/// When Entra is disabled (<c>Entra:Enabled=false</c>) and the operator opts in
/// with <c>Mcp:DevPerRequestIdentity=true</c>, each request is authenticated as the
/// subject named in its OWN <c>Authorization: Bearer &lt;id&gt;</c> header. That is
/// what lets an agent act as any identity by simply changing the bearer token on
/// the call - no Entra tenant, no per-identity client registration. The resolved
/// id is forwarded to the silo by the credential bridge, where the silo's own dev
/// authenticator maps it to a subject and its groups and the deny-by-default access
/// model enforces on it.
/// </summary>
/// <remarks>
/// <para>
/// The handler is fail-closed: a request with no bearer token is NOT authenticated
/// (it reads as anonymous), so fail-closed tool discovery advertises zero tools to
/// it. There is no fixed fallback subject - the identity always comes from the
/// request - so the head cannot silently serve one caller's tools to another.
/// </para>
/// <para>
/// This handler NEVER activates in an Entra deployment: the wiring in
/// <c>Program.cs</c> forces the opt-in flag off whenever <c>Entra:Enabled=true</c>,
/// so it cannot weaken a real deployment even if the flag is left set. Trusting a
/// bearer token as a subject verbatim is safe only because this is a throwaway
/// local harness with no real secrets and enforcement is real on the silo.
/// </para>
/// </remarks>
internal sealed class DevBypassAuthenticationHandler(
    IOptionsMonitor<DevBypassAuthenticationOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder)
    : AuthenticationHandler<DevBypassAuthenticationOptions>(options, logger, encoder)
{
    /// <summary>The scheme name registered for the dev per-request identity.</summary>
    public const string SchemeName = "LocalDevBypass";

    private const string BearerPrefix = "Bearer ";

    /// <inheritdoc />
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        // Fail-closed: authenticate only a request that carries a bearer token, as
        // the subject that token names. No bearer => anonymous => zero tools.
        string? authorization = Request.Headers.Authorization;
        if (string.IsNullOrWhiteSpace(authorization)
            || !authorization.StartsWith(BearerPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var subjectId = authorization[BearerPrefix.Length..].Trim();
        if (string.IsNullOrWhiteSpace(subjectId))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var identity = new ClaimsIdentity(
            [
                // The credential bridge resolves the principal id from the "oid"
                // claim first, then NameIdentifier; supply both so the forwarded
                // principal id matches the forwarded bearer token exactly.
                new Claim("oid", subjectId),
                new Claim(ClaimTypes.NameIdentifier, subjectId),
                new Claim(ClaimTypes.Name, subjectId),
            ],
            SchemeName);

        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, SchemeName);
        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}

/// <summary>Options for the local dev per-request identity authentication scheme.</summary>
internal sealed class DevBypassAuthenticationOptions : AuthenticationSchemeOptions
{
}

/// <summary>Registration helpers for the local dev per-request identity authentication scheme.</summary>
internal static class DevBypassAuthenticationExtensions
{
    /// <summary>
    /// Registers the dev per-request identity scheme as the default authentication
    /// scheme so <c>UseAuthentication</c> stamps the bearer-named subject on
    /// <c>HttpContext.User</c> for each request.
    /// </summary>
    public static AuthenticationBuilder AddLocalDevPerRequestIdentity(this IServiceCollection services)
    {
        return services
            .AddAuthentication(DevBypassAuthenticationHandler.SchemeName)
            .AddScheme<DevBypassAuthenticationOptions, DevBypassAuthenticationHandler>(
                DevBypassAuthenticationHandler.SchemeName,
                _ => { });
    }
}
