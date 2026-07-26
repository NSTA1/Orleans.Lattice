using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.ReferenceArchitecture.Mcp;

/// <summary>
/// Development-only authentication handler for the local compose harness. When
/// Entra is disabled (<c>Entra:Enabled=false</c>) and the operator opts in with
/// <c>Mcp:DevAuthenticateAll=true</c>, every request is authenticated as a single
/// fixed synthetic subject. This exists solely so the MCP head surfaces its tools
/// without a real identity provider: tool discovery is fail-closed on the caller's
/// resolved credential, so an anonymous local head advertises zero tools. Stamping
/// a synthetic subject that the silo has seeded as a bootstrap administrator lights
/// up the full tool set for local exploration.
/// </summary>
/// <remarks>
/// This handler NEVER activates in an Entra deployment: the wiring in
/// <c>Program.cs</c> forces the opt-in flag off whenever <c>Entra:Enabled=true</c>,
/// so it cannot weaken a real deployment even if the flag is left set.
/// </remarks>
internal sealed class DevBypassAuthenticationHandler(
    IOptionsMonitor<DevBypassAuthenticationOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder)
    : AuthenticationHandler<DevBypassAuthenticationOptions>(options, logger, encoder)
{
    /// <summary>The scheme name registered for the dev bypass.</summary>
    public const string SchemeName = "LocalDevBypass";

    /// <summary>
    /// Default synthetic subject id used when <c>Mcp:DevSubjectId</c> is unset.
    /// Must match a <c>Auth:BootstrapAdministrators</c> entry on the silo so the
    /// subject carries a seeded full-access grant (MCP discovery advertises tools
    /// only against authored rules).
    /// </summary>
    public const string DefaultSubjectId = "local-dev-admin";

    /// <inheritdoc />
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var subjectId = string.IsNullOrWhiteSpace(Options.SubjectId)
            ? DefaultSubjectId
            : Options.SubjectId;

        var identity = new ClaimsIdentity(
            [
                // The credential bridge resolves the principal id from the "oid"
                // claim first, then NameIdentifier; supply both so it maps to the
                // seeded bootstrap-administrator subject regardless of order.
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

/// <summary>Options for the local dev-bypass authentication scheme.</summary>
internal sealed class DevBypassAuthenticationOptions : AuthenticationSchemeOptions
{
    /// <summary>The synthetic subject id stamped on every authenticated request.</summary>
    public string SubjectId { get; set; } = DevBypassAuthenticationHandler.DefaultSubjectId;
}

/// <summary>Registration helpers for the local dev-bypass authentication scheme.</summary>
internal static class DevBypassAuthenticationExtensions
{
    /// <summary>
    /// Registers the dev-bypass scheme as the default authentication scheme so
    /// <c>UseAuthentication</c> stamps the synthetic subject on <c>HttpContext.User</c>.
    /// </summary>
    public static AuthenticationBuilder AddLocalDevBypass(
        this IServiceCollection services,
        string subjectId)
    {
        return services
            .AddAuthentication(DevBypassAuthenticationHandler.SchemeName)
            .AddScheme<DevBypassAuthenticationOptions, DevBypassAuthenticationHandler>(
                DevBypassAuthenticationHandler.SchemeName,
                options => options.SubjectId = subjectId);
    }
}
