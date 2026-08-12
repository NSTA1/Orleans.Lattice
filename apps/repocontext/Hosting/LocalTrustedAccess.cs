using Microsoft.AspNetCore.Http;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Well-known identity constants for the single trusted local agent the box
/// serves. The container's only application listener is the MCP endpoint on a
/// private container network, so every request is the local developer's agent;
/// there is no multi-tenant identity to distinguish.
/// </summary>
public static class LocalTrustedAgent
{
    /// <summary>The subject id the local agent resolves to.</summary>
    public const string SubjectId = "local-agent";

    /// <summary>The bootstrap administrator subject used to seed the access grant.</summary>
    public const string BootstrapAdministrator = "repocontext-bootstrap-admin";

    /// <summary>The credential scheme the local authenticator claims.</summary>
    public const string Scheme = "repocontext-local";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.repocontext.local/";
}

/// <summary>
/// Maps every inbound MCP request onto the fixed
/// <see cref="LocalTrustedAgent.SubjectId"/> credential. Because the container
/// exposes only the MCP endpoint on a private network to a single trusted agent,
/// there is no anonymous surface to fail closed against here - the fail-closed
/// discovery seam still runs underneath, scoped by the local agent's seeded
/// grant.
/// </summary>
public sealed class LocalTrustedCredentialBridge : ILatticeApiMcpCredentialBridge
{
    /// <inheritdoc />
    public LatticeCredential? Resolve(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return new LatticeCredential(
            token: LocalTrustedAgent.SubjectId,
            scheme: LocalTrustedAgent.Scheme,
            principalId: LocalTrustedAgent.SubjectId);
    }
}

/// <summary>
/// Trusts the ambient credential's token as the caller subject id for credentials
/// stamped with <see cref="LocalTrustedAgent.Scheme"/>, so the local agent's tool
/// calls resolve to <see cref="LocalTrustedAgent.SubjectId"/> inside the cluster.
/// It handles only its own scheme, so it never shadows the anonymous authenticator
/// on a system-origin turn.
/// </summary>
public sealed class LocalTrustedAuthenticator : ILatticeCredentialAuthenticator
{
    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential)
        => string.Equals(credential.Scheme, LocalTrustedAgent.Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
        => new(new LatticePrincipal(credential.Token, LocalTrustedAgent.Issuer));
}
