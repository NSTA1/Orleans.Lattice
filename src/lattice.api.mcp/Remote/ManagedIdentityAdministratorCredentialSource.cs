using Azure.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A managed-identity-backed <see cref="ILatticeApiMcpAdministratorCredentialSource"/>
/// that keeps the remote-host discovery core supplied with a valid administrator
/// introspection token. It acquires an Entra access token for the configured
/// silo-audience scope from an Azure <see cref="TokenCredential"/>, caches it, and
/// refreshes it a configurable skew before expiry, so a long-lived MCP server no
/// longer loses its introspection capability when a static token expires.
/// </summary>
/// <remarks>
/// <para>
/// Resolution is synchronous because the credential-forwarding interceptor stamps
/// the outbound header on the synchronous gRPC path; the source therefore uses the
/// <see cref="TokenCredential"/> synchronous acquisition and its own cache rather
/// than blocking on an async call. A concurrent burst is coalesced under a lock so
/// a rotation triggers at most one acquisition.
/// </para>
/// <para>
/// <b>Fail-closed.</b> When acquisition throws, the source logs and returns
/// <see langword="null"/> so the introspection call falls through to anonymous and
/// the remote cluster denies it (discovery then advertises no tools until the next
/// acquisition succeeds), rather than forwarding a stale or fabricated credential.
/// </para>
/// </remarks>
internal sealed class ManagedIdentityAdministratorCredentialSource
    : ILatticeApiMcpAdministratorCredentialSource
{
    private readonly IOptions<LatticeApiMcpManagedIdentityAdministratorOptions> _options;
    private readonly ILogger<ManagedIdentityAdministratorCredentialSource> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly object _gate = new();
    private CachedToken? _cached;

    /// <summary>
    /// Initialises the source over the bound managed-identity options, a logger for
    /// the fail-closed diagnostic path, and an optional <paramref name="timeProvider"/>
    /// used only to decide when a cached token is due for refresh (defaults to
    /// <see cref="TimeProvider.System"/>).
    /// </summary>
    public ManagedIdentityAdministratorCredentialSource(
        IOptions<LatticeApiMcpManagedIdentityAdministratorOptions> options,
        ILogger<ManagedIdentityAdministratorCredentialSource> logger,
        TimeProvider? timeProvider = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public LatticeCredential? Resolve()
    {
        var options = _options.Value;

        // Lock-free fast path: a still-fresh cached token is served without taking
        // the gate. The cache is a single immutable reference, so this read can
        // never observe a torn token.
        if (TryGetFresh(options, Volatile.Read(ref _cached), out var token))
        {
            return new LatticeCredential(token);
        }

        lock (_gate)
        {
            // Re-check under the gate: a concurrent caller may have refreshed while
            // this one waited, so the acquisition happens at most once per rotation.
            if (TryGetFresh(options, _cached, out token))
            {
                return new LatticeCredential(token);
            }

            var credential = options.Credential;
            if (credential is null)
            {
                _logger.LogWarning(
                    "No managed-identity administrator credential is configured; MCP remote discovery "
                    + "introspection fails closed.");
                return null;
            }

            try
            {
                var context = new TokenRequestContext([options.Scope]);
                var acquired = credential.GetToken(context, CancellationToken.None);
                Volatile.Write(ref _cached, new CachedToken(acquired.Token, acquired.ExpiresOn));
                return new LatticeCredential(acquired.Token);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Acquiring the managed-identity administrator token failed; MCP remote discovery "
                    + "introspection fails closed until the next acquisition succeeds.");
                return null;
            }
        }
    }

    private bool TryGetFresh(
        LatticeApiMcpManagedIdentityAdministratorOptions options,
        CachedToken? cached,
        out string token)
    {
        if (cached is not null
            && _timeProvider.GetUtcNow() < cached.ExpiresOn - options.RefreshSkew)
        {
            token = cached.Token;
            return true;
        }

        token = string.Empty;
        return false;
    }

    private sealed record CachedToken(string Token, DateTimeOffset ExpiresOn);
}
