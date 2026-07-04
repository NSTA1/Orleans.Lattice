namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// Caches the app-only Graph access token and refreshes it transparently before
/// expiry, sharing a single in-flight acquisition across concurrent lookups. A
/// cold cache therefore triggers exactly one call to the underlying
/// <see cref="IEntraGraphTokenAcquirer"/> no matter how many callers request a
/// token at once, and an expired token is re-acquired on the next request without
/// operator involvement.
/// </summary>
internal sealed class EntraGraphTokenProvider
{
    private readonly IEntraGraphTokenAcquirer _acquirer;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _refreshSkew;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile CachedToken? _cached;

    /// <summary>
    /// Initializes a new <see cref="EntraGraphTokenProvider"/>.
    /// </summary>
    /// <param name="acquirer">The token acquisition seam. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The clock used to judge token freshness. Must not be <c>null</c>.</param>
    /// <param name="refreshSkew">How long before expiry a token is considered stale. Must not be negative.</param>
    public EntraGraphTokenProvider(IEntraGraphTokenAcquirer acquirer, TimeProvider timeProvider, TimeSpan refreshSkew)
    {
        ArgumentNullException.ThrowIfNull(acquirer);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThan(refreshSkew, TimeSpan.Zero);
        _acquirer = acquirer;
        _timeProvider = timeProvider;
        _refreshSkew = refreshSkew;
    }

    /// <summary>
    /// Returns a valid app-only access token, acquiring or refreshing it only when
    /// the cached token is missing or within the refresh skew of expiry.
    /// </summary>
    /// <param name="cancellationToken">Cancels a pending acquisition.</param>
    public async ValueTask<string> GetAccessTokenAsync(CancellationToken cancellationToken = default)
    {
        if (TryGetFresh(out var token))
        {
            return token;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Another waiter may have refreshed while this call waited on the gate.
            if (TryGetFresh(out token))
            {
                return token;
            }

            var acquired = await _acquirer.AcquireAsync(cancellationToken).ConfigureAwait(false);
            _cached = new CachedToken(acquired.AccessToken, acquired.ExpiresOn);
            return acquired.AccessToken;
        }
        finally
        {
            _gate.Release();
        }
    }

    private bool TryGetFresh(out string token)
    {
        var cached = _cached;
        if (cached is not null && cached.ExpiresOn - _refreshSkew > _timeProvider.GetUtcNow())
        {
            token = cached.AccessToken;
            return true;
        }

        token = string.Empty;
        return false;
    }

    private sealed class CachedToken(string accessToken, DateTimeOffset expiresOn)
    {
        public string AccessToken { get; } = accessToken;

        public DateTimeOffset ExpiresOn { get; } = expiresOn;
    }
}
