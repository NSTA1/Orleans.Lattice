using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Owns the lifecycle of a single interactive-login access token: it holds the
/// current token, renews it silently before it expires, and hands the composed
/// <c>authorization</c> header to the connection on every call. This is the
/// reusable engine every token-based auth method (Entra, generic OIDC) plugs
/// its silent-renewal delegate into; the method supplies <em>how</em> to acquire
/// a token, this type supplies <em>when</em> and guarantees that acquisition
/// happens at most once across a burst of concurrent calls.
/// </summary>
/// <remarks>
/// <para>
/// Renewal is proactive and clock-skew-aware: a token is treated as expiring
/// once the clock reaches <c>ExpiresOn - RefreshMargin</c>, so a fresh token is
/// obtained before the old one is actually rejected. The margin absorbs both the
/// renewal round-trip and any skew between the client and the token issuer.
/// </para>
/// <para>
/// Concurrency is single-flight: many simultaneous callers that all observe an
/// expiring token queue behind one gate; the first performs the single renewal
/// and the rest observe the freshly-stored token and return without calling the
/// acquire delegate again (no thundering herd on the token endpoint).
/// </para>
/// <para>
/// Token material lives only in memory here. When the acquire delegate reports
/// that it can no longer renew (returns <see langword="null"/>), the source
/// latches into a revoked state and every subsequent request returns
/// <see langword="null"/> / <see langword="false"/> so the user is re-challenged
/// interactively rather than dropped into a broken session.
/// </para>
/// </remarks>
public sealed class ExplorerAccessTokenSource : ILatticeCallCredentialProvider, IDisposable
{
    /// <summary>The default proactive-refresh margin: renew two minutes before expiry.</summary>
    public static readonly TimeSpan DefaultRefreshMargin = TimeSpan.FromMinutes(2);

    private readonly Func<CancellationToken, ValueTask<ExplorerAccessToken?>> _acquire;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _refreshMargin;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private ExplorerAccessToken _current;
    private string? _currentHeader;
    private long _generation;
    private bool _revoked;
    private bool _disposed;

    /// <summary>
    /// Creates a token source seeded with an initial token and the delegate that
    /// silently renews it.
    /// </summary>
    /// <param name="initial">The token acquired by the interactive challenge.</param>
    /// <param name="acquire">
    /// The silent-renewal delegate (for example an MSAL silent acquisition). It
    /// returns a fresh <see cref="ExplorerAccessToken"/>, or <see langword="null"/>
    /// when renewal is no longer possible so the caller must re-challenge.
    /// </param>
    /// <param name="timeProvider">The clock used to test expiry. Must not be <see langword="null"/>.</param>
    /// <param name="refreshMargin">
    /// How long before <see cref="ExplorerAccessToken.ExpiresOn"/> a token is
    /// treated as expiring. Defaults to <see cref="DefaultRefreshMargin"/>. Must
    /// not be negative.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="acquire"/> or <paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="refreshMargin"/> is negative.</exception>
    public ExplorerAccessTokenSource(
        ExplorerAccessToken initial,
        Func<CancellationToken, ValueTask<ExplorerAccessToken?>> acquire,
        TimeProvider timeProvider,
        TimeSpan? refreshMargin = null)
    {
        ArgumentNullException.ThrowIfNull(acquire);
        ArgumentNullException.ThrowIfNull(timeProvider);

        var margin = refreshMargin ?? DefaultRefreshMargin;
        if (margin < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(refreshMargin), margin, "The refresh margin must not be negative.");
        }

        _current = initial;
        _currentHeader = initial.ToAuthorizationHeader();
        _acquire = acquire;
        _timeProvider = timeProvider;
        _refreshMargin = margin;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetAuthorizationHeaderAsync(CancellationToken cancellationToken = default)
    {
        // Fast path: a still-valid token is served without touching the gate, so
        // steady-state calls pay no synchronization cost. The header is composed
        // once when the token is stored (not per call), and a reference read is
        // atomic, so the fast path allocates nothing and cannot tear.
        if (!IsExpiring(_current) && !Volatile.Read(ref _revoked))
        {
            return Volatile.Read(ref _currentHeader);
        }

        var refreshed = await RefreshCoreAsync(force: false, cancellationToken).ConfigureAwait(false);
        return refreshed ? Volatile.Read(ref _currentHeader) : null;
    }

    /// <inheritdoc />
    public ValueTask<bool> RefreshAsync(CancellationToken cancellationToken = default)
        => RefreshCoreAsync(force: true, cancellationToken);

    private async ValueTask<bool> RefreshCoreAsync(bool force, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _revoked))
        {
            return false;
        }

        // Captured before the gate so a caller that queues behind the single
        // in-flight renewal can detect that someone else already renewed and skip
        // its own acquire. This makes even a burst of forced (post-401) refreshes
        // single-flight, not just the proactive expiry path.
        var seenGeneration = Volatile.Read(ref _generation);

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_revoked)
            {
                return false;
            }

            // A caller that queued behind the single in-flight renewal observes the
            // token another caller just stored; unless a forced refresh was asked
            // for, it returns without calling the acquire delegate again.
            if (!force && !IsExpiring(_current))
            {
                return true;
            }

            // Another caller refreshed while we waited on the gate: adopt its token
            // (which is fresh) rather than acquiring a redundant one.
            if (Volatile.Read(ref _generation) != seenGeneration)
            {
                return true;
            }

            var acquired = await _acquire(cancellationToken).ConfigureAwait(false);
            if (acquired is { } token)
            {
                _current = token;
                Volatile.Write(ref _currentHeader, token.ToAuthorizationHeader());
                _generation++;
                return true;
            }

            _revoked = true;
            return false;
        }
        finally
        {
            _gate.Release();
        }
    }

    private bool IsExpiring(ExplorerAccessToken token)
        => _timeProvider.GetUtcNow() >= token.ExpiresOn - _refreshMargin;

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _gate.Dispose();
    }
}
