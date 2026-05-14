using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationSecretProvider"/>. Caches the result
/// of <see cref="ILatticeReplicationSecretSource.GetAcceptedSecretsAsync"/>
/// for the duration of
/// <see cref="LatticeReplicationSecurityOptions.SecretRefreshInterval"/>
/// so that a remote secret store is hit at most once per interval per
/// silo. Outbound-secret resolution is cached per peer with the same
/// interval; per-peer entries expire independently.
/// </summary>
/// <remarks>
/// The cache is intentionally simple: a single accepted-set snapshot
/// shared across every inbound check, and a ConcurrentDictionary keyed
/// by peer id for outbound resolution. The provider never serves a
/// stale entry past the refresh interval - if the underlying source
/// throws, the cache surfaces the exception rather than continuing to
/// serve the previous value, so an outage in the secret store is
/// visible (fail-closed) rather than masked.
/// </remarks>
internal sealed class CachingReplicationSecretProvider(
    ILatticeReplicationSecretSource source,
    IOptionsMonitor<LatticeReplicationSecurityOptions> options,
    TimeProvider time) : IReplicationSecretProvider
{
    private readonly ILatticeReplicationSecretSource _source = source ?? throw new ArgumentNullException(nameof(source));
    private readonly IOptionsMonitor<LatticeReplicationSecurityOptions> _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly TimeProvider _time = time ?? throw new ArgumentNullException(nameof(time));

    private readonly SemaphoreSlim _acceptedGate = new(initialCount: 1, maxCount: 1);
    private LatticeReplicationAcceptedSecrets? _acceptedSnapshot;
    private long _acceptedExpiresAtTicks;

    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, OutboundEntry> _outbound = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(peerClusterId);

        var nowTicks = _time.GetUtcNow().UtcTicks;
        if (_outbound.TryGetValue(peerClusterId, out var entry) && entry.ExpiresAtTicks > nowTicks)
        {
            return entry.Value;
        }

        var fresh = await _source.GetOutboundSecretAsync(peerClusterId, cancellationToken).ConfigureAwait(false);
        var refresh = _options.CurrentValue.SecretRefreshInterval;
        var newEntry = new OutboundEntry(fresh, nowTicks + refresh.Ticks);
        _outbound[peerClusterId] = newEntry;
        return fresh;
    }

    /// <inheritdoc />
    public async ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
    {
        var nowTicks = _time.GetUtcNow().UtcTicks;
        var snapshot = _acceptedSnapshot;
        if (snapshot is not null && Volatile.Read(ref _acceptedExpiresAtTicks) > nowTicks)
        {
            return snapshot;
        }

        await _acceptedGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            nowTicks = _time.GetUtcNow().UtcTicks;
            snapshot = _acceptedSnapshot;
            if (snapshot is not null && _acceptedExpiresAtTicks > nowTicks)
            {
                return snapshot;
            }

            var fresh = await _source.GetAcceptedSecretsAsync(cancellationToken).ConfigureAwait(false);
            var refresh = _options.CurrentValue.SecretRefreshInterval;
            _acceptedSnapshot = fresh;
            Volatile.Write(ref _acceptedExpiresAtTicks, nowTicks + refresh.Ticks);
            return fresh;
        }
        finally
        {
            _acceptedGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsAcceptedAsync(string? presented, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(presented))
        {
            return false;
        }

        var snapshot = await GetAcceptedSecretsAsync(cancellationToken).ConfigureAwait(false);
        // Walk the full set even after a match so the comparison time
        // does not leak which entry matched. FixedTimeEquals already
        // protects per-comparison; the outer loop completion protects
        // the set-position signal.
        var matched = false;
        for (var i = 0; i < snapshot.Secrets.Count; i++)
        {
            matched |= LatticeReplicationSharedSecret.FixedTimeEquals(presented, snapshot.Secrets[i]);
        }
        return matched;
    }

    private readonly record struct OutboundEntry(string? Value, long ExpiresAtTicks);
}
