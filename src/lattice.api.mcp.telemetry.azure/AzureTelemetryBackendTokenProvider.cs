using Azure.Core;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure;

/// <summary>
/// An <see cref="ITelemetryBackendTokenProvider"/> backed by an Azure
/// <see cref="TokenCredential"/>. Acquires an Entra access token for the
/// configured scope, caches it, and refreshes it a configurable skew before
/// expiry. Concurrent callers share a single in-flight acquisition, so a token
/// rotation never fans out into a burst of duplicate credential calls.
/// </summary>
internal sealed class AzureTelemetryBackendTokenProvider
    : ITelemetryBackendTokenProvider, IDisposable
{
    private readonly IOptions<AzureTelemetryBackendTokenOptions> _options;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private CachedToken? _cached;

    /// <summary>
    /// Creates the provider over the bound <paramref name="options"/> (the
    /// credential, scope, and refresh skew) and an optional
    /// <paramref name="timeProvider"/> used only to decide when a cached token is
    /// due for refresh; defaults to <see cref="TimeProvider.System"/>.
    /// </summary>
    /// <param name="options">The bound Azure token options.</param>
    /// <param name="timeProvider">Clock used for expiry comparisons.</param>
    public AzureTelemetryBackendTokenProvider(
        IOptions<AzureTelemetryBackendTokenOptions> options,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        var options = _options.Value;

        // Lock-free fast path: a still-fresh cached token is served without taking
        // the gate. The cache is a single immutable reference, so this read can
        // never observe a torn token.
        if (TryGetFresh(options, Volatile.Read(ref _cached), out var token))
        {
            return token;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Re-check under the gate: a concurrent caller may have refreshed while
            // this one waited, so the acquisition happens at most once per rotation.
            if (TryGetFresh(options, _cached, out token))
            {
                return token;
            }

            var credential = options.Credential
                ?? throw new InvalidOperationException(
                    $"{nameof(AzureTelemetryBackendTokenOptions)}.{nameof(AzureTelemetryBackendTokenOptions.Credential)} "
                    + "must be supplied before a token can be acquired.");

            var context = new TokenRequestContext([options.Scope]);
            var acquired = await credential.GetTokenAsync(context, cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _cached, new CachedToken(acquired.Token, acquired.ExpiresOn));
            return acquired.Token;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _gate.Dispose();

    private bool TryGetFresh(
        AzureTelemetryBackendTokenOptions options,
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
