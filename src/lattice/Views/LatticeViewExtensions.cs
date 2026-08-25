using System.Runtime.CompilerServices;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for <see cref="ILatticeView"/> that add resilient
/// streaming scans over the view surface, mirroring the
/// <see cref="LatticeExtensions.ScanKeysAsync"/> /
/// <see cref="LatticeExtensions.ScanEntriesAsync"/> wrappers for
/// <see cref="ILattice"/>. The raw <see cref="ILatticeView.KeysAsync"/> /
/// <see cref="ILatticeView.EntriesAsync"/> primitives have no
/// <c>EnumerationAbortedException</c> recovery; these wrappers do, so they are
/// the recommended surface for long-running view scans.
/// </summary>
public static class LatticeViewExtensions
{
    /// <summary>
    /// Resilient forward key scan over a view. Wraps
    /// <see cref="ILatticeView.KeysAsync"/> and transparently recovers from
    /// <c>Orleans.Runtime.EnumerationAbortedException</c> (raised when the remote
    /// enumerator on the view's active tree is reclaimed mid-scan by grain
    /// deactivation, idle expiry, silo failover, or a rebuild's shadow-swap).
    /// The wrapper tracks the last yielded key and - on abort - reopens the scan
    /// with the lower bound tightened to the successor of that key
    /// (<c>lastKey + "\u0000"</c>), so the result stream is deterministic: no
    /// duplicates, no gaps, original lexicographic ordering preserved. The view's
    /// reserved-floor / range semantics are honoured because the first segment
    /// passes <paramref name="startInclusive"/> through unchanged (the view
    /// applies its own reserved floor when it is <c>null</c>) and every resume
    /// bound is a real yielded key that already sits above that floor.
    /// <para>
    /// The first reconnect is immediate; subsequent attempts apply a small linear
    /// backoff (10&#160;ms × attempt, capped at 100&#160;ms) to avoid a tight loop
    /// against a persistently-faulting activation. If the reconnect budget is
    /// exhausted the last <c>EnumerationAbortedException</c> is rethrown verbatim.
    /// This is the recommended client API for long-running view key scans -
    /// <see cref="ILatticeView.KeysAsync"/> is retained for short, single-page
    /// reads and for deliberate low-level use.
    /// </para>
    /// </summary>
    /// <param name="view">The view to scan.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the view's lowest (non-reserved) key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the view's end.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static IAsyncEnumerable<string> ScanKeysAsync(
        this ILatticeView view,
        string? startInclusive = null,
        string? endExclusive = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        // Eager argument validation. An async-iterator core defers any throw until
        // first MoveNextAsync, so the null-guard lives in this non-async wrapper to
        // surface synchronously the moment a caller invokes ScanKeysAsync(...).
        ArgumentNullException.ThrowIfNull(view);
        return ScanKeysAsyncCore(view, startInclusive, endExclusive, maxAttempts, cancellationToken);
    }

    private static async IAsyncEnumerable<string> ScanKeysAsyncCore(
        ILatticeView view,
        string? startInclusive,
        string? endExclusive,
        int? maxAttempts,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var budget = maxAttempts ?? LatticeExtensions.DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        // See LatticeExtensions.ScanKeysAsyncCore: a caller-established
        // system-origin scope OR credential scope is reset by Orleans in this
        // iterator's execution flow after the first physical segment completes, so
        // each must be re-asserted around every reopen or a resumed segment
        // resolves to an anonymous subject and a fail-closed gate silently
        // truncates the scan.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
        var reassertCredential = LatticeCredentialContext.Current;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeForwardScanBounds(startInclusive, endExclusive, lastKey);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
            using var credentialScope = reassertCredential is { } entryCredential
                ? LatticeCredentialContext.With(entryCredential)
                : null;
            var enumerator = view.KeysAsync(s, e, cancellationToken).GetAsyncEnumerator(cancellationToken);
            var completedNormally = false;
            var shouldReopen = false;
            try
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (EnumerationAbortedException) when (attempt < budget)
                    {
                        attempt++;
                        shouldReopen = true;
                        break;
                    }

                    if (!hasNext)
                    {
                        completedNormally = true;
                        break;
                    }

                    lastKey = enumerator.Current;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (completedNormally)
            {
                yield break;
            }

            if (shouldReopen)
            {
                var delayMs = ComputeReconnectDelayMs(attempt);
                if (delayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Resilient forward entry scan over a view. Wraps
    /// <see cref="ILatticeView.EntriesAsync"/> with the same
    /// <c>EnumerationAbortedException</c> recovery and deterministic resume
    /// semantics as <see cref="ScanKeysAsync"/>. This is the recommended client
    /// API for long-running view entry exports.
    /// </summary>
    /// <param name="view">The view to scan.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the view's lowest (non-reserved) key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the view's end.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsync(
        this ILatticeView view,
        string? startInclusive = null,
        string? endExclusive = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        // See ScanKeysAsync for why the null-guard lives in a non-async wrapper.
        ArgumentNullException.ThrowIfNull(view);
        return ScanEntriesAsyncCore(view, startInclusive, endExclusive, maxAttempts, cancellationToken);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsyncCore(
        ILatticeView view,
        string? startInclusive,
        string? endExclusive,
        int? maxAttempts,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var budget = maxAttempts ?? LatticeExtensions.DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        // See ScanKeysAsyncCore: re-assert the caller's system-origin and
        // credential scopes around every reopen so no resumed segment resolves to
        // an anonymous subject.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
        var reassertCredential = LatticeCredentialContext.Current;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeForwardScanBounds(startInclusive, endExclusive, lastKey);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
            using var credentialScope = reassertCredential is { } entryCredential
                ? LatticeCredentialContext.With(entryCredential)
                : null;
            var enumerator = view.EntriesAsync(s, e, cancellationToken).GetAsyncEnumerator(cancellationToken);
            var completedNormally = false;
            var shouldReopen = false;
            try
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (EnumerationAbortedException) when (attempt < budget)
                    {
                        attempt++;
                        shouldReopen = true;
                        break;
                    }

                    if (!hasNext)
                    {
                        completedNormally = true;
                        break;
                    }

                    lastKey = enumerator.Current.Key;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (completedNormally)
            {
                yield break;
            }

            if (shouldReopen)
            {
                var delayMs = ComputeReconnectDelayMs(attempt);
                if (delayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Computes the resume bounds for a resilient forward view scan given the last
    /// successfully yielded key. The lower bound is tightened to the successor of
    /// <paramref name="lastKey"/> (<c>lastKey + "\u0000"</c>); the upper bound is
    /// left unchanged. The view surface enumerates forward only, so there is no
    /// reverse variant.
    /// </summary>
    private static (string? Start, string? End) ComputeForwardScanBounds(
        string? originalStart, string? originalEnd, string? lastKey) =>
        lastKey is null
            ? (originalStart, originalEnd)
            : (lastKey + "\u0000", originalEnd);

    /// <summary>
    /// Computes the inter-reconnect backoff for a resilient view scan, matching
    /// <see cref="ILattice"/>'s wrappers: the first reconnect is immediate and
    /// subsequent attempts apply a small linear ramp capped at 100&#160;ms.
    /// </summary>
    private static int ComputeReconnectDelayMs(int attempt) =>
        attempt <= 1 ? 0 : Math.Min(100, 10 * attempt);
}
