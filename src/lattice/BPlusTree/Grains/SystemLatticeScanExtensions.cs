using System.Runtime.CompilerServices;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal resilient streaming-scan wrappers for <see cref="ISystemLattice"/>,
/// the unguarded internal surface the library uses against reserved system
/// trees. Mirrors <see cref="LatticeExtensions.ScanEntriesAsync(ILattice, string?, string?, bool, bool?, int?, CancellationToken)"/>
/// for the public <see cref="ILattice"/> surface: the raw
/// <see cref="ISystemLattice.EntriesAsync"/> primitive has no
/// <c>Orleans.Runtime.EnumerationAbortedException</c> recovery, whereas this
/// wrapper transparently reconnects (resuming from the successor of the last
/// yielded key, so no duplicates and no gaps) when the remote enumerator is
/// reclaimed mid-scan by grain deactivation, idle expiry, silo failover, or
/// scale-down. It is the recommended surface for long-running internal scans of
/// system trees (for example a queue's cold-start backlog scan); the raw
/// primitive is retained for short, single-page reads.
/// </summary>
internal static class SystemLatticeScanExtensions
{
    /// <summary>
    /// Resilient forward/reverse entry scan over an <see cref="ISystemLattice"/>
    /// system tree. Wraps <see cref="ISystemLattice.EntriesAsync"/> with the same
    /// <c>EnumerationAbortedException</c> recovery and deterministic resume
    /// semantics as <see cref="LatticeExtensions.ScanEntriesAsync(ILattice, string?, string?, bool, bool?, int?, CancellationToken)"/>.
    /// </summary>
    /// <param name="tree">The system tree to scan.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the tree's lowest key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the tree's end.</param>
    /// <param name="reverse">If <c>true</c>, yields entries in descending key order.</param>
    /// <param name="prefetch">Optional per-call override for shard prefetch.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsync(
        this ISystemLattice tree,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        // Eager argument validation. An async-iterator core defers any throw
        // until first MoveNextAsync, so the null-guard lives in this non-async
        // wrapper to surface synchronously the moment a caller invokes
        // ScanEntriesAsync(...).
        ArgumentNullException.ThrowIfNull(tree);
        return ScanEntriesAsyncCore(tree, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsyncCore(
        ISystemLattice tree,
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        int? maxAttempts,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var budget = maxAttempts ?? LatticeExtensions.DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        // See LatticeExtensions.ScanEntriesAsyncCore: Orleans resets the
        // caller-established RequestContext in this iterator's execution flow
        // after the first physical segment completes, so a caller-established
        // system-origin scope or credential scope must be re-asserted around
        // every reopen. System-tree scans run under EnterSystemOrigin; capturing
        // both here keeps the wrapper robust and identical to the ILattice path.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
        var reassertCredential = LatticeCredentialContext.Current;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
            using var credentialScope = reassertCredential is { } entryCredential
                ? LatticeCredentialContext.With(entryCredential)
                : null;
            var enumerator = tree.EntriesAsync(s, e, reverse, prefetch, cancellationToken).GetAsyncEnumerator(cancellationToken);
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
    /// Computes the resume bounds for a resilient scan given the last successfully
    /// yielded key. Forward scans tighten the lower bound to the successor of
    /// <paramref name="lastKey"/> (<c>lastKey + "\u0000"</c>); reverse scans
    /// tighten the upper bound to <paramref name="lastKey"/> (exclusive).
    /// </summary>
    private static (string? Start, string? End) ComputeScanBounds(
        string? originalStart, string? originalEnd, string? lastKey, bool reverse)
    {
        if (lastKey is null)
        {
            return (originalStart, originalEnd);
        }

        return reverse
            ? (originalStart, lastKey)
            : (lastKey + "\u0000", originalEnd);
    }

    /// <summary>
    /// Computes the inter-reconnect backoff, matching
    /// <see cref="ILattice"/>'s wrappers: the first reconnect is immediate and
    /// subsequent attempts apply a small linear ramp capped at 100&#160;ms.
    /// </summary>
    private static int ComputeReconnectDelayMs(int attempt) =>
        attempt <= 1 ? 0 : Math.Min(100, 10 * attempt);
}
