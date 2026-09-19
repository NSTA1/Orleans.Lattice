using System.Runtime.CompilerServices;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal resilient streaming-scan wrappers for <see cref="ISystemLattice"/>,
/// the unguarded internal surface the library uses against reserved system
/// trees. Mirrors <see cref="LatticeExtensions.ScanKeysAsync(ILattice, string?, string?, bool, bool?, int?, CancellationToken)"/> and
/// <see cref="LatticeExtensions.ScanEntriesAsync(ILattice, string?, string?, bool, bool?, int?, CancellationToken)"/>
/// for the public <see cref="ILattice"/> surface: the raw
/// <see cref="ISystemLattice.KeysAsync"/> / <see cref="ISystemLattice.EntriesAsync"/>
/// primitives have no
/// <c>Orleans.Runtime.EnumerationAbortedException</c> recovery, whereas these
/// wrappers transparently reconnect (resuming from the successor of the last
/// yielded key, so no duplicates and no gaps) when the remote enumerator is
/// reclaimed mid-scan. It is the recommended surface for internal scans of
/// system trees; the raw primitives are retained for short, single-page reads.
/// <para>
/// Five distinct events reclaim a remote enumerator, and the fifth is the one
/// that is easy to miss because Orleans' own abort message does not name it.
/// The familiar four are grain deactivation, enumerator idle expiry, silo
/// failover, and scale-down. The fifth is <b>stateless-worker routing</b>:
/// Orleans' async-enumerable protocol keeps enumerator state in a dictionary on
/// the <em>activation</em> that served <c>StartEnumeration</c>, while every
/// subsequent <c>MoveNext</c> is an independent grain message. A
/// <c>[StatelessWorker]</c> grain has many local activations and its dispatcher
/// picks one per message with no request affinity, so a <c>MoveNext</c> can land
/// on a sibling worker that has never heard of the enumerator and the call
/// aborts. <see cref="LatticeGrain"/> - which backs every system tree - is
/// <c>[StatelessWorker]</c>, so this is a steady-state background rate on any
/// system-tree scan, not a rare failover event, and it rises with concurrency
/// rather than with scan duration.
/// </para>
/// <para>
/// The exposure begins at the first <c>MoveNext</c>, not at the first element.
/// Orleans' server extension returns <c>MissingEnumeratorError</c> from
/// <c>MoveNext</c> alone - <c>StartEnumeration</c> creates the entry and cannot
/// miss it - so an enumeration that the consumer abandons after its first
/// element, or that the server completes inside the initial response, issues a
/// single RPC and cannot abort. A tree scan drained to its end does not enjoy
/// that: the server closes a batch at the first element its own iterator cannot
/// produce synchronously, and a Lattice scan awaits shard grain calls, so
/// completion is normally learned from a later <c>MoveNext</c> however few keys
/// the range holds. A short range is therefore not thereby exempt, though a
/// probe that stops at one element genuinely is. That is why these wrappers are
/// the default for system-tree scans of any length, and not merely an insurance
/// policy for long ones.
/// </para>
/// </summary>
internal static class SystemLatticeScanExtensions
{
    /// <summary>
    /// Resilient forward/reverse key scan over an <see cref="ISystemLattice"/>
    /// system tree. Wraps <see cref="ISystemLattice.KeysAsync"/> with the same
    /// <c>EnumerationAbortedException</c> recovery and deterministic resume
    /// semantics as <see cref="LatticeExtensions.ScanKeysAsync(ILattice, string?, string?, bool, bool?, int?, CancellationToken)"/>, and keeps the
    /// scan keys-only so no values cross the wire.
    /// </summary>
    /// <param name="tree">The system tree to scan.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the tree's lowest key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the tree's end.</param>
    /// <param name="reverse">If <c>true</c>, yields keys in descending order.</param>
    /// <param name="prefetch">Optional per-call override for shard prefetch.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="LatticeExtensions.DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static IAsyncEnumerable<string> ScanKeysAsync(
        this ISystemLattice tree,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        // Eager argument validation, for the same reason as ScanEntriesAsync
        // below: an async-iterator core defers any throw until first
        // MoveNextAsync, so the null-guard lives in this non-async wrapper.
        ArgumentNullException.ThrowIfNull(tree);
        return ScanKeysAsyncCore(tree, startInclusive, endExclusive, reverse, prefetch, maxAttempts, cancellationToken);
    }

    private static async IAsyncEnumerable<string> ScanKeysAsyncCore(
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

        // See ScanEntriesAsyncCore below: Orleans resets the caller-established
        // RequestContext in this iterator's execution flow after the first
        // physical segment completes, so a system-origin or credential scope must
        // be re-asserted around every reopen or a resumed segment resolves to an
        // anonymous subject and a fail-closed gate silently truncates the scan.
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
            // raw-enumeration-ok: this IS the wrapper that recovers the abort.
            var enumerator = tree.KeysAsync(s, e, reverse, prefetch, cancellationToken).GetAsyncEnumerator(cancellationToken);
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

                    // No ScanPageStalledException arm here, deliberately, and the
                    // reasoning survives this surface acquiring its first caller.
                    // A stall is a TimeoutException subclass, and the one caller
                    // that scans on a schedule - LatticeWalGcScheduler - already
                    // has a dedicated TimeoutException arm above its per-tree loop
                    // that attributes a stall as `registry_timed_out` and retries
                    // on the next tick, distinctly from the `registry_failed` arm
                    // an abort was being mis-filed under. So a stall is already
                    // correctly classified and recovered at the caller, whereas an
                    // abort was not, and only the abort needs handling here.
                    // Add the arm, with coverage, if a caller appears that must
                    // resume a stalled system-tree page mid-scan.
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
            // raw-enumeration-ok: this IS the wrapper that recovers the abort.
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

                    // No ScanPageStalledException arm here, deliberately. This
                    // wrapper mirrors the public wrappers' EnumerationAborted
                    // recovery only - the class doc scopes the mirror to exactly
                    // that - and it currently has no callers, so it is not on any
                    // path a stall can reach. Adding a fourth copy of the resume
                    // loop would be untestable through a real caller and would
                    // widen a fix whose whole point is the loops that do run. Add
                    // the arm, and coverage for it, when this surface acquires a
                    // caller that scans a system tree for long enough to stall.
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
