using System.ComponentModel;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Caller-supplied idempotency identity for a single logical
/// <see cref="ILattice"/> mutation. When carried through the public
/// API via an ambient <see cref="LatticeIdempotencyContext"/> scope,
/// every retry of the same logical operation re-stamps the produced
/// <see cref="LwwValue{T}.Timestamp"/> with this value verbatim so the
/// existing WAL-append HWM dedup, the LWW merge rule, and the
/// <see cref="PnCounterAccessor"/> counter-side dedup guard collapse
/// the retries into a single observable mutation.
/// </summary>
/// <remarks>
/// <para>
/// The key carries only the logical <see cref="Timestamp"/>. The
/// authoring cluster identity is owned exclusively by the silo via
/// <see cref="LatticeOriginContext"/> / <see cref="ILatticeOriginClusterIdResolver"/>
/// and is deliberately not part of the caller-supplied identity:
/// origin is infrastructure-resolved provenance, and letting callers
/// stamp it would silently misroute loop-suppression, per-origin
/// merge resolution, and WAL/observer audit. The key's
/// <see cref="Timestamp"/> must be stable across retries of the same
/// operation for the dedup to collapse - minting a fresh
/// <see cref="HybridLogicalClock"/> per retry (the default behaviour
/// when no key is supplied) deliberately produces N distinct
/// mutations, which is the negative control for the feature.
/// </para>
/// <para>
/// A typical caller constructs the key once at the boundary of a
/// logical operation, wraps the lattice call in
/// <c>using var _ = LatticeIdempotencyContext.With(key);</c>, and
/// either drives retry from its own infrastructure or registers an
/// <see cref="ILatticeRetryPolicy"/> via
/// <see cref="LatticeOptions.RetryPolicy"/>. The library never mints
/// keys itself - the policy is strictly opt-in.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeIdempotencyKey)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly record struct LatticeIdempotencyKey
{
    /// <summary>
    /// Process-local monotonic high-water-mark backing <see cref="Fresh"/>.
    /// Every successful CAS advances strictly past the previous value so
    /// back-to-back callers each observe a distinct <see cref="HybridLogicalClock"/>
    /// even when the underlying wall clock resolution is coarser than the
    /// inter-call interval (notably true on Linux CI runners where two
    /// <see cref="DateTimeOffset.UtcNow"/> reads inside the same JIT-compiled
    /// method routinely return identical <c>Ticks</c> values). Kept private
    /// to this type because the seam is specific to <see cref="Fresh"/>'s
    /// contract; grain-side HLC stamping continues to use
    /// <see cref="HybridLogicalClock.Tick(HybridLogicalClock)"/> against
    /// the per-grain local frontier.
    /// </summary>
    private static long s_lastTicks;

    /// <summary>
    /// The logical <see cref="HybridLogicalClock"/> every retry of the
    /// operation stamps onto its emitted
    /// <see cref="LwwValue{T}.Timestamp"/>. Stable across retries so
    /// the LWW merge rule treats the second arrival as an exact-tie
    /// duplicate of the first.
    /// </summary>
    [Id(0)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// Convenience factory: builds a key whose
    /// <see cref="Timestamp"/> ticks past
    /// <see cref="HybridLogicalClock.Zero"/>. Useful for unit tests
    /// and ad-hoc callers that mint one key per logical operation
    /// and have not captured an HLC frontier of their own.
    /// Production callers whose retry needs to survive a process
    /// restart should construct the key from a stable, derivable
    /// HLC source so every restarted attempt agrees on the
    /// timestamp bit-identically.
    /// <para>
    /// The factory is safe under arbitrary concurrency: it advances a
    /// process-local 64-bit high-water-mark via lock-free
    /// <see cref="Interlocked.CompareExchange(ref long, long, long)"/>
    /// so any two completed calls observe strictly-ordered (and
    /// therefore distinct) <see cref="HybridLogicalClock.WallClockTicks"/>
    /// values regardless of wall-clock resolution. The wall clock is
    /// consulted on every call, so steady-state output still tracks
    /// real time; only same-tick races fall back to <c>last + 1</c>.
    /// </para>
    /// </summary>
    public static LatticeIdempotencyKey Fresh()
    {
        var now = DateTimeOffset.UtcNow.Ticks;
        long next;
        while (true)
        {
            var last = Interlocked.Read(ref s_lastTicks);
            next = Math.Max(now, last + 1);
            if (Interlocked.CompareExchange(ref s_lastTicks, next, last) == last)
            {
                break;
            }
        }
        return new LatticeIdempotencyKey
        {
            Timestamp = new HybridLogicalClock { WallClockTicks = next, Counter = 0 },
        };
    }
}

