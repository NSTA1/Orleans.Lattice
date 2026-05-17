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
    /// </summary>
    public static LatticeIdempotencyKey Fresh() =>
        new()
        {
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };
}
