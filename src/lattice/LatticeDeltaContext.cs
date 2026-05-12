using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient delta-capture context used to stamp
/// <see cref="LatticeMutation.DeltaKind"/> /
/// <see cref="LatticeMutation.DeltaPayload"/> onto mutations authored
/// by the current logical call.
/// </summary>
/// <remarks>
/// <para>
/// The post-commit <see cref="IMutationObserver"/> hook receives both
/// the post-merge <see cref="Primitives.LwwValue{T}"/> bytes (via
/// <see cref="LatticeMutation.Value"/>) and - when the producer chooses to
/// supply one - the <em>pre-merge author's delta</em> in opaque-bytes form
/// via <see cref="LatticeMutation.DeltaKind"/> /
/// <see cref="LatticeMutation.DeltaPayload"/>. The author's delta is the
/// minimal record the producer would replay against an in-memory
/// projection to reach the same converged state - for an LWW write it is
/// the value-and-HLC tuple, for an OR-Set add it is the new dot, for a
/// PN-Counter increment it is the per-replica delta, and so on.
/// </para>
/// <para>
/// Encoding is the producer's choice (typically Orleans serialization of a
/// typed delta record from the replication package). The lattice library
/// itself never opens the payload; consumers (notably the replication
/// observer) decode based on <see cref="LatticeMutation.DeltaKind"/>. This
/// keeps the public extensibility contract independent of any specific
/// replication-side delta DTO.
/// </para>
/// <para>
/// Producers wrap their public-API call (or a CRDT accessor's
/// <c>SetIfVersionAsync</c> step) in <see cref="With(string, byte[])"/>
/// so the publish helpers read the context at the HLC-tick site and stamp
/// it onto the emitted <see cref="LatticeMutation"/>. Local writes that
/// leave the context unset produce <see langword="null"/> on both slots -
/// observers then operate from <see cref="LatticeMutation.Value"/> alone,
/// exactly as before this slot existed.
/// </para>
/// </remarks>
public static class LatticeDeltaContext
{
    /// <summary>
    /// Gets or sets the ambient author-delta carry on the current
    /// <see cref="RequestContext"/>. Setting <see langword="null"/>
    /// clears the carry rather than storing a sentinel.
    /// </summary>
    public static (string Kind, byte[] Payload)? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.DeltaRequestContextKey);
            return raw is Carry c ? (c.Kind, c.Payload) : null;
        }
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.DeltaRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.DeltaRequestContextKey,
                    new Carry(value.Value.Kind, value.Value.Payload));
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to the supplied <paramref name="kind"/> /
    /// <paramref name="payload"/> for the lifetime of the returned scope,
    /// restoring the prior value on <see cref="IDisposable.Dispose"/>.
    /// Safe to nest; disposal is idempotent.
    /// </summary>
    /// <param name="kind">
    /// Stable string identifier for the delta encoding (typically the
    /// fully-qualified type name or a short alias of the typed delta
    /// record). Must not be <see langword="null"/> or empty.
    /// </param>
    /// <param name="payload">
    /// Encoded delta bytes. Must not be <see langword="null"/>.
    /// </param>
    public static IDisposable With(string kind, byte[] payload)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        ArgumentNullException.ThrowIfNull(payload);
        var previous = Current;
        Current = (kind, payload);
        return new Scope(previous);
    }

    private sealed class Scope((string Kind, byte[] Payload)? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previous;
        }
    }

    /// <summary>
    /// Internal struct stored on <see cref="RequestContext"/>. Wrapped
    /// (rather than stored as a raw <c>ValueTuple</c>) so the round-trip
    /// has a stable shape independent of language-level tuple identity,
    /// and so an unrelated tuple stored under the same key by a third
    /// party cannot be misinterpreted as a delta carry.
    /// </summary>
    [GenerateSerializer]
    [Alias(TypeAliases.LatticeDeltaCarry)]
    [Immutable]
    internal readonly record struct Carry([property: Id(0)] string Kind, [property: Id(1)] byte[] Payload);
}
