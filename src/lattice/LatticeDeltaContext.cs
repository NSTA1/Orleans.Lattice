using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient delta-capture context used to stamp
/// <see cref="LatticeMutation.Delta"/> onto mutations authored by the
/// current logical call.
/// </summary>
/// <remarks>
/// <para>
/// The post-commit <see cref="IMutationObserver"/> hook receives both
/// the post-merge <see cref="Primitives.LwwValue{T}"/> bytes (via
/// <see cref="LatticeMutation.Value"/>) and - when the producer chooses
/// to supply one - the <em>pre-merge author's delta</em> in opaque-bytes
/// form via <see cref="LatticeMutation.Delta"/>. The author's delta is
/// the minimal record the producer would replay against an in-memory
/// projection to reach the same converged state - for an LWW write it
/// is the value-and-HLC tuple, for an OR-Set add it is the new dot, for
/// a PN-Counter increment it is the per-replica delta, and so on.
/// </para>
/// <para>
/// Encoding is the Orleans-serialised form of one of the typed delta
/// DTOs (<see cref="LwwRegisterDelta"/>, <see cref="OrSetDelta"/>,
/// <see cref="PnCounterDelta"/>, <see cref="VersionVectorDelta"/>,
/// <see cref="MvRegisterDelta"/>); the receiver dispatches on the
/// <see cref="LatticeMergeMode"/> stamped on the same record to pick
/// the right deserialiser and call <c>MergeDelta</c> on the loaded
/// primitive. The lattice library itself never opens the payload.
/// </para>
/// <para>
/// Producers wrap their public-API call (or a CRDT accessor's
/// <c>SetIfVersionAsync</c> step) in <see cref="With(byte[])"/> so the
/// publish helpers read the context at the HLC-tick site and stamp it
/// onto the emitted <see cref="LatticeMutation"/>. Local writes that
/// leave the context unset produce <see langword="null"/> on the slot;
/// observers then operate from <see cref="LatticeMutation.Value"/>
/// alone (the LWW / opaque-bytes path).
/// </para>
/// </remarks>
public static class LatticeDeltaContext
{
    /// <summary>
    /// Gets or sets the ambient author-delta carry on the current
    /// <see cref="RequestContext"/>. Setting <see langword="null"/>
    /// clears the carry rather than storing a sentinel.
    /// </summary>
    public static byte[]? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.DeltaRequestContextKey);
            return raw is Carry c ? c.Payload : null;
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
                    new Carry(value));
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to the supplied <paramref name="payload"/>
    /// for the lifetime of the returned scope, restoring the prior
    /// value on <see cref="IDisposable.Dispose"/>. Safe to nest;
    /// disposal is idempotent.
    /// </summary>
    /// <param name="payload">Encoded delta bytes. Must not be <see langword="null"/>.</param>
    public static IDisposable With(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var previous = Current;
        Current = payload;
        return new Scope(previous);
    }

    private sealed class Scope(byte[]? previous) : IDisposable
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
    /// (rather than stored as a raw <c>byte[]</c>) so the round-trip has
    /// a stable Orleans-codec shape and so an unrelated byte array stored
    /// under the same key by a third party cannot be misinterpreted as a
    /// delta carry.
    /// </summary>
    [GenerateSerializer]
    [Alias(TypeAliases.LatticeDeltaCarry)]
    [Immutable]
    internal readonly record struct Carry([property: Id(0)] byte[] Payload);
}
