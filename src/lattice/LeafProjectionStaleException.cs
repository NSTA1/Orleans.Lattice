using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Surfaced when a leaf grain''s persisted projection checkpoint is
/// stale relative to the per-shard WAL and the configured
/// <see cref="ProjectionRebuildPolicy"/> elects to surface the
/// condition rather than recover automatically. Callers respond by
/// invoking the operator surface to drive an explicit rebuild
/// (the operator rebuild API) or by
/// reconfiguring the option to <see cref="ProjectionRebuildPolicy.SnapshotThenWal"/>
/// and reactivating the leaf.
/// <para>
/// Three triggers can produce this exception: (1) the WAL has been
/// trimmed past the leaf''s persisted projection checkpoint;
/// (2) the gap between the persisted checkpoint and the WAL head
/// exceeds <see cref="LatticeOptions.MaxLeafReplayEntries"/>; or
/// (3) the persisted checkpoint is older than
/// <see cref="LatticeOptions.LeafProjectionRetention"/>.
/// </para>
/// <para>
/// Orleans-serializable so that an activation fault raised on a leaf placed
/// on a peer silo (for example when the data API or a replication digest
/// probe activates the leaf from another silo) round-trips cleanly to the
/// caller as this typed, actionable exception rather than degrading into an
/// opaque <c>CodecNotFoundException</c> messaging failure.
/// </para>
/// <para>
/// Implements <see cref="ILatticeLeafUnavailable"/>: a stale projection means the
/// leaf cannot be activated, so an operation that enumerates it cannot make
/// progress and a caller is entitled to fall back to a primitive that does not
/// enumerate.
/// </para>
/// <para>
/// Also implements <see cref="ILatticeDomainFault"/>, and that marker - not the
/// base type - is what a handler must branch on. This exception derives from
/// <see cref="InvalidOperationException"/> for historical reasons, and that
/// inheritance is a hazard rather than a convenience: a broad
/// <c>catch (InvalidOperationException)</c> written to absorb an unrelated
/// framework condition will also absorb this one and apply a remediation that
/// has nothing to do with a stale projection - silently converting an actionable,
/// operator-addressable fault into a retry, a cache discard, or a swallow. A
/// stale projection is never resolved by retrying: it is resolved only by an
/// explicit operator rebuild or by reconfiguring
/// <see cref="ProjectionRebuildPolicy"/>. Handlers that catch the base type
/// broadly must therefore decline this exception explicitly, for example with
/// <c>catch (InvalidOperationException ex) when (ex is not ILatticeDomainFault)</c>,
/// and let it propagate to the caller who can act on it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafProjectionStale)]
public sealed class LeafProjectionStaleException : InvalidOperationException, ILatticeDomainFault, ILatticeLeafUnavailable
{
    /// <summary>
    /// Initialises a new instance with no diagnostic context. Provided to
    /// satisfy the framework's exception construction contract; production
    /// throw sites use the message or message + inner-exception overloads.
    /// </summary>
    public LeafProjectionStaleException() { }

    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LeafProjectionStaleException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LeafProjectionStaleException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Deep-copier for <see cref="LeafProjectionStaleException"/>. Orleans copies a
/// grain call's result across an in-process (same-silo) boundary rather than
/// serialising it, and the generated copier for a <c>[GenerateSerializer]</c>
/// exception that derives from a BCL exception (here
/// <see cref="InvalidOperationException"/>) requires a registered copier for its
/// base type, which Orleans does not provide - so a same-silo throw would fail
/// with an opaque <c>KeyNotFoundException</c> ("Could not find a base type
/// copier for type System.InvalidOperationException") and mask the real,
/// actionable fault. An exception is immutable once constructed, so sharing the
/// same instance is a correct deep copy and keeps the typed exception intact on
/// the co-located path (the cross-silo serialize path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LeafProjectionStaleExceptionCopier : IDeepCopier<LeafProjectionStaleException>
{
    /// <inheritdoc />
    public LeafProjectionStaleException DeepCopy(LeafProjectionStaleException input, CopyContext context) => input;
}
