using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by a WAL shard that has been quiesced for an in-progress
/// <see cref="ILatticeAdmin">administrative</see> placement move. While a
/// partition is fenced, appends are refused so the move coordinator can copy a
/// stable log tail and atomically flip the placement pin without racing a
/// concurrent writer.
/// <para>
/// <b>Caller contract.</b> This is transient back-pressure, not a durable
/// failure: the entries the refused append carried were never committed.
/// Callers should retry after a short delay - the fence is released (and the
/// shard re-routed to the new provider) within the move's quiesce lease, after
/// which a fresh activation accepts appends again. The fence is self-healing:
/// if the move coordinator fails mid-move, the lease expires and the shard
/// deactivates so the next activation re-resolves placement from the durable
/// pin.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing
/// catch handlers continue to absorb it; the typed slot lets retry-aware
/// callers distinguish the quiesce regime from genuine failures.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeWalQuiescing)]
public sealed class LatticeWalQuiescingException : InvalidOperationException
{
    /// <summary>
    /// Initialises a new instance with no diagnostic message. Provided to
    /// satisfy the framework's exception construction contract.
    /// </summary>
    public LatticeWalQuiescingException() { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message.
    /// </summary>
    /// <param name="message">Diagnostic context describing the fenced partition.</param>
    public LatticeWalQuiescingException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception.
    /// </summary>
    /// <param name="message">Diagnostic context describing the fenced partition.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeWalQuiescingException(string message, Exception innerException)
        : base(message, innerException) { }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeWalQuiescingException"/>. Orleans deep-copies a grain result
/// across an in-process (co-located) boundary instead of serialising it, and the
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide - so a same-silo throw would fail with an opaque <c>KeyNotFoundException</c>
/// ("Could not find a base type copier for ...") and mask the real, actionable fault.
/// An exception is immutable once constructed, so returning the same instance is a
/// correct deep copy and keeps the typed exception intact (the cross-silo serialise
/// path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LatticeWalQuiescingExceptionCopier : IDeepCopier<LatticeWalQuiescingException>
{
    /// <inheritdoc />
    public LatticeWalQuiescingException DeepCopy(LatticeWalQuiescingException input, CopyContext context) => input;
}
