using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by the shard-root write path when the target tree is
/// <b>write-fenced</b> for the duration of a cross-cluster saga (for example a
/// restore cutover). While the fence is engaged every mutation to the tree is
/// refused cluster-wide so no post-cut writer can race the cutover; reads are
/// unaffected.
/// <para>
/// <b>Caller contract.</b> This is transient back-pressure, not a durable
/// failure: the refused mutation was never committed. Callers should back off
/// and retry after a short delay - the fence lifts when the saga reaches its
/// terminal decision (commit or abort/compensation) or, if the saga never
/// returns, self-lifts once the bounded cutover fence deadline passes. Retries
/// against the same silo activation succeed once the fence lifts.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing
/// catch handlers that match on <see cref="System.InvalidOperationException"/>
/// continue to absorb it; the typed slot lets retry-aware callers distinguish
/// the fence regime from genuine failures. Mirrors the retryable-back-pressure
/// family (<see cref="LatticeSaturatedException"/>,
/// <see cref="LatticeWalQuiescingException"/>) rather than inventing a separate
/// client-facing contract.
/// </para>
/// <para>
/// Carries the fenced <see cref="TreeId"/> and the engaging <see cref="SagaId"/>
/// so caller-side diagnostics can attribute the refusal without parsing the
/// exception message.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeWriteFenced)]
public sealed class LatticeWriteFencedException : InvalidOperationException
{
    /// <summary>
    /// Logical tree id whose write fence caused the refusal. Empty on the
    /// parameterless constructor; populated on the production overload.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// Identifier of the saga that engaged the write fence. Empty when the
    /// engaging saga is not known to the throw site.
    /// </summary>
    [Id(1)]
    public string SagaId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty
    /// attribution. Provided to satisfy the framework's exception construction
    /// contract; production throw sites use the attributed overload.
    /// </summary>
    public LatticeWriteFencedException()
    {
        TreeId = string.Empty;
        SagaId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty attribution.
    /// </summary>
    /// <param name="message">Diagnostic context describing the refused mutation.</param>
    public LatticeWriteFencedException(string message) : base(message)
    {
        TreeId = string.Empty;
        SagaId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty attribution.
    /// </summary>
    /// <param name="message">Diagnostic context describing the refused mutation.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeWriteFencedException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
        SagaId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message,
    /// fenced tree id, and engaging saga id. The primary production throw
    /// shape.
    /// </summary>
    /// <param name="message">Diagnostic context describing the refused mutation.</param>
    /// <param name="treeId">Logical tree id whose write fence caused the refusal.</param>
    /// <param name="sagaId">Identifier of the saga that engaged the fence.</param>
    public LatticeWriteFencedException(string message, string treeId, string sagaId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(sagaId);
        TreeId = treeId;
        SagaId = sagaId;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeWriteFencedException"/>. Orleans deep-copies a grain result
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
internal sealed class LatticeWriteFencedExceptionCopier : IDeepCopier<LatticeWriteFencedException>
{
    /// <inheritdoc />
    public LatticeWriteFencedException DeepCopy(LatticeWriteFencedException input, CopyContext context) => input;
}
