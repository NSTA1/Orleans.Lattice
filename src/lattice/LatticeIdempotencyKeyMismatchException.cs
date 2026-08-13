using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a caller-supplied idempotency <c>operationId</c> is re-submitted
/// with a different set of keys than the first submission that bound it. An
/// <c>operationId</c> is bound to the exact key set of its first call, so a
/// retry that adds, removes, or renames a key is a client-side misuse of the
/// idempotency key rather than a server fault: it is rejected before any write,
/// leaving the original saga's committed data untouched.
/// <para>
/// <b>Caller contract.</b> This is a deterministic caller error. Either resubmit
/// the original key set (reordering keys or changing their values is allowed -
/// only the set of keys is fingerprinted) or use a fresh <c>operationId</c> for
/// the new batch. Retrying the identical bad request will fail identically.
/// </para>
/// <para>
/// <b>Sources.</b> Raised by the single-tree atomic-write saga
/// (<c>ILattice.SetManyAtomicAsync</c> with a caller-supplied
/// <c>operationId</c>) and by the cross-tree atomic-write coordinator
/// (<c>ILattice.SetManyAtomicCrossTreeAsync</c>); the cross-tree form also
/// covers a re-submit that presents a different set of participating trees.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing catch
/// handlers that match on <see cref="System.InvalidOperationException"/>
/// continue to absorb it; the typed slot lets the API bindings map this specific
/// misuse to a client-error status (for example gRPC
/// <c>FailedPrecondition</c>) rather than collapsing it into an opaque
/// server-side fault.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeIdempotencyKeyMismatch)]
public sealed class LatticeIdempotencyKeyMismatchException : InvalidOperationException
{
    /// <summary>
    /// The caller-supplied idempotency key whose reuse with a different key (or
    /// tree) set was rejected. Empty on the parameterless constructor; populated
    /// on the production overloads so caller-side diagnostics can attribute the
    /// rejection without parsing the exception message.
    /// </summary>
    [Id(0)]
    public string OperationId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="OperationId"/>. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use the overloads that
    /// carry diagnostic context.
    /// </summary>
    public LatticeIdempotencyKeyMismatchException()
    {
        OperationId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// empty <see cref="OperationId"/>.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the misuse.</param>
    public LatticeIdempotencyKeyMismatchException(string message) : base(message)
    {
        OperationId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and an empty <see cref="OperationId"/>.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the misuse.</param>
    /// <param name="innerException">The underlying cause, when one is available.</param>
    public LatticeIdempotencyKeyMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
        OperationId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and the
    /// offending idempotency key. The primary production throw shape.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the misuse.</param>
    /// <param name="operationId">The caller-supplied idempotency key whose reuse was rejected.</param>
    public LatticeIdempotencyKeyMismatchException(string message, string operationId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        OperationId = operationId;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeIdempotencyKeyMismatchException"/>. Orleans deep-copies a grain result
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
internal sealed class LatticeIdempotencyKeyMismatchExceptionCopier : IDeepCopier<LatticeIdempotencyKeyMismatchException>
{
    /// <inheritdoc />
    public LatticeIdempotencyKeyMismatchException DeepCopy(LatticeIdempotencyKeyMismatchException input, CopyContext context) => input;
}
