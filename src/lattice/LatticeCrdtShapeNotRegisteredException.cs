using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a typed OR-Map CRDT verb targets a tree whose host never
/// registered the matching <c>(TKey, TValue)</c> shape via
/// <c>ISiloBuilder.AddOrMapShape&lt;TKey, TValue&gt;(treeName)</c>. The generic
/// OR-Map wire shape cannot be resolved without a registered descriptor, so the
/// write is rejected before any state changes: this is a deterministic
/// host-configuration precondition, not a server fault.
/// <para>
/// <b>Caller contract.</b> Register the OR-Map pair for the tree at silo
/// construction (closed-shape modes - OR-Set, PN-Counter, Version-Vector,
/// MV-Register - resolve through the global registry automatically and never
/// raise this). Retrying the identical request against an unconfigured tree will
/// fail identically.
/// </para>
/// <para>
/// <b>Sources.</b> Raised by the leaf grain's typed CRDT apply path
/// (<c>ApplyCrdtDeltaAsync</c>) and by the prepared-atomic-write fold on the
/// terminal commit, both of which require a shape descriptor for OR-Map trees.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing catch
/// handlers that match on <see cref="System.InvalidOperationException"/>
/// continue to absorb it; the typed slot lets the API bindings map this specific
/// configuration precondition to a client-error status (for example gRPC
/// <c>FailedPrecondition</c>) rather than collapsing it into an opaque
/// server-side fault.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCrdtShapeNotRegistered)]
public sealed class LatticeCrdtShapeNotRegisteredException : InvalidOperationException
{
    /// <summary>
    /// The tree id whose OR-Map shape was unresolved. Empty on the parameterless
    /// constructor; populated on the production overload so caller-side
    /// diagnostics can attribute the rejection without parsing the message.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="TreeId"/>. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use the overloads that
    /// carry diagnostic context.
    /// </summary>
    public LatticeCrdtShapeNotRegisteredException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the precondition.</param>
    public LatticeCrdtShapeNotRegisteredException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and an empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the precondition.</param>
    /// <param name="innerException">The underlying cause, when one is available.</param>
    public LatticeCrdtShapeNotRegisteredException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and the
    /// tree id whose shape was unresolved. The primary production throw shape.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the precondition.</param>
    /// <param name="treeId">The tree id whose OR-Map shape was not registered.</param>
    public LatticeCrdtShapeNotRegisteredException(string message, string treeId) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeCrdtShapeNotRegisteredException"/>. Orleans deep-copies a grain result
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
internal sealed class LatticeCrdtShapeNotRegisteredExceptionCopier : IDeepCopier<LatticeCrdtShapeNotRegisteredException>
{
    /// <inheritdoc />
    public LatticeCrdtShapeNotRegisteredException DeepCopy(LatticeCrdtShapeNotRegisteredException input, CopyContext context) => input;
}
