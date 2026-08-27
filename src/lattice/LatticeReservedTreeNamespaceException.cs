using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a user-origin call names a tree inside a reserved namespace that
/// is composed internally by the library - the <c>_lattice_</c> system namespace,
/// the <c>sys-</c> system-data namespace, or the <c>t/</c> structural tenant
/// namespace - or the reserved all-trees authorization sentinel.
/// </summary>
/// <remarks>
/// <para>
/// This is a deterministic, caller-side precondition: the id is not addressable
/// through the public surface no matter who asks, so it is not an authorization
/// failure and not a server fault. Carrying it as a distinct type lets a transport
/// binding map it to a typed status (the data gRPC binding maps it to
/// <c>InvalidArgument</c>) instead of letting a bare
/// <see cref="InvalidOperationException"/> fall through to the generic server-fault
/// arm, which reported a client error as <c>Internal</c> and pointed the caller at
/// the cluster logs for a fault they could see and fix themselves.
/// </para>
/// <para>
/// Derives from <see cref="InvalidOperationException"/> so existing callers that
/// catch the broader type are unaffected.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeReservedTreeNamespace)]
public sealed class LatticeReservedTreeNamespaceException : InvalidOperationException
{
    /// <summary>
    /// The reserved tree id that was rejected. Empty on the parameterless
    /// constructor; populated on the production overload so caller-side
    /// diagnostics can attribute the rejection without parsing the message.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="TreeId"/>. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use the overloads that carry
    /// diagnostic context.
    /// </summary>
    public LatticeReservedTreeNamespaceException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the precondition.</param>
    public LatticeReservedTreeNamespaceException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and inner
    /// exception, and an empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Self-contained caller-facing description of the precondition.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeReservedTreeNamespaceException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the rejected <paramref name="treeId"/>
    /// alongside the diagnostic message.
    /// </summary>
    /// <param name="treeId">The reserved tree id that was rejected.</param>
    /// <param name="message">Self-contained caller-facing description of the precondition.</param>
    public LatticeReservedTreeNamespaceException(string treeId, string message) : base(message)
    {
        TreeId = treeId ?? string.Empty;
    }
}

/// <summary>
/// No-op deep copier for <see cref="LatticeReservedTreeNamespaceException"/>. The
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide - so a same-silo throw would fail with an opaque <c>KeyNotFoundException</c>
/// ("Could not find a base type copier for ...") and mask the real, actionable fault.
/// An exception is immutable once constructed, so returning the same instance is a
/// correct deep copy and keeps the typed exception intact (the cross-silo serialise
/// path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LatticeReservedTreeNamespaceExceptionCopier : IDeepCopier<LatticeReservedTreeNamespaceException>
{
    /// <inheritdoc />
    public LatticeReservedTreeNamespaceException DeepCopy(LatticeReservedTreeNamespaceException input, CopyContext context) => input;
}
