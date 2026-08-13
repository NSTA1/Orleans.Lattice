using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by the public <see cref="ILattice"/> write, CRDT, atomic, and
/// bulk-load surface when the registered <see cref="ILatticeWriteInterceptor"/>
/// <b>rejects</b> an incoming value at the pre-commit choke point. The offending
/// write is <b>fail-closed</b>: nothing is persisted before this exception is
/// raised, so a rejection never leaves a partial write behind.
/// </summary>
/// <remarks>
/// <para>
/// A <b>dead-letter</b> decision is deliberately <em>not</em> surfaced with this
/// exception on a single-key write: a dead-lettered value is diverted by the
/// interceptor and the caller observes a normal, non-throwing completion. Only
/// an explicit <see cref="LatticeWriteDecisionKind.Reject"/> raises this type. In
/// an <em>atomic</em> batch, however, both a reject and a dead-letter abort the
/// whole batch (a dead-letter cannot silently drop one leg of an all-or-nothing
/// commit), and the abort surfaces here.
/// </para>
/// <para>
/// The type is Orleans-serializable so the rejection propagates intact across a
/// grain-call boundary from the enforcing <c>LatticeGrain</c> back to the client.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeWriteRejected)]
public sealed class LatticeWriteRejectedException : InvalidOperationException
{
    /// <summary>
    /// The logical tree id the rejected write targeted. Empty on the
    /// parameterless / message-only constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>The operation the interceptor rejected.</summary>
    [Id(1)]
    public LatticeOperation Operation { get; }

    /// <summary>
    /// The key the rejected write targeted. Empty on the parameterless /
    /// message-only constructors.
    /// </summary>
    [Id(2)]
    public string Key { get; }

    /// <summary>
    /// The human-readable reason the interceptor returned for the rejection.
    /// Empty on the parameterless / message-only constructors.
    /// </summary>
    [Id(3)]
    public string Reason { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeWriteRejectedException()
    {
        TreeId = string.Empty;
        Key = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    public LatticeWriteRejectedException(string message) : base(message)
    {
        TreeId = string.Empty;
        Key = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeWriteRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
        Key = string.Empty;
        Reason = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the rejected tree id, operation, key,
    /// and interceptor reason. The primary production throw shape.
    /// </summary>
    /// <param name="treeId">The tree id the rejected write targeted. Must not be <c>null</c>.</param>
    /// <param name="operation">The operation the interceptor rejected.</param>
    /// <param name="key">The key the rejected write targeted. Must not be <c>null</c>.</param>
    /// <param name="reason">The reason the interceptor returned for the rejection. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/>, <paramref name="key"/>, or <paramref name="reason"/> is <c>null</c>.</exception>
    public LatticeWriteRejectedException(
        string treeId,
        LatticeOperation operation,
        string key,
        string reason)
        : base(BuildMessage(treeId, operation, key, reason))
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(reason);
        TreeId = treeId;
        Operation = operation;
        Key = key;
        Reason = reason;
    }

    private static string BuildMessage(string treeId, LatticeOperation operation, string key, string reason)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(reason);
        return $"Write rejected: {operation} on key '{key}' of tree '{treeId}' "
            + $"was refused by the write interceptor. {reason}";
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeWriteRejectedException"/>. Orleans deep-copies a grain result
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
internal sealed class LatticeWriteRejectedExceptionCopier : IDeepCopier<LatticeWriteRejectedException>
{
    /// <inheritdoc />
    public LatticeWriteRejectedException DeepCopy(LatticeWriteRejectedException input, CopyContext context) => input;
}
