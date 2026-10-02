using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a tree-registry verb that changes an existing tree's registry
/// row - its shard map, a per-tree configuration override, its WAL placement,
/// or a one-way latch - targets a tree id that has no row: the tree was never
/// registered, or it has been purged.
/// </summary>
/// <remarks>
/// <para>
/// Such a verb fails closed rather than creating the row. A row created by a
/// configuration change, a late split, or a leaf latch carries none of the
/// tree's structural pins, and for a purged id it silently undoes the purge
/// (issue #4230). Only an explicit create or register path creates a row.
/// </para>
/// <para>
/// Derives from <see cref="KeyNotFoundException"/>, so a transport binding that
/// already maps that type to a not-found status - every API facade binding does
/// - reports a missing tree as not found. It also implements
/// <see cref="ILatticeDomainFault"/>: it is a deterministic refusal, not a
/// transient fault, and a retry cannot succeed until the tree is created.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeNotRegistered)]
public sealed class LatticeTreeNotRegisteredException : KeyNotFoundException, ILatticeDomainFault
{
    /// <summary>
    /// The tree id that has no registry row. Empty on the parameterless and
    /// message-only constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="TreeId"/>. Provided to satisfy the framework's exception
    /// construction contract; production throw sites use
    /// <see cref="LatticeTreeNotRegisteredException(string, string)"/>.
    /// </summary>
    public LatticeTreeNotRegisteredException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and an
    /// empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Diagnostic description of the refusal.</param>
    public LatticeTreeNotRegisteredException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and inner
    /// exception, and an empty <see cref="TreeId"/>.
    /// </summary>
    /// <param name="message">Diagnostic description of the refusal.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeTreeNotRegisteredException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance naming the tree that has no registry row.
    /// </summary>
    /// <param name="treeId">The tree id that has no registry row.</param>
    /// <param name="operation">The refused operation, named in the message.</param>
    public LatticeTreeNotRegisteredException(string treeId, string operation)
        : base($"Tree '{treeId}' is not registered (it was never created, or it has been purged), so {operation} " +
               "was refused and nothing was created. Create the tree first.")
    {
        TreeId = treeId ?? string.Empty;
    }
}

/// <summary>
/// No-op deep copier for <see cref="LatticeTreeNotRegisteredException"/>. The
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide - so a same-silo throw would fail with an opaque <c>KeyNotFoundException</c>
/// ("Could not find a base type copier for ...") and mask the real refusal. An
/// exception is immutable once constructed, so returning the same instance is a
/// correct deep copy (the cross-silo serialise path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class LatticeTreeNotRegisteredExceptionCopier : IDeepCopier<LatticeTreeNotRegisteredException>
{
    /// <inheritdoc />
    public LatticeTreeNotRegisteredException DeepCopy(LatticeTreeNotRegisteredException input, CopyContext context) => input;
}
