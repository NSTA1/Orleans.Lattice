using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown when a tree's alias reservation cannot be taken because another alias
/// operation (a resize, a replication bootstrap copy, a restore or its revert, or a
/// schema remediation or migration) holds it (issue #5750).
/// <para>
/// <b>Caller contract.</b> The refusal is transient: nothing was changed, and the
/// reservation is released when its holder completes. A caller that runs as a
/// tracked, cancellable operation may back off and retry. A caller that cannot wait
/// surfaces it as before.
/// </para>
/// <para>
/// Derives from <see cref="InvalidOperationException"/> with the message the
/// untyped refusal always carried, so every existing catcher and status mapping
/// treats it exactly as before. That base needs the no-op same-silo copier below.
/// It implements <see cref="ILatticeDomainFault"/> because it is a deliberate
/// refusal, so a handler written for a framework failure can decline it: a
/// requested resize undo that meets it stays pending and retries on the next tick,
/// instead of being withdrawn as unrecoverable.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAliasOperationInProgress)]
internal sealed class LatticeAliasOperationInProgressException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>Initialises a new instance with no message and empty identities.</summary>
    public LatticeAliasOperationInProgressException()
    {
        TreeId = string.Empty;
        HolderOperationId = string.Empty;
    }

    /// <summary>Initialises a new instance with a message and empty identities.</summary>
    /// <param name="message">The refusal.</param>
    public LatticeAliasOperationInProgressException(string message) : base(message)
    {
        TreeId = string.Empty;
        HolderOperationId = string.Empty;
    }

    /// <summary>Initialises a new instance with a message, an inner exception and empty identities.</summary>
    /// <param name="message">The refusal.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public LatticeAliasOperationInProgressException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
        HolderOperationId = string.Empty;
    }

    /// <summary>Initialises a new instance naming the tree and the holder. The production throw shape.</summary>
    /// <param name="treeId">The tree whose alias reservation is held.</param>
    /// <param name="holderOperationId">The operation that holds it.</param>
    public LatticeAliasOperationInProgressException(string treeId, string holderOperationId)
        : base($"Tree '{treeId}' has alias operation '{holderOperationId}' in progress; retry after it completes.")
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(holderOperationId);
        TreeId = treeId;
        HolderOperationId = holderOperationId;
    }

    /// <summary>The tree whose alias reservation is held. Empty on the parameterless constructors.</summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>The operation that holds the reservation. Empty on the parameterless constructors.</summary>
    [Id(1)]
    public string HolderOperationId { get; }
}

/// <summary>
/// Same-silo deep copier for <see cref="LatticeAliasOperationInProgressException"/>.
/// Orleans has no copier for its <see cref="InvalidOperationException"/> base, and an
/// exception is immutable once constructed, so the instance is its own copy.
/// </summary>
[RegisterCopier]
internal sealed class LatticeAliasOperationInProgressExceptionCopier : IDeepCopier<LatticeAliasOperationInProgressException>
{
    /// <inheritdoc />
    public LatticeAliasOperationInProgressException DeepCopy(LatticeAliasOperationInProgressException input, CopyContext context) => input;
}
