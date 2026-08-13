using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Thrown by <see cref="ILatticeReplicationConfigAuthority"/> when a runtime
/// precondition for enabling replication on a tree is not satisfied, so the
/// enable is rejected cleanly before any config write rather than faulting on a
/// later CRDT authoring step.
/// <para>
/// Two preconditions are enforced. First, authoring the config tree's own
/// enablement flag (an <see cref="RwFlag"/>) mints dots stamped with the local
/// replica id, so a host without a configured
/// <see cref="ILatticeReplicationContext.LocalReplicaId"/> (an unset
/// <see cref="LatticeReplicationOptions.ClusterId"/>) cannot author any
/// enable/disable and the request is rejected. Second, the target tree's own
/// flag-based merge modes (<see cref="LatticeMergeMode.OrFlag"/> and
/// <see cref="LatticeMergeMode.RwFlag"/>) likewise require a non-empty local
/// replica id, enforced through
/// <see cref="ILatticeReplicationPreconditionValidator"/>.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing
/// handlers that match it continue to absorb the rejection; the typed slot lets
/// the API facade surface the precondition failure explicitly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.LatticeReplicationPreconditionFailedException)]
public sealed class LatticeReplicationPreconditionFailedException : InvalidOperationException
{
    /// <summary>
    /// The target tree id whose enable was rejected. Empty on the
    /// context-free constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// The wire merge mode the rejected enable requested for the tree.
    /// </summary>
    [Id(1)]
    public LatticeMergeMode RequestedMode { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeReplicationPreconditionFailedException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected enable.</param>
    public LatticeReplicationPreconditionFailedException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected enable.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeReplicationPreconditionFailedException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the target tree id and the merge mode
    /// the rejected enable requested. The primary production throw shape.
    /// </summary>
    /// <param name="message">Actionable context describing the failed precondition.</param>
    /// <param name="treeId">The target tree id whose enable was rejected.</param>
    /// <param name="requestedMode">The merge mode the rejected enable requested.</param>
    public LatticeReplicationPreconditionFailedException(
        string message,
        string treeId,
        LatticeMergeMode requestedMode) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        RequestedMode = requestedMode;
    }
}

/// <summary>
/// Same-silo deep-copier for <see cref="LatticeReplicationPreconditionFailedException"/>. Orleans deep-copies a grain result
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
internal sealed class LatticeReplicationPreconditionFailedExceptionCopier : IDeepCopier<LatticeReplicationPreconditionFailedException>
{
    /// <inheritdoc />
    public LatticeReplicationPreconditionFailedException DeepCopy(LatticeReplicationPreconditionFailedException input, CopyContext context) => input;
}
