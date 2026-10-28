namespace Orleans.Lattice.Replication;

/// <summary>
/// Thrown by the local bootstrap snapshot source (the default
/// <see cref="IBootstrapSnapshotSource"/> registered when no
/// <see cref="IRemoteSnapshotTransport"/> is present) when a bootstrap asks it
/// to export a tree on behalf of a source cluster other than the local one.
/// <para>
/// The local source can only read the local tree. Serving a request that names
/// a foreign source cluster would drain the receiver's own contents back into
/// itself and record that self-export as a completed bootstrap from the foreign
/// source, so the refusal is fail-closed: it is raised before the local
/// <see cref="ISnapshotProvider"/> is called, an unset local
/// <see cref="LatticeReplicationOptions.ClusterId"/> also refuses, and the
/// condition is permanent host configuration, so the bootstrap coordinator does
/// not retry it and records no drained lineage or completed export for the
/// requested source.
/// </para>
/// <para>
/// It derives directly from <see cref="Exception"/>, so the same-silo deep copy
/// needs no registered copier, and implements <see cref="ILatticeDomainFault"/>
/// because it reports a deliberate refusal rather than a transient fault.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.LatticeBootstrapSourceMismatchException)]
public sealed class LatticeBootstrapSourceMismatchException : Exception, ILatticeDomainFault
{
    /// <summary>
    /// The tree whose bootstrap export was refused. Empty on the context-free
    /// constructors.
    /// </summary>
    [Id(0)]
    public string TreeName { get; }

    /// <summary>
    /// The source cluster id the bootstrap requested. Empty on the context-free
    /// constructors.
    /// </summary>
    [Id(1)]
    public string RequestedSourceClusterId { get; }

    /// <summary>
    /// The local cluster id configured for the tree, or empty when none is
    /// configured.
    /// </summary>
    [Id(2)]
    public string LocalClusterId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeBootstrapSourceMismatchException()
    {
        TreeName = string.Empty;
        RequestedSourceClusterId = string.Empty;
        LocalClusterId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the refusal.</param>
    public LatticeBootstrapSourceMismatchException(string message) : base(message)
    {
        TreeName = string.Empty;
        RequestedSourceClusterId = string.Empty;
        LocalClusterId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the refusal.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeBootstrapSourceMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeName = string.Empty;
        RequestedSourceClusterId = string.Empty;
        LocalClusterId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the refused tree, the requested source
    /// cluster id, and the local cluster id. The production throw shape.
    /// </summary>
    /// <param name="treeName">The tree whose bootstrap export was refused.</param>
    /// <param name="requestedSourceClusterId">The source cluster id the bootstrap requested.</param>
    /// <param name="localClusterId">The local cluster id, or empty when none is configured.</param>
    public LatticeBootstrapSourceMismatchException(
        string treeName,
        string requestedSourceClusterId,
        string localClusterId)
        : base(
            $"Tree '{treeName}': the local bootstrap snapshot source cannot export on behalf of source cluster "
            + $"'{requestedSourceClusterId}' because the local cluster id is "
            + (string.IsNullOrEmpty(localClusterId) ? "not configured" : $"'{localClusterId}'")
            + ". Register an IRemoteSnapshotTransport to bootstrap from a peer cluster.")
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(requestedSourceClusterId);
        ArgumentNullException.ThrowIfNull(localClusterId);
        TreeName = treeName;
        RequestedSourceClusterId = requestedSourceClusterId;
        LocalClusterId = localClusterId;
    }
}
