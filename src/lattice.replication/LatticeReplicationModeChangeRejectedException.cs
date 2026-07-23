namespace Orleans.Lattice.Replication;

/// <summary>
/// Thrown by
/// <see cref="ILatticeReplicationConfigAuthority.EnableReplicationAsync"/> when
/// an enable request would change the merge mode of a tree that is already
/// enabled. A populated tree's merge mode is effectively immutable: it must
/// match the tree's CRDT semantics, and silently switching it risks
/// last-writer-wins data loss. The sanctioned way to change a mode is to
/// <see cref="ILatticeReplicationConfigAuthority.DisableReplicationAsync">disable</see>
/// the tree and then re-enable it under the new mode, which re-bootstraps.
/// <para>
/// This is raised in two cases: the tree is enabled under an unambiguous mode
/// that differs from the requested one, or the tree's mode is currently
/// <see cref="LatticeReplicationConfigEntry.HasAmbiguousMode">ambiguous</see>
/// (concurrent clusters assigned divergent modes) and must be cleared by a
/// disable-then-re-enable before a new mode can be fixed. Re-enabling under the
/// <i>same</i> unambiguous mode is idempotent and does <b>not</b> throw.
/// </para>
/// <para>
/// Derives from <see cref="System.InvalidOperationException"/> so existing
/// handlers that match it continue to absorb the rejection; the typed slot lets
/// the API facade surface the mode-change rejection explicitly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.LatticeReplicationModeChangeRejectedException)]
public sealed class LatticeReplicationModeChangeRejectedException : InvalidOperationException
{
    /// <summary>
    /// The target tree id whose in-place mode change was rejected. Empty on the
    /// context-free constructors.
    /// </summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>
    /// The wire merge mode the rejected enable requested.
    /// </summary>
    [Id(1)]
    public LatticeMergeMode RequestedMode { get; }

    /// <summary>
    /// The tree's current unambiguous merge mode, or the default value when the
    /// current mode is ambiguous (see <see cref="CurrentModeAmbiguous"/>).
    /// </summary>
    [Id(2)]
    public LatticeMergeMode CurrentMode { get; }

    /// <summary>
    /// <see langword="true"/> when the tree's current mode is ambiguous
    /// (more than one live value), so <see cref="CurrentMode"/> is not
    /// meaningful and the tree must be disabled then re-enabled to clear the
    /// ambiguity.
    /// </summary>
    [Id(3)]
    public bool CurrentModeAmbiguous { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty context.
    /// Provided to satisfy the framework's exception construction contract;
    /// production throw sites use the context-carrying overload.
    /// </summary>
    public LatticeReplicationModeChangeRejectedException()
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected mode change.</param>
    public LatticeReplicationModeChangeRejectedException(string message) : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception, and empty context.
    /// </summary>
    /// <param name="message">Diagnostic context describing the rejected mode change.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeReplicationModeChangeRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
        TreeId = string.Empty;
    }

    /// <summary>
    /// Initialises a new instance carrying the target tree id, the current mode
    /// (or ambiguity flag), and the requested mode. The primary production throw
    /// shape.
    /// </summary>
    /// <param name="message">Actionable context instructing the operator to disable then re-enable.</param>
    /// <param name="treeId">The target tree id whose in-place mode change was rejected.</param>
    /// <param name="requestedMode">The merge mode the rejected enable requested.</param>
    /// <param name="currentMode">The tree's current unambiguous mode, or the default value when ambiguous.</param>
    /// <param name="currentModeAmbiguous">Whether the current mode is ambiguous.</param>
    public LatticeReplicationModeChangeRejectedException(
        string message,
        string treeId,
        LatticeMergeMode requestedMode,
        LatticeMergeMode currentMode,
        bool currentModeAmbiguous) : base(message)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        RequestedMode = requestedMode;
        CurrentMode = currentMode;
        CurrentModeAmbiguous = currentModeAmbiguous;
    }
}
