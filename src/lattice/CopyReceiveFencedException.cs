namespace Orleans.Lattice;

/// <summary>
/// Thrown when a replication apply routes to a physical tree copy that a
/// coordinated restore has fenced against it (issue #4593): either the restored
/// copy is still closed (between its alias swap and the saga's fence lift), or
/// the apply was admitted before the restore paused receiving, so it carries a
/// pre-cutover write the restored copy must never hold.
/// <para>
/// The replication applier maps it to a deferral, so the sender re-ships the
/// entry: a live entry the sender re-ships after the lift is admitted afresh and
/// lands, and a pre-cutover entry is never re-shipped, because every saga
/// participant's shipper rebinds to its restored copy before it sends again.
/// The causal-apply buffer instead discards a parked entry refused as
/// <see cref="AdmittedBeforeRestore"/>: it was parked before the restore's pause,
/// so it is a pre-cutover write.
/// </para>
/// <para>
/// This exception derives directly from <see cref="Exception"/> so the
/// same-silo deep copier Orleans registers for <see cref="Exception"/> covers
/// its base slice. It is part of the internal protocol between the tree's
/// replication apply seam and the replication applier.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CopyReceiveFenced)]
internal sealed class CopyReceiveFencedException : Exception
{
    /// <summary>Creates a new <see cref="CopyReceiveFencedException"/>.</summary>
    /// <param name="treeId">The logical tree the apply addressed.</param>
    /// <param name="physicalTreeId">The fenced physical copy the apply routed to.</param>
    /// <param name="admittedBeforeRestore">
    /// <see langword="true"/> when the copy is open but the apply was admitted
    /// before the restore paused receiving; <see langword="false"/> when the copy
    /// is still closed.
    /// </param>
    public CopyReceiveFencedException(string treeId, string physicalTreeId, bool admittedBeforeRestore = false)
        : base(admittedBeforeRestore
            ? $"Replication apply to tree '{treeId}' routed to restored copy '{physicalTreeId}' but was admitted before the coordinated restore paused receiving; a pre-cutover write never lands on a restored copy."
            : $"Replication apply to tree '{treeId}' routed to physical copy '{physicalTreeId}', whose receive fence is closed by a coordinated restore; the entry is deferred until the restore's fence lifts.")
    {
        TreeId = treeId;
        PhysicalTreeId = physicalTreeId;
        AdmittedBeforeRestore = admittedBeforeRestore;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public CopyReceiveFencedException() { }

    /// <summary>The logical tree the apply addressed.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>The fenced physical copy the apply routed to.</summary>
    [Id(1)] public string PhysicalTreeId { get; set; } = string.Empty;

    /// <summary>
    /// <see langword="true"/> when the apply was admitted before the restore
    /// paused receiving (the copy may be open); <see langword="false"/> when the
    /// copy is still closed.
    /// </summary>
    [Id(2)] public bool AdmittedBeforeRestore { get; set; }
}