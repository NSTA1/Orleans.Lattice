namespace Orleans.Lattice;

/// <summary>
/// Thrown when a replicated write reaches a shard root carrying a replication
/// admission epoch older than the one the shard holds (issue #4549). The write
/// was admitted by the replication applier before a bootstrap drop floor was
/// installed for the tree, so it was never checked against that floor; letting
/// it land after the bootstrap's reconcile scan could resurrect a key the source
/// deleted.
/// <para>
/// The replication applier maps it to a deferral, so the sender re-ships the
/// entry and the re-shipped delivery is admitted against the floor. It derives
/// directly from <see cref="Exception"/> so the same-silo deep copier Orleans
/// registers for <see cref="Exception"/> covers its base slice. It is part of the
/// internal protocol between the shard root and the replication applier.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ReplicationFloorAdmissionStale)]
internal sealed class ReplicationFloorAdmissionStaleException : Exception
{
    /// <summary>Creates a new <see cref="ReplicationFloorAdmissionStaleException"/>.</summary>
    /// <param name="treeId">The physical tree the write routed to.</param>
    /// <param name="admittedEpoch">The admission epoch the write carried.</param>
    /// <param name="requiredEpoch">The admission epoch the shard holds.</param>
    public ReplicationFloorAdmissionStaleException(string treeId, long admittedEpoch, long requiredEpoch)
        : base($"Replicated write to tree '{treeId}' was admitted under replication floor-admission epoch {admittedEpoch}, "
            + $"but the tree now requires epoch {requiredEpoch}: a bootstrap drop floor was installed after it was admitted, "
            + "so it is deferred and re-admitted against the floor.")
    {
        TreeId = treeId;
        AdmittedEpoch = admittedEpoch;
        RequiredEpoch = requiredEpoch;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public ReplicationFloorAdmissionStaleException() { }

    /// <summary>The physical tree the write routed to.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>The admission epoch the write carried.</summary>
    [Id(1)] public long AdmittedEpoch { get; set; }

    /// <summary>The admission epoch the shard holds.</summary>
    [Id(2)] public long RequiredEpoch { get; set; }
}
