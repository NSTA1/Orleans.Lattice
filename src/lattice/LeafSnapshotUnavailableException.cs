namespace Orleans.Lattice;

/// <summary>
/// Thrown when a leaf's snapshot could not be loaded - a storage fault, an
/// unreadable payload, or a missing or unreadable segment - so the leaf's WAL
/// replay fails closed rather than rebuilding the leaf from the write-ahead log
/// alone (issue #4450).
/// <para>
/// Under coverage-gated trim the WAL GC removes a checkpointed prefix precisely
/// because a snapshot covers it, so the snapshot may be the only durable copy of
/// acknowledged writes. A failed load is therefore not the same as having no
/// snapshot: replaying the readable WAL into an empty cache would bring the leaf
/// up without those writes, and nothing would fault. Probing the WAL tail first
/// is not enough either, because the leaf's durable pin still carries the failed
/// snapshot's coverage and keeps the GC entitled to trim for the whole rebuild.
/// </para>
/// <para>
/// The condition is transient by design. The replay barrier re-arms on the next
/// data operation or WAL GC touch, and the retry loads the snapshot once the
/// store answers. If the snapshot is permanently unreadable, the prefix it
/// covered is lost: recovery is a restore from backup, or
/// <c>ILattice.RebuildLeafProjectionAsync</c> for the leaf's shard, which
/// discards a snapshot proven unreadable and rebuilds the leaf from the WAL that
/// survives, accepting the loss.
/// </para>
/// <para>
/// Derives directly from <see cref="Exception"/> so the generated same-silo deep
/// copier can resolve a base-type copier, and implements
/// <see cref="ILatticeLeafUnavailable"/> so a caller outside this assembly can
/// recognise the condition without naming this internal type.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotUnavailable)]
internal sealed class LeafSnapshotUnavailableException : Exception, ILatticeLeafUnavailable
{
    /// <summary>The tree whose leaf could not be rebuilt.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>Creates a new <see cref="LeafSnapshotUnavailableException"/>.</summary>
    public LeafSnapshotUnavailableException(string treeId, Exception? innerException)
        : base(BuildMessage(treeId), innerException)
    {
        TreeId = treeId;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public LeafSnapshotUnavailableException() { }

    private static string BuildMessage(string treeId)
        => $"A leaf snapshot for tree '{treeId}' could not be loaded. Under coverage-gated write-ahead-log "
            + "trimming that snapshot may be the only durable copy of acknowledged writes, so the leaf's replay "
            + "has failed closed rather than rebuilding it from the log alone. It is retried on the next data "
            + "operation or WAL GC touch and succeeds once the snapshot loads. If the snapshot is permanently "
            + "unreadable, the prefix it covered is lost: restore the tree from a backup, or call "
            + "ILattice.RebuildLeafProjectionAsync for the leaf's shard, which discards an unreadable snapshot "
            + "and rebuilds the leaf from the write-ahead log that survives, accepting the loss.";
}
