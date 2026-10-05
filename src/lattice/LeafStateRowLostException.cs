namespace Orleans.Lattice;

/// <summary>
/// Thrown when a leaf finds its own state row missing although that row was once
/// written, so its WAL replay fails closed rather than bringing the leaf up empty
/// (issue #4654).
/// <para>
/// The row holds the leaf's tree binding, key range, projection checkpoint and
/// kept-snapshot record. Without it the leaf cannot replay its WAL or load its
/// snapshot, and an empty cache would report every acknowledged key it held as
/// absent with no error. The leaf knows the row existed from its row record (a
/// separate row written before its first state write and deleted only by a
/// deliberate clear) or from a snapshot that survived it.
/// </para>
/// <para>
/// The condition does not clear by itself. Recovery is a restore of the leaf's
/// row or of the tree from a backup. Derives directly from
/// <see cref="Exception"/> so the generated same-silo deep copier can resolve a
/// base-type copier, and implements <see cref="ILatticeLeafUnavailable"/> so a
/// caller outside this assembly can recognise the condition without naming this
/// internal type.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafStateRowLost)]
internal sealed class LeafStateRowLostException : Exception, ILatticeLeafUnavailable
{
    /// <summary>The tree the leaf was bound to, when its row record names one; otherwise empty.</summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>Creates a new <see cref="LeafStateRowLostException"/>.</summary>
    public LeafStateRowLostException(string leafId, string? treeId, string evidence, Exception? innerException)
        : base(BuildMessage(leafId, treeId, evidence), innerException)
    {
        TreeId = treeId ?? string.Empty;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public LeafStateRowLostException() { }

    private static string BuildMessage(string leafId, string? treeId, string evidence)
        => $"Leaf {leafId}{(string.IsNullOrEmpty(treeId) ? string.Empty : $" of tree '{treeId}'")} has no state row, but "
            + $"{evidence}. The row held the leaf's tree binding, key range, checkpoint and kept-snapshot record, so the "
            + "leaf cannot replay its write-ahead log or load its snapshot, and coming up empty would report every "
            + "acknowledged key it held as absent. Its replay has failed closed. Restore the leaf's row, or the tree, "
            + "from a backup (issue #4654).";
}
