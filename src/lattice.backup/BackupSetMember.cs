namespace Orleans.Lattice.Backup;

/// <summary>
/// One resolved member of a captured backup set: the member backup id and the
/// tree it restores. Returned by <see cref="ILatticeBackupSetResolver"/> so a
/// caller can expand a set id into the per-tree restores it implies without
/// knowing how the set is catalogued. This is an in-process value only; it never
/// crosses the wire, so it carries no serializer surface.
/// </summary>
/// <param name="BackupId">The content-addressed id of the member backup.</param>
/// <param name="TreeId">The tree the member backup restores.</param>
public readonly record struct BackupSetMember(string BackupId, string TreeId);
