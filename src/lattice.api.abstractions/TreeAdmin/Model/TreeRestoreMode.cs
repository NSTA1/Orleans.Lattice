namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// How a tree-administration restore applied a backup to its target tree. Mirrors
/// the backup engine's own restore mode as a transport-agnostic value the
/// tree-administration facade can return and accept without the abstractions
/// package taking a dependency on the backup engine.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeRestoreMode)]
public enum TreeRestoreMode
{
    /// <summary>
    /// The backup was replayed directly into the target tree (an empty-tree
    /// bulk-load fast path, or a last-writer-wins merge into existing data). Not
    /// undoable through <see cref="ILatticeTreeAdmin.RevertTreeRestoreAsync"/>.
    /// </summary>
    InPlace = 0,

    /// <summary>
    /// The backup was installed into a fresh shadow physical tree and the target's
    /// registry alias was atomically cut over to it, leaving the prior physical
    /// tree intact for an undo. This is the mode the tree-administration
    /// <see cref="ILatticeTreeAdmin.RestoreTreeAsync"/> verb always uses, so a
    /// restore is online and reversible.
    /// </summary>
    ShadowCutover = 1,
}
