namespace Orleans.Lattice.Apps;

/// <summary>Why an install cannot own one of the trees its manifest declares.</summary>
public enum AppTreeOwnershipConflictReason
{
    /// <summary>No conflict.</summary>
    None = 0,

    /// <summary>Another install in the same tenant (a different app, or the same slug from a different publisher) owns the tree.</summary>
    OwnedByAnotherApp = 1,

    /// <summary>
    /// The app's structural tree already exists with no ownership claim: it was created outside any
    /// app lifecycle, so it is never taken over.
    /// </summary>
    PreExistingUnownedTree = 2,

    /// <summary>The tree is a physical copy core derived from another tree (a resize, restore or remediation copy), so it is not a logical tree and cannot be owned.</summary>
    DerivedTree = 3,

    /// <summary>
    /// The tree's physical backing is also the alias target of another logical tree, or the tree is
    /// aliased to a tree not derived from it, so owning it would give the data a second name.
    /// </summary>
    AliasTarget = 4,
}
