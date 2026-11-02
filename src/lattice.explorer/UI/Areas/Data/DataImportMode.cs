namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>How an import writes into its tree (issue #5365).</summary>
internal enum DataImportMode
{
    /// <summary>
    /// Writes every entry through the Data API in batches, in any order, into a tree that
    /// may already hold keys; each refused line is counted and listed, and the rest land.
    /// Needs write access to the tree.
    /// </summary>
    Upsert,

    /// <summary>
    /// Seeds an empty tree with a bottom-up bulk load. The file must be in strictly
    /// ascending key order, which a pre-flight pass checks before anything is written.
    /// Needs the bulk-load grant.
    /// </summary>
    Seed,
}
