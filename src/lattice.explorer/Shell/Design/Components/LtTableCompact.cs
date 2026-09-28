namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>How an <see cref="LtTable{TItem}"/> lays out below the small breakpoint.</summary>
public enum LtTableCompact
{
    /// <summary>
    /// The default: each row becomes a two-line list row - the row's identifier,
    /// then a summary with its state - that opens a detail sheet holding the whole
    /// record and the row's actions.
    /// </summary>
    List = 0,

    /// <summary>
    /// The booktabs table stays, scrolling inside its own frame. Reserved for
    /// genuinely matrix-shaped data, such as a shard map, where a list row would
    /// lose the grid's meaning.
    /// </summary>
    ScrollFrame = 1,
}
