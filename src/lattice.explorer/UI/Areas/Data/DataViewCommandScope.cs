namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The tree a circuit's open workspace offers the New view command over: set by
/// the tree workspace once the caller is proven to administer its tree, and
/// cleared when the workspace shows another tree or closes. The Data area
/// contributes the <see cref="DataArea.CreateViewCommandId"/> palette command only
/// while it holds a tree, so the command is offered exactly where its visible
/// control is.
/// </summary>
internal sealed class DataViewCommandScope
{
    /// <summary>The administered tree a view can be created over, or <see langword="null"/>.</summary>
    public DataTreeEntry? Source { get; private set; }

    /// <summary>Offers the command over <paramref name="source"/>.</summary>
    /// <param name="source">The administered tree.</param>
    public void Offer(DataTreeEntry source)
    {
        ArgumentNullException.ThrowIfNull(source);
        Source = source;
    }

    /// <summary>Withdraws the command when it was offered over <paramref name="source"/>.</summary>
    /// <param name="source">The tree the workspace no longer shows.</param>
    public void Withdraw(DataTreeEntry? source)
    {
        if (source is not null && ReferenceEquals(Source, source))
        {
            Source = null;
        }
    }
}
