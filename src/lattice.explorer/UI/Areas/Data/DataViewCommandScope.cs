using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The tree a circuit's open workspace offers the New view command over: set by
/// the tree workspace once the caller is proven to administer its tree, and
/// cleared when the workspace shows another tree or closes. The Data area
/// contributes the <see cref="DataArea.CreateViewCommandId"/> palette command only
/// while it holds a tree, so the command is offered exactly where its visible
/// control is.
/// </summary>
/// <remarks>
/// The offer belongs to the caller it was proven for (the sign-in, the endpoint and
/// the asserted tenant): once any of them changes it reads as withdrawn, so one
/// caller's authority is never offered to another.
/// </remarks>
internal sealed class DataViewCommandScope
{
    private readonly ShellCaller _caller;
    private (ShellCallerKey Caller, DataTreeEntry Tree)? _offer;

    /// <summary>Creates the scope.</summary>
    /// <param name="services">The circuit's services, from which the caller is read.</param>
    public DataViewCommandScope(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _caller = ShellCaller.Of(services);
    }

    /// <summary>The administered tree a view can be created over, or <see langword="null"/>.</summary>
    public DataTreeEntry? Source => _offer is { } offer && offer.Caller == _caller.Current ? offer.Tree : null;

    /// <summary>Offers the command over <paramref name="source"/> for the current caller.</summary>
    /// <param name="source">The administered tree.</param>
    public void Offer(DataTreeEntry source)
    {
        ArgumentNullException.ThrowIfNull(source);
        _offer = (_caller.Current, source);
    }

    /// <summary>Withdraws the command when it was offered over <paramref name="source"/>.</summary>
    /// <param name="source">The tree the workspace no longer shows.</param>
    public void Withdraw(DataTreeEntry? source)
    {
        if (source is not null && _offer is { } offer && ReferenceEquals(offer.Tree, source))
        {
            _offer = null;
        }
    }
}
