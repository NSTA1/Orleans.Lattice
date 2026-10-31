using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The tree a circuit's open workspace offers the Import command over (issue #5365): set by
/// the tree workspace once the import gate admits the caller, and cleared when the workspace
/// shows another tree or closes. The Data area contributes the
/// <see cref="DataArea.ImportCommandId"/> palette command only while it holds a tree, so the
/// command is offered exactly where its visible control is.
/// </summary>
/// <remarks>
/// The offer belongs to the caller it was proven for (the sign-in, the endpoint and the
/// asserted tenant): once any of them changes it reads as withdrawn.
/// </remarks>
internal sealed class DataImportCommandScope
{
    private readonly ShellCaller _caller;
    private (ShellCallerKey Caller, DataTreeEntry Tree)? _offer;

    /// <summary>Creates the scope.</summary>
    /// <param name="services">The circuit's services, from which the caller is read.</param>
    public DataImportCommandScope(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _caller = ShellCaller.Of(services);
    }

    /// <summary>The tree the open workspace can import into, or <see langword="null"/>.</summary>
    public DataTreeEntry? Source => _offer is { } offer && offer.Caller == _caller.Current ? offer.Tree : null;

    /// <summary>Offers the command over <paramref name="tree"/> for the current caller.</summary>
    /// <param name="tree">The tree.</param>
    public void Offer(DataTreeEntry tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        _offer = (_caller.Current, tree);
    }

    /// <summary>Withdraws the command when it was offered over <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree the workspace no longer shows.</param>
    public void Withdraw(DataTreeEntry? tree)
    {
        if (tree is not null && _offer is { } offer && ReferenceEquals(offer.Tree, tree))
        {
            _offer = null;
        }
    }
}
