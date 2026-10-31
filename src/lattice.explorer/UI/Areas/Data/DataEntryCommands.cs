using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Data area's entry commands - <c>data.new-entry</c>, <c>data.edit-entry</c>
/// and <c>data.delete-entry</c> - and the signal that carries one from the palette
/// to the control that performs it. The open tree workspace publishes the tree and
/// key it shows with the rights the caller holds there; a command is offered only
/// while its control is drawn, and running it navigates to that control's address
/// and asks it to act. An unheard request is held until the control renders.
/// </summary>
internal sealed class DataEntryCommands
{
    /// <summary>The id of the "New entry..." command.</summary>
    public const string NewEntryId = "data.new-entry";

    /// <summary>The id of the "Edit entry" command.</summary>
    public const string EditEntryId = "data.edit-entry";

    /// <summary>The id of the "Delete entry..." command.</summary>
    public const string DeleteEntryId = "data.delete-entry";

    private string? _pending;
    private ExplorerAddress? _treeAddress;
    private string? _treeName;
    private DataWriteRights _treeRights;
    private string? _key;
    private DataWriteRights _keyRights;

    /// <summary>Raised when a command is invoked while a control is listening.</summary>
    public event Action<string>? Requested;

    /// <summary>The commands offered right now, for the palette.</summary>
    public IReadOnlyList<ExplorerCommand> Commands
    {
        get
        {
            if (_treeAddress is not { } tree || _treeName is not { } name)
            {
                return [];
            }

            var commands = new List<ExplorerCommand>(3);
            if (_treeRights.CanWrite)
            {
                commands.Add(new ExplorerCommand(NewEntryId, "New entry...")
                {
                    Detail = $"Write a new key in {name}",
                    Target = tree,
                    InvokeAsync = _ => RequestAsync(NewEntryId),
                });
            }

            if (_key is { } key)
            {
                var at = tree.WithQuery(ExplorerAddress.KeyQuery, key);
                if (_keyRights.CanWrite)
                {
                    commands.Add(new ExplorerCommand(EditEntryId, "Edit entry")
                    {
                        Detail = $"Change the value of {DataKeysPanel.Clip(key)}",
                        Target = at,
                        InvokeAsync = _ => RequestAsync(EditEntryId),
                    });
                }

                if (_keyRights.CanDelete)
                {
                    commands.Add(new ExplorerCommand(DeleteEntryId, "Delete entry...")
                    {
                        Detail = "Asks for confirmation first",
                        Target = at,
                        InvokeAsync = _ => RequestAsync(DeleteEntryId),
                    });
                }
            }

            return commands;
        }
    }

    /// <summary>Publishes the open tree and what the caller may change in it.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="rights">The caller's rights over the tree as a whole.</param>
    public void ShowTree(DataTreeEntry tree, DataWriteRights rights)
    {
        ArgumentNullException.ThrowIfNull(tree);
        _treeAddress = tree.Address;
        _treeName = tree.DisplayName;
        _treeRights = rights;
    }

    /// <summary>Publishes the open entry and what the caller may change at it.</summary>
    /// <param name="key">The key.</param>
    /// <param name="rights">The caller's rights at the key.</param>
    public void ShowEntry(string key, DataWriteRights rights)
    {
        ArgumentNullException.ThrowIfNull(key);
        _key = key;
        _keyRights = rights;
    }

    /// <summary>Withdraws the entry commands when the entry closes.</summary>
    /// <param name="key">The key whose entry closed; a later entry's publication is kept.</param>
    public void HideEntry(string key)
    {
        if (string.Equals(_key, key, StringComparison.Ordinal))
        {
            _key = null;
            _keyRights = DataWriteRights.None;
        }
    }

    /// <summary>Withdraws every command when the tree workspace closes.</summary>
    /// <param name="tree">The tree whose workspace closed; a later tree's publication is kept.</param>
    public void HideTree(DataTreeEntry tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        if (Equals(_treeAddress, tree.Address))
        {
            _treeAddress = null;
            _treeName = null;
            _treeRights = DataWriteRights.None;
            _key = null;
            _keyRights = DataWriteRights.None;
        }
    }

    /// <summary>Requests the command <paramref name="commandId"/>.</summary>
    /// <param name="commandId">The command id.</param>
    /// <returns>A completed task.</returns>
    public ValueTask RequestAsync(string commandId)
    {
        ArgumentException.ThrowIfNullOrEmpty(commandId);
        if (Requested is { } listeners)
        {
            _pending = null;
            listeners(commandId);
        }
        else
        {
            _pending = commandId;
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Takes a held request for <paramref name="commandId"/>, if there is one.</summary>
    /// <param name="commandId">The command id.</param>
    /// <returns><see langword="true"/> when a request was held.</returns>
    public bool TryTake(string commandId)
    {
        if (!string.Equals(_pending, commandId, StringComparison.Ordinal))
        {
            return false;
        }

        _pending = null;
        return true;
    }
}
