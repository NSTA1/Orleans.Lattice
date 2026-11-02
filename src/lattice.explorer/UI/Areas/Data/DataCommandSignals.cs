namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Carries a Data palette command to the tree page that owns its visible control. A
/// command run while a tree page is open is delivered at once; one run elsewhere is
/// held until the next tree page opens and takes it.
/// </summary>
internal sealed class DataCommandSignals
{
    private string? _pending;

    /// <summary>Raised when a command is requested while a page is listening.</summary>
    public event Action<string>? Requested;

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
