namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// Carries a palette command to the page that owns its visible control. The
/// palette navigates to the command's target and then invokes it; the page at the
/// target may already be listening, or may render a moment later, so an
/// unheard request is held until the page takes it.
/// </summary>
internal sealed class ClusterCommandSignals
{
    private string? _pending;

    /// <summary>Raised when a command is invoked while a page is listening.</summary>
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
