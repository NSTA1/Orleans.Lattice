namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// One read a Cluster page shows: loading, loaded, or failed with a sentence.
/// A denial is kept apart from other failures so a page can say "you cannot see
/// this" rather than "something went wrong".
/// </summary>
/// <typeparam name="T">The value read.</typeparam>
internal sealed class ClusterLoad<T>
    where T : class
{
    /// <summary>A read that has not answered yet.</summary>
    public static ClusterLoad<T> Loading => new(null, null, denied: false);

    private ClusterLoad(T? value, string? error, bool denied)
    {
        Value = value;
        Error = error;
        Denied = denied;
    }

    /// <summary>The value, once read.</summary>
    public T? Value { get; }

    /// <summary>The failure sentence, when the read failed.</summary>
    public string? Error { get; }

    /// <summary>Whether the read failed because the caller may not read it.</summary>
    public bool Denied { get; }

    /// <summary>Whether the read has not answered yet.</summary>
    public bool IsLoading => Value is null && Error is null;

    /// <summary>Runs a read, turning any fault other than this page's own cancellation into a failed load.</summary>
    /// <param name="read">The read.</param>
    /// <param name="cancellationToken">The page's token.</param>
    /// <returns>The load.</returns>
    public static async Task<ClusterLoad<T>> RunAsync(Func<CancellationToken, Task<T>> read, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(read);

        try
        {
            return new ClusterLoad<T>(await read(cancellationToken).ConfigureAwait(false), null, denied: false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            return new ClusterLoad<T>(null, ClusterFaults.Describe(exception), ClusterFaults.IsDenied(exception));
        }
    }
}
