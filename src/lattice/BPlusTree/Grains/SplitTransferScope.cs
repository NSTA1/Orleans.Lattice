using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Marks a split donor's transfer of rows into its sibling (#5837, transfer condition (d)):
/// only the rows the donor applied - the transfer batches and the stragglers - ride the
/// transfer class. Carried in the request context, so the leaf grain interface is unchanged
/// and a build that does not read it appends the transfer unstamped, as before.
/// </summary>
internal static class SplitTransferScope
{
    /// <summary>The request-context key the mark travels under.</summary>
    internal const string ContextKey = "lattice.split.transfer";

    /// <summary>Whether the call in flight is a split donor's transfer.</summary>
    internal static bool IsTransfer => RequestContext.Get(ContextKey) is true;

    /// <summary>Runs <paramref name="call"/>, a transfer into a split sibling, marked as one.</summary>
    internal static async Task RunAsync(Func<Task> call)
    {
        var previous = RequestContext.Get(ContextKey);
        RequestContext.Set(ContextKey, true);
        try
        {
            await call();
        }
        finally
        {
            if (previous is null)
            {
                RequestContext.Remove(ContextKey);
            }
            else
            {
                RequestContext.Set(ContextKey, previous);
            }
        }
    }
}
