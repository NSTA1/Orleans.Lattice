using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The chain of leaves a write was forwarded through before its authoring leaf was linked
/// (#5837, condition 4 and F2'), carried in the request context so the leaf grain
/// interface is unchanged and a build that does not read it ignores it. Each hop is a
/// (forwarder, narrow tag) pair, the tag being the id of the split sibling the forwarder's
/// narrow moved the key to (X2). The authoring leaf's append fence copies the chain into
/// its <see cref="WalAppendStamp"/>; the WAL shard attributes the record to every hop.
/// </summary>
/// <remarks>
/// A chain holds at most <see cref="WalAppendStamp.MaxForwardHops"/> hops. A forward past
/// that bound replaces it with the empty overflow marker, and a write carrying the marker
/// is appended unstamped, which every leaf sharing the partition is reported (C4); it is
/// never stamped with a truncated chain. Only a forward allocates: the chain is copied on
/// each hop, and an append that was not forwarded reads one absent context entry.
/// </remarks>
internal static class WalForwardChain
{
    /// <summary>The request-context key the chain travels under.</summary>
    internal const string ContextKey = "lattice.wal.fwd";

    private static readonly Guid[] Overflow = [];

    /// <summary>The chain of the call in flight, or <see langword="null"/> when it was not forwarded.</summary>
    internal static Guid[]? Current => RequestContext.Get(ContextKey) as Guid[];

    /// <summary><paramref name="chain"/> with a hop through <paramref name="forwarder"/> under <paramref name="tag"/> appended, or the overflow marker past the bound.</summary>
    internal static Guid[] Extend(Guid[]? chain, Guid forwarder, Guid tag)
    {
        if (chain is { Length: 0 })
        {
            return chain;
        }

        var hops = (chain?.Length ?? 0) / 2;
        if (hops >= WalAppendStamp.MaxForwardHops)
        {
            return Overflow;
        }

        var next = new Guid[(hops + 1) * 2];
        chain?.CopyTo(next, 0);
        next[hops * 2] = forwarder;
        next[(hops * 2) + 1] = tag;
        return next;
    }

    /// <summary>
    /// Runs <paramref name="call"/>, a forward of a write to the sibling
    /// <paramref name="tag"/>'s narrow moved it to, with this hop added to the chain.
    /// </summary>
    internal static async Task<T> ForwardAsync<T>(Guid forwarder, Guid tag, Func<Task<T>> call)
    {
        var previous = RequestContext.Get(ContextKey);
        RequestContext.Set(ContextKey, Extend(previous as Guid[], forwarder, tag));
        try
        {
            return await call();
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

    /// <summary>The <see cref="ForwardAsync{T}"/> of a forward that returns no value.</summary>
    internal static Task ForwardAsync(Guid forwarder, Guid tag, Func<Task> call) =>
        ForwardAsync(forwarder, tag, async () =>
        {
            await call();
            return true;
        });

    /// <summary>
    /// Copies <paramref name="chain"/> into <paramref name="stamp"/>. Returns
    /// <see langword="false"/> for an overflowed chain, whose write must be appended
    /// unstamped.
    /// </summary>
    internal static bool TryApply(ref WalAppendStamp stamp, Guid[]? chain)
    {
        if (chain is null)
        {
            return true;
        }

        if (chain.Length == 0)
        {
            return false;
        }

        stamp = stamp with
        {
            Forwarder = chain[0],
            ForwarderTag = chain[1],
            Forwarder2 = chain.Length >= 4 ? chain[2] : Guid.Empty,
            ForwarderTag2 = chain.Length >= 4 ? chain[3] : Guid.Empty,
            Forwarder3 = chain.Length >= 6 ? chain[4] : Guid.Empty,
            ForwarderTag3 = chain.Length >= 6 ? chain[5] : Guid.Empty,
        };
        return true;
    }
}
