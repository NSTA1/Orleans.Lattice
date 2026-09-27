namespace Orleans.Lattice.Apps;

/// <summary>
/// One activated subscription bound to its handler, precomputed when the routing table is built so
/// the per-mutation dispatch does no resolution work.
/// </summary>
/// <param name="context">The activated subscription handed to the handler.</param>
/// <param name="handler">The resolved app handler.</param>
/// <param name="revision">The registry record revision the route was activated from.</param>
internal sealed class AppSubscriptionRoute(AppSubscriptionContext context, IAppChangeFeedHandler handler, long revision)
{
    /// <summary>The activated subscription.</summary>
    public AppSubscriptionContext Context { get; } = context;

    /// <summary>The app handler invoked for matching mutations.</summary>
    public IAppChangeFeedHandler Handler { get; } = handler;

    /// <summary>The registry record revision the route was activated from.</summary>
    public long Revision { get; } = revision;

    /// <summary>
    /// <c>true</c> when <paramref name="mutation"/> falls inside the subscription's key prefix. The
    /// tree id has already matched. Allocation-free ordinal comparisons only.
    /// </summary>
    /// <param name="mutation">The committed mutation.</param>
    /// <returns><c>true</c> when the mutation should be delivered.</returns>
    public bool Matches(in LatticeMutation mutation)
    {
        var prefix = Context.KeyPrefix;
        if (prefix is null)
            return true;

        if (mutation.Kind != MutationKind.DeleteRange)
            return mutation.Key is { } key && key.StartsWith(prefix, StringComparison.Ordinal);

        if (mutation.MatchedKeys is { } matched)
        {
            for (var i = 0; i < matched.Count; i++)
                if (matched[i] is { } candidate && candidate.StartsWith(prefix, StringComparison.Ordinal))
                    return true;
            return false;
        }

        return RangeIntersectsPrefix(mutation.Key, mutation.EndExclusiveKey, prefix);
    }

    /// <summary>
    /// <c>true</c> while the install this route was activated from is still enabled at the same
    /// revision in <paramref name="snapshot"/>, so a disable or uninstall stops delivery at once,
    /// before the routing table is rebuilt.
    /// </summary>
    /// <param name="snapshot">The current registry snapshot.</param>
    /// <returns><c>true</c> when delivery may proceed.</returns>
    public bool IsStillEnabled(CompiledAppRegistrySnapshot snapshot) =>
        snapshot.TryGet(Context.Tenant, Context.App, out var record)
        && record.State == AppRegistryLifecycleState.Enabled
        && record.Revision == Revision;

    /// <summary>
    /// <c>true</c> when the half-open range <c>[start, end)</c> contains at least one key starting
    /// with <paramref name="prefix"/>. Keys with the prefix form the ordinal interval beginning at the
    /// prefix itself, so the range intersects it when its start is below that interval's end and its
    /// end is above the prefix. A <c>null</c> bound is unbounded.
    /// </summary>
    /// <param name="start">The inclusive start key.</param>
    /// <param name="end">The exclusive end key.</param>
    /// <param name="prefix">The subscription key prefix.</param>
    /// <returns><c>true</c> when the range may touch a subscribed key.</returns>
    internal static bool RangeIntersectsPrefix(string? start, string? end, string prefix)
    {
        var startInside = start is null
            || string.CompareOrdinal(start, prefix) < 0
            || start.StartsWith(prefix, StringComparison.Ordinal);
        return startInside && (end is null || string.CompareOrdinal(end, prefix) > 0);
    }
}
