namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Calls a stamped leaf write method (epic #5605, member 2), falling back to its
/// existing, unstamped counterpart only on the definite "method unknown" failure of a
/// leaf hosted by an older build that joined before the caller's gate observed it
/// (design note section 4, "Racing the gate"). The existing method appends unstamped,
/// which the WAL shard always admits and reports, so the fallback is always safe. A
/// <see cref="WalRoutingStamp.Unstamped"/> routing goes straight to the existing method.
/// </summary>
/// <remarks>
/// Used on leaf-to-leaf hand-off and span-forward paths, which already allocate their
/// call closures. The shard root's per-write dispatch inlines the same rule instead.
/// </remarks>
internal static class WalRoutedLeafCall
{
    /// <summary>Runs <paramref name="stamped"/>, or <paramref name="plain"/> as described above.</summary>
    public static async Task<T> StampedOrPlainAsync<T>(WalRoutingStamp routing, Func<Task<T>> stamped, Func<Task<T>> plain)
    {
        ArgumentNullException.ThrowIfNull(stamped);
        ArgumentNullException.ThrowIfNull(plain);
        if (routing.ForcesUnstamped)
        {
            return await plain();
        }

        try
        {
            return await stamped();
        }
        catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
        {
            return await plain();
        }
    }
}
