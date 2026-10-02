namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Refuses a lazy, read-side registry registration for an id whose purge has
/// completed (issue #4219).
/// <para>
/// A purge removes the tree's registry row and leaves its deletion record
/// behind. Two seams used to register any id with no row on first touch - the
/// options resolver and a shard root seeding its first root - so a later read
/// of any kind (a background healing sweep, a hot-shard sample, a client read)
/// silently recreated the purged tree, and recover-after-purge then succeeded.
/// Both seams consult this guard before they register; a deliberate create or
/// write registers without it, which is the reuse issue #3940 defines. The
/// options resolver refuses a purged id. A shard root answers a data read or
/// delete of a purged id as the empty tree the purge left, seeding and
/// registering nothing, and refuses any other operation on it.
/// </para>
/// </summary>
internal static class PurgedTreeRegistrationGuard
{
    /// <summary>
    /// Throws <see cref="InvalidOperationException"/> when
    /// <paramref name="treeId"/>'s deletion record holds a completed purge.
    /// Fails closed: a fault reading the record propagates, so nothing is
    /// registered when the answer is unknown.
    /// </summary>
    internal static async Task ThrowIfPurgedAsync(IGrainFactory grainFactory, string treeId)
    {
        if (await IsPurgedAsync(grainFactory, treeId).ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                $"Tree '{treeId}' has been purged and no longer exists. A read does not recreate it; "
                + "create the tree or write to it to reuse the id.");
        }
    }

    /// <summary>
    /// Whether <paramref name="treeId"/>'s deletion record holds a completed
    /// purge. Fails closed: a fault reading the record propagates.
    /// <para>
    /// An empty or whitespace id - a leaf or internal node whose tree id was never
    /// set - is never purged: Orleans refuses it as a grain key, so no deletion
    /// record can exist for it, and the deletion grain is not addressed.
    /// </para>
    /// </summary>
    internal static async Task<bool> IsPurgedAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);

        if (string.IsNullOrWhiteSpace(treeId))
            return false;

        return await grainFactory.GetGrain<ITreeDeletionGrain>(treeId).HoldsCompletedPurgeAsync().ConfigureAwait(false);
    }
}
