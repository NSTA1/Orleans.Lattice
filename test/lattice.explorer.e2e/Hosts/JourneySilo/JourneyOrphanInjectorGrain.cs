using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The journey host's orphaned-leaf injector, reachable only when the fault seams are
/// enabled (<c>LATTICE_E2E_FAULTS=1</c>). It leaves behind exactly the persisted state a
/// silo that crashed part way through a leaf split leaves: the new sibling initialised,
/// filled with a shadow copy of its donor's rows, and linked into the shard's sibling
/// chain from both sides - and the parent never told. Nothing routes to the leaf, so the
/// orphaned-leaf audit finds it and, because every row it holds is readable by descent,
/// the repair may unsplice it.
/// </summary>
internal interface IJourneyOrphanInjectorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Splices an orphan after the leftmost leaf of physical shard <paramref name="shardIndex"/>
    /// of the tree this grain is keyed by.
    /// </summary>
    /// <param name="shardIndex">The physical shard.</param>
    /// <returns>The orphan's grain id.</returns>
    Task<string> InjectAsync(int shardIndex);
}

/// <inheritdoc cref="IJourneyOrphanInjectorGrain" />
internal sealed class JourneyOrphanInjectorGrain : Grain, IJourneyOrphanInjectorGrain
{
    /// <inheritdoc />
    public async Task<string> InjectAsync(int shardIndex)
    {
        var treeId = this.GetPrimaryKeyString();
        var physical = await GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeId);
        var shard = GrainFactory.GetGrain<IShardRootGrain>($"{physical}/{shardIndex}");
        var donorId = await shard.GetLeftmostLeafIdAsync()
            ?? throw new InvalidOperationException($"Shard {shardIndex} of '{treeId}' has no leaf to split.");

        var donor = GrainFactory.GetGrain<IBPlusLeafGrain>(donorId);
        var link = await donor.GetKeyRangeAndNextSiblingAsync();
        var rows = await donor.GetLiveRawEntriesAsync();
        if (rows.Count == 0)
        {
            throw new InvalidOperationException($"The leftmost leaf of shard {shardIndex} of '{treeId}' holds no rows to shadow.");
        }

        var orphanId = GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        var orphan = GrainFactory.GetGrain<IBPlusLeafGrain>(orphanId);
        using (LatticeNewLeafIntentContext.BeginScope(orphanId))
        {
            // The split's first two steps: the sibling is born with its bounds and links,
            // then receives the rows it was handed.
            await orphan.InitializeSiblingAsync(new SiblingInitialization
            {
                TreeId = physical,
                ShardIndex = shardIndex,
                LowKeyInclusive = link.KeyRange.LowKeyInclusive,
                HighKeyExclusive = link.KeyRange.HighKeyExclusive,
                PrevSibling = donorId,
                NextSibling = link.NextSibling,
            });

            var shadow = new Dictionary<string, LwwValue<byte[]>>(rows.Count, StringComparer.Ordinal);
            foreach (var row in rows)
            {
                shadow[row.Key] = row.ToLwwValue();
            }

            await orphan.MergeEntriesAsync(shadow);
        }

        // The third: the chain learns of the sibling from both sides. The fourth - the
        // parent adopting it - is the step the crash never reached.
        await donor.SetNextSiblingAsync(orphanId);
        if (link.NextSibling is { } next)
        {
            await GrainFactory.GetGrain<IBPlusLeafGrain>(next).SetPrevSiblingAsync(orphanId);
        }

        return orphanId.ToString();
    }
}
