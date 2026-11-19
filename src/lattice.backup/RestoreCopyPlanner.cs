using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Classifies the physical copies the shadow-cutover restores of one logical tree
/// have left behind, from authoritative registry state (issue #6103).
/// <para>
/// Only one restore of a tree is revertible at a time: the one whose shadow the
/// tree resolves to now, while it is not final. Its revert returns to the copy
/// its commit recorded (<see cref="TreeRegistryEntry.RestoreRevertPhysicalTreeId"/>),
/// so that copy and the alias target are kept; every other restore copy is
/// reachable by no revert. A restore copy is a registered tree whose
/// <see cref="TreeRegistryEntry.RestoreShadowOfTreeId"/> provenance names the
/// tree - the shadow naming only locates candidates, it never decides one - plus
/// the tree's own original shards once a restore replaced them and no bare alias
/// recorded a way back to them.
/// </para>
/// <para>
/// The classification fails safe. A revertible restore committed before its
/// revert target was recorded, or an alias target no restore built, holds every
/// copy rather than guess which one some revert or other operation still needs.
/// A copy already soft-deleted is not reported again.
/// </para>
/// </summary>
internal static class RestoreCopyPlanner
{
    /// <summary>The infix every restore shadow id carries after its logical tree id.</summary>
    internal const string ShadowInfix = "-bkprestore-";

    /// <summary>Registry rows read per batch, so a large family never builds one large request.</summary>
    private const int EntryBatchSize = 64;

    /// <summary>Classifies <paramref name="treeId"/>'s restore copies. The caller holds a system-origin scope.</summary>
    public static async Task<LatticeRestoreCopyAudit> PlanAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var registry = grainFactory.GetLatticeRegistry();
        var logical = await registry.GetEntryAsync(treeId).ConfigureAwait(false);
        if (logical is null)
        {
            return new LatticeRestoreCopyAudit
            {
                TreeId = treeId,
                AliasTargetPhysicalTreeId = treeId,
                Hold = LatticeRestoreCopyHold.TreeNotRegistered,
            };
        }

        var aliasTarget = logical.PhysicalTreeId ?? treeId;
        var shadows = await ReadShadowsAsync(registry, treeId).ConfigureAwait(false);
        var shadowIds = new string[shadows.Count];
        for (var i = 0; i < shadows.Count; i++)
        {
            shadowIds[i] = shadows[i].Id;
        }

        string? revertTarget = null;
        DateTimeOffset? committedAt = null;
        var hold = LatticeRestoreCopyHold.None;
        if (!string.Equals(aliasTarget, treeId, StringComparison.Ordinal))
        {
            var current = Find(shadows, aliasTarget);
            if (current is null)
            {
                hold = LatticeRestoreCopyHold.ForeignAliasTarget;
            }
            else if (!current.RestoreFinal)
            {
                committedAt = current.RestoreCommittedAtUtc;
                if (current.RestoreRevertPhysicalTreeId is { } recorded)
                {
                    revertTarget = recorded;
                }
                else if (current.ReplacedShardMap is not null)
                {
                    hold = LatticeRestoreCopyHold.UnknownRevertTarget;
                }
            }
        }

        if (hold != LatticeRestoreCopyHold.None)
        {
            return new LatticeRestoreCopyAudit
            {
                TreeId = treeId,
                AliasTargetPhysicalTreeId = aliasTarget,
                RevertCommittedAtUtc = committedAt,
                RestoreShadowPhysicalTreeIds = shadowIds,
                Hold = hold,
            };
        }

        List<string>? retirable = null;
        foreach (var (id, _) in shadows)
        {
            if (IsKept(id, aliasTarget, revertTarget))
            {
                continue;
            }

            if (!await IsRetiredAsync(grainFactory, id).ConfigureAwait(false))
            {
                (retirable ??= []).Add(id);
            }
        }

        // The tree's own shards are a restore copy only once the alias left them
        // for a restore shadow. A bare alias records the way back to them
        // (UnaliasedShardMap), so they are then left alone.
        if (!string.Equals(aliasTarget, treeId, StringComparison.Ordinal)
            && !IsKept(treeId, aliasTarget, revertTarget)
            && logical.UnaliasedShardMap is null
            && !await IsRetiredAsync(grainFactory, treeId).ConfigureAwait(false))
        {
            (retirable ??= []).Add(treeId);
        }

        return new LatticeRestoreCopyAudit
        {
            TreeId = treeId,
            AliasTargetPhysicalTreeId = aliasTarget,
            RevertTargetPhysicalTreeId = revertTarget,
            RevertCommittedAtUtc = revertTarget is null ? null : committedAt,
            RetirablePhysicalTreeIds = retirable is null ? [] : retirable,
            RestoreShadowPhysicalTreeIds = shadowIds,
        };
    }

    /// <summary>Whether a soft delete, retirement or purge already took the copy's shards.</summary>
    internal static Task<bool> IsRetiredAsync(IGrainFactory grainFactory, string physicalTreeId) =>
        grainFactory.GetGrain<ITreeDeletionGrain>(physicalTreeId).IsPhysicalDeletedAsync();

    private static bool IsKept(string id, string aliasTarget, string? revertTarget) =>
        string.Equals(id, aliasTarget, StringComparison.Ordinal)
        || string.Equals(id, revertTarget, StringComparison.Ordinal);

    private static TreeRegistryEntry? Find(List<(string Id, TreeRegistryEntry Entry)> shadows, string id)
    {
        foreach (var (shadowId, entry) in shadows)
        {
            if (string.Equals(shadowId, id, StringComparison.Ordinal))
            {
                return entry;
            }
        }

        return null;
    }

    /// <summary>
    /// The registered restore shadows of <paramref name="treeId"/>, in ordinal order:
    /// the rows under its shadow prefix whose provenance names it.
    /// </summary>
    private static async Task<List<(string Id, TreeRegistryEntry Entry)>> ReadShadowsAsync(
        ILatticeRegistry registry, string treeId)
    {
        var candidates = await registry.GetAllTreeIdsAsync(treeId + ShadowInfix).ConfigureAwait(false);
        var shadows = new List<(string, TreeRegistryEntry)>(candidates.Count);
        for (var start = 0; start < candidates.Count; start += EntryBatchSize)
        {
            var count = Math.Min(EntryBatchSize, candidates.Count - start);
            var batch = new string[count];
            for (var i = 0; i < count; i++)
            {
                batch[i] = candidates[start + i];
            }

            var entries = await registry.GetEntriesAsync(batch).ConfigureAwait(false);
            foreach (var id in batch)
            {
                if (entries.TryGetValue(id, out var entry)
                    && string.Equals(entry.RestoreShadowOfTreeId, treeId, StringComparison.Ordinal))
                {
                    shadows.Add((id, entry));
                }
            }
        }

        shadows.Sort(static (left, right) => string.CompareOrdinal(left.Item1, right.Item1));
        return shadows;
    }
}
