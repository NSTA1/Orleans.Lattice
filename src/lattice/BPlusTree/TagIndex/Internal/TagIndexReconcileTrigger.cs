using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Default <see cref="ITagIndexReconcileTrigger"/>. Discovers the tag indexes
/// covering a swapped subject tree by listing the registered index trees (named
/// <c>tag-{indexName}</c>) from the tree registry - there is no forward
/// tree-to-indexes map, so the sibling index trees are the discovery surface -
/// and fires a coverage-gated reconcile on each index's reconciliation
/// coordinator.
/// </summary>
internal sealed class TagIndexReconcileTrigger(
    IGrainFactory grainFactory,
    ILogger<TagIndexReconcileTrigger> logger)
    : ITagIndexReconcileTrigger
{
    private const string IndexTreeIdPrefix = "tag-";

    public async Task TriggerForTreeAsync(string subjectTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subjectTreeId);

        IReadOnlyList<string> treeIds;
        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

            // Push the tag-index prefix down: a bounded range scan over the sorted
            // registry rather than a full catalog read whose ids are then filtered
            // to the tag- prefixed ones below.
            treeIds = await registry.GetAllTreeIdsAsync(IndexTreeIdPrefix).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Best-effort: the scheduled reconcile sweep is the correctness
            // backstop, so a failure to enumerate must not fault the swap.
            logger.LogWarning(
                ex,
                "Tag-index reconcile trigger could not enumerate registered trees for swapped tree '{TreeId}'; relying on the scheduled sweep.",
                subjectTreeId);
            return;
        }

        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!treeId.StartsWith(IndexTreeIdPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var indexName = treeId[IndexTreeIdPrefix.Length..];
            try
            {
                await grainFactory.GetGrain<ITagIndexReconcileGrain>(indexName)
                    .ReconcileTreeAsync(subjectTreeId)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Tag-index reconcile trigger failed for index '{IndexName}' after a physical-identity swap of tree '{TreeId}'; relying on the scheduled sweep.",
                    indexName,
                    subjectTreeId);
            }
        }
    }
}
