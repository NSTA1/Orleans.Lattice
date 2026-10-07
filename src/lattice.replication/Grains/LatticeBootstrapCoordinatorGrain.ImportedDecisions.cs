using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

internal sealed partial class LatticeBootstrapCoordinatorGrain
{
    /// <summary>
    /// Hands the decision rows the drain recorded to the tree's
    /// <see cref="IImportedDecisionRetirementGrain"/> with the export's own
    /// boundary (issue #4524), which forgets them once the incremental stream
    /// from <paramref name="sourceClusterId"/> has passed it. Idempotent, so a
    /// drain resumed after a crash registers the same rows again.
    /// </summary>
    private async Task RegisterImportedDecisionsAsync(
        string treeName, string? sourceClusterId, SnapshotStream snapshot, HashSet<Guid> importedDecisions)
    {
        if (importedDecisions.Count == 0 || string.IsNullOrEmpty(sourceClusterId))
        {
            return;
        }

        await _grainFactory.GetGrain<IImportedDecisionRetirementGrain>(treeName)
            .RegisterAsync(sourceClusterId, snapshot.ExportBoundary, [.. importedDecisions])
            .ConfigureAwait(true);
    }
}
