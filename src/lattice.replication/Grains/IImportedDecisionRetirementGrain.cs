using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Retires the saga decision rows a bootstrap imported into a tree's transaction
/// registry (issue #4524), keyed by the receiver's logical tree name. The drain
/// records every decision row the export carried so a pre-cut prepare the
/// source re-ships afterwards settles against it (#4482); this grain forgets
/// those rows once the incremental stream from the source has passed the
/// export's cut on every write-ahead-log partition - the source's shipper has
/// vouched acknowledged positions on the exported log at or past every tail the
/// export captured - so no pre-cut prepare can still arrive. Rows imported from
/// a source that did not capture the boundary are retained.
/// </summary>
[Alias(ReplicationTypeAliases.IImportedDecisionRetirementGrain)]
internal interface IImportedDecisionRetirementGrain : IGrainWithStringKey
{
    /// <summary>
    /// Durably records the decision rows <paramref name="transactionIds"/> a
    /// bootstrap from <paramref name="sourceClusterId"/> imported, together with
    /// the export's own boundary (<see langword="null"/> when the source did not
    /// capture one, which retains the rows). Unions with rows a prior import
    /// from the same source recorded and takes the newer boundary, which covers
    /// the older one. Then retires whatever has already passed.
    /// </summary>
    Task RegisterAsync(string sourceClusterId, CrossTreeSiblingBoundary? exportBoundary, ImmutableArray<Guid> transactionIds);

    /// <summary>
    /// Forgets every recorded row whose source has passed its export boundary,
    /// and returns how many rows remain retained.
    /// </summary>
    Task<int> RetireAsync();
}
