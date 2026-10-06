using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The replication <see cref="ICrossTreeDecisionStamper"/> (issue #4684): stamps
/// a cross-tree write's decision with each participating tree's snapshot export
/// epoch. Every export advances its tree's epoch on the single
/// <see cref="IReplicationExportEpochGrain"/> before it opens, and the stamp is
/// read after the decision is durable, so an export whose epoch is greater than
/// a tree's stamp opened after the decision.
/// </summary>
internal sealed class ReplicationCrossTreeDecisionStamper(IGrainFactory grainFactory) : ICrossTreeDecisionStamper
{
    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, long>> StampAsync(
        string operationId, IReadOnlyList<string> participants, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(participants);
        var reads = new Task<long>[participants.Count];
        for (var i = 0; i < participants.Count; i++)
        {
            reads[i] = grainFactory.GetGrain<IReplicationExportEpochGrain>(participants[i]).GetAsync();
        }

        var epochs = await Task.WhenAll(reads);
        var stamps = new Dictionary<string, long>(participants.Count, StringComparer.Ordinal);
        for (var i = 0; i < participants.Count; i++)
        {
            stamps[participants[i]] = epochs[i];
        }

        return stamps;
    }
}
