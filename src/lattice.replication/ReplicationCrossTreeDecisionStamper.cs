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

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, long>> IssueSequencesAsync(
        string operationId, IReadOnlyList<string> participants, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(participants);

        // Covered by the frontier before any sequence exists to cover.
        await grainFactory.GetGrain<ICrossTreePurgeFrontierSourceGrain>(ICrossTreePurgeFrontierSourceGrain.Key)
            .RegisterTreesAsync(participants);

        // Each participant's sequence lives on its own per-tree grain, and each
        // issue is a durable state write, so issuing them one after another cost
        // one full grain round trip plus storage write per tree on every
        // replicated cross-tree commit. They are independent - no grain reads
        // another's sequence - and IssueAsync is idempotent per operation id
        // (a retry returns the pending sequence already issued), so a partial
        // failure leaves exactly the state a failure part-way through the
        // sequential loop left, and the caller's retry converges it the same
        // way. They are issued together, as StampAsync reads the epochs.
        var issues = new Task<long>[participants.Count];
        for (var i = 0; i < participants.Count; i++)
        {
            issues[i] = grainFactory.GetGrain<ICrossTreeDecisionSequenceGrain>(participants[i]).IssueAsync(operationId);
        }

        var issued = await Task.WhenAll(issues);
        var sequences = new Dictionary<string, long>(participants.Count, StringComparer.Ordinal);
        for (var i = 0; i < participants.Count; i++)
        {
            sequences[participants[i]] = issued[i];
        }

        return sequences;
    }

    /// <inheritdoc />
    public async Task ConfirmSequencesAsync(
        string operationId, IReadOnlyList<string> participants, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(participants);

        // Independent per-tree grains, and ConfirmAsync is idempotent per
        // operation id (a no-op once nothing is pending), so the confirms are
        // issued together for the reason the issues are.
        var confirms = new Task[participants.Count];
        for (var i = 0; i < participants.Count; i++)
        {
            confirms[i] = grainFactory.GetGrain<ICrossTreeDecisionSequenceGrain>(participants[i]).ConfirmAsync(operationId);
        }

        await Task.WhenAll(confirms);
    }
}
