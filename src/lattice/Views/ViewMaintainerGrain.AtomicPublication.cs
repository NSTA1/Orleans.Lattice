using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    private List<Guid> LocalPublicationClosure(Guid seed, List<Guid> completed)
    {
        var closure = new List<Guid> { seed };
        if (_staging[seed].CrossTreeOperationId is not null)
        {
            return closure;
        }

        var keys = new HashSet<string>(_staging[seed].StagedSourceKeys, StringComparer.Ordinal);
        bool changed;
        do
        {
            changed = false;
            foreach (var candidate in completed)
            {
                if (closure.Contains(candidate)
                    || !_staging.TryGetValue(candidate, out var tx)
                    || tx.CrossTreeOperationId is not null
                    || !tx.StagedSourceKeys.Overlaps(keys))
                {
                    continue;
                }

                closure.Add(candidate);
                keys.UnionWith(tx.StagedSourceKeys);
                changed = true;
            }
        }
        while (changed);

        return closure;
    }

    private async Task<int> PublishLocalFilterClosureAsync(
        ILattice viewTree,
        ViewRegistration registration,
        List<Guid> closure,
        CancellationToken cancellationToken)
    {
        var writes = new List<ViewWrite>();
        foreach (var prepared in PublicationWinners(closure))
        {
            writes.AddRange(registration.Projection!.Project(ProjectionInput(registration, prepared)));
        }

        var coalesced = ViewWriteCoalescer.Coalesce(writes);
        var upserts = new List<KeyValuePair<string, byte[]>>(coalesced.Count);
        var deletes = new List<string>(coalesced.Count);
        foreach (var write in coalesced)
        {
            if (write.Kind == ViewWriteKind.Upsert)
            {
                upserts.Add(new(write.Key, write.Value!));
            }
            else if (write.Kind == ViewWriteKind.Delete)
            {
                deletes.Add(write.Key);
            }
        }

        await PublishLocalClosureSliceAsync(viewTree, closure, upserts, deletes, cancellationToken);
        return upserts.Count + deletes.Count;
    }

    private async Task<int> PublishLocalAggregationClosureAsync(
        ILattice viewTree,
        ViewRegistration registration,
        List<Guid> closure,
        CancellationToken cancellationToken)
    {
        var contributions = new List<AggregationContribution>();
        foreach (var prepared in PublicationWinners(closure))
        {
            contributions.AddRange(registration.AggregationProjection!.Project(ProjectionInput(registration, prepared)));
        }

        contributions.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        var buffering = new BufferingAggregationViewStore(new LatticeViewStore(viewTree));
        var capturing = CreateAggregationApplierOver(buffering);
        foreach (var contribution in contributions)
        {
            await capturing.ApplyAsync(contribution, cancellationToken);
        }

        var (upserts, deletes) = buffering.Capture();
        await PublishLocalClosureSliceAsync(viewTree, closure, upserts, deletes, cancellationToken);
        return upserts.Count + deletes.Count;
    }

    private IEnumerable<LatticeMutation> PublicationWinners(List<Guid> closure)
    {
        var winners = new Dictionary<string, LatticeMutation>(StringComparer.Ordinal);
        foreach (var txId in closure)
        {
            var tx = _staging[txId];
            foreach (var prepared in tx.PreparesByIndex.Values)
            {
                if (IsSupersededByOrdinary(prepared) || IsSourceSuperseded(tx, prepared))
                {
                    continue;
                }

                if (!winners.TryGetValue(prepared.Key, out var prior)
                    || prepared.Timestamp.CompareTo(prior.Timestamp) > 0)
                {
                    winners[prepared.Key] = prepared;
                }
            }
        }

        return winners.Values;
    }

    private async Task PublishLocalClosureSliceAsync(
        ILattice viewTree,
        List<Guid> closure,
        List<KeyValuePair<string, byte[]>> upserts,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        if (upserts.Count > 0 || deletes.Count > 0)
        {
            // Crossed per-key clocks admit no serial transaction order. Publish
            // the complete overlapping ready closure in one visibility change.
            closure.Sort();
            var identity = string.Join(",", closure.Select(id => id.ToString("N")));
            var operationId = "mv-closure-" + Convert.ToHexString(SHA256.HashData(Encoding.ASCII.GetBytes(identity)));
            await viewTree.SetManyAtomicAsync(upserts, deletes, operationId, cancellationToken);
            await ReassertResolvedSliceAsync(viewTree, upserts, deletes, cancellationToken);
        }

        foreach (var txId in closure)
        {
            var tx = _staging[txId];
            RecordFlushedPreparesOverStagedKeys(tx);
            ReleaseStagedKeys(tx);
            _staging.Remove(txId);
            MarkResolved(txId);
        }
    }
}
