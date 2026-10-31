using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaDeadLetterStore"/>. Dogfoods the reserved
/// <c>sys-schema-dlq</c> <c>ILattice</c> tree: each entry is stored under a
/// time-ordered composite key (<see cref="SchemaDeadLetterKey"/>) so a tree's
/// entries are a single contiguous prefix scan. Writes run under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> so the diversion is
/// not itself re-validated or blocked by the access gate, and under
/// <see cref="LatticeCommittedWriteBoundary"/> so an append is always an ordinary
/// committed write, never part of the diverted write's transaction.
/// </summary>
internal sealed class LatticeSchemaDeadLetterStore(IGrainFactory grainFactory) : ILatticeSchemaDeadLetterStore
{
    private ILattice Queue => grainFactory.GetGrain<ILattice>(SchemaConstants.DeadLetterTree);

    /// <inheritdoc />
    public async Task AppendAsync(string treeId, LatticeSchemaDeadLetterEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(entry);

        var unique = Guid.NewGuid().ToString("N")[..8];
        var key = SchemaDeadLetterKey.Encode(treeId, entry.TimestampUtc, entry.Key, unique);

        // A diversion is judged while the diverted write's own context is ambient:
        // a replicated atomic-batch prepare carries its transaction id and prepare
        // flag, and every replicated entry its origin, clock and delta. The record
        // is an ordinary committed write of its own, so it is detached from all of
        // them; inheriting a prepare would stage it in a pending bucket of this tree
        // that no terminal ever commits (#5297).
        using (LatticeCommittedWriteBoundary.Enter())
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Queue.SetAsync(key, entry, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListAsync(
        string treeId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var start = SchemaDeadLetterKey.PrefixStart(treeId);
        var end = SchemaDeadLetterKey.PrefixEnd(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in Queue
                .ScanEntriesAsync<LatticeSchemaDeadLetterEntry>(start, end, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                // Tree ids may contain the delimiter. Check the complete key
                // against the stored timestamp and item key, not just its prefix.
                if (entry.Value is { } deadLetter && SchemaDeadLetterKey.BelongsToTree(entry.Key, treeId, deadLetter))
                {
                    yield return deadLetter;
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var count = 0;
        await foreach (var _ in ListAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            count++;
        }
        return count;
    }
}
