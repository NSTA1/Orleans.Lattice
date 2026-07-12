using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaDeadLetterStore"/>. Dogfoods the reserved
/// <c>sys-schema-dlq</c> <c>ILattice</c> tree: each entry is stored under a
/// time-ordered composite key (<see cref="SchemaDeadLetterKey"/>) so a tree's
/// entries are a single contiguous prefix scan. Writes run under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> so the diversion is
/// not itself re-validated or blocked by the access gate.
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
                if (entry.Value is { } deadLetter)
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

        var start = SchemaDeadLetterKey.PrefixStart(treeId);
        var end = SchemaDeadLetterKey.PrefixEnd(treeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await Queue.CountAsync(start, end, cancellationToken).ConfigureAwait(false);
        }
    }
}
