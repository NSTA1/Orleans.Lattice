using System.Buffers;
using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The production <see cref="ILeafReReplaySink"/>: frames the selected entries
/// into a <see cref="ReplicationBatchEnvelope"/> and re-ships them through the
/// ordinary <see cref="IReplicationTransport"/> so the repair travels the same
/// causal-stable apply pipeline as ordinary replication. Returns the entry
/// count when the peer accepts the batch and zero when it rejects it.
/// </summary>
internal sealed class TransportLeafReReplaySink(
    IReplicationTransport transport,
    IReplicationBatchEncoder encoder,
    string originClusterId) : ILeafReReplaySink
{
    /// <inheritdoc />
    public async ValueTask<int> ReplayAsync(
        string peer,
        string treeName,
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(encoder);
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(entries);

        if (entries.Count == 0)
        {
            return 0;
        }

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 0,
            TreeName = treeName,
            OriginClusterId = originClusterId,
            Entries = entries,
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(envelope, writer);

        var batch = new ReplicationBatch
        {
            TargetClusterId = peer,
            TreeName = treeName,
            OriginClusterId = originClusterId,
            Payload = writer.WrittenMemory,
            Envelope = envelope,
        };

        var ack = await transport.SendAsync(batch, cancellationToken).ConfigureAwait(false);
        return ack.Accepted ? entries.Count : 0;
    }
}
