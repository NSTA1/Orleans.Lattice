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

        // Re-stamp every entry's TreeId from the batch tree name before
        // framing. This egress ships a per-tree batch through the typed
        // ReplicationBatchEnvelope, which the receiver decodes verbatim -
        // unlike the ordinary shipper's framing path, which strips the
        // batch-constant TreeId at encode time and re-stamps it from the
        // framing tail's tree name on decode. WAL-sourced re-replay entries
        // (WalGrainReReplaySource) arrive here with an empty TreeId: the
        // durable WAL codec strips that redundant slot on encode and the
        // LatticeMutation read-back does not restore it. An entry that
        // reaches the receiver with an empty TreeId is rejected by the
        // applier ("WalRecord.TreeId must be non-empty") and cannot even be
        // quarantined (the per-tree dead-letter/high-water-mark grains
        // cannot be keyed on an empty id), so a single such entry wedges the
        // peer in an unbounded re-ship loop and blocks convergence. Stamping
        // from the batch tree name here mirrors the framing receiver's
        // re-stamp exactly and is correct because the envelope is strictly
        // per-tree: every entry in the batch belongs to treeName.
        var stamped = new WalRecord[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            stamped[i] = entries[i] with { TreeId = treeName };
        }

        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 0,
            TreeName = treeName,
            OriginClusterId = originClusterId,
            Entries = stamped,
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
