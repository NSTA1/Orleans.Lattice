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
    ILatticeMergeModeResolver modeResolver,
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
        ArgumentNullException.ThrowIfNull(modeResolver);
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(entries);

        if (entries.Count == 0)
        {
            return 0;
        }

        // Re-stamp every entry's TreeId AND Mode from the batch tree name
        // before framing. This egress ships a per-tree batch through the typed
        // ReplicationBatchEnvelope, which the receiver decodes verbatim -
        // unlike the ordinary shipper's framing path, which strips the
        // batch-constant TreeId at encode time and re-stamps both fields from
        // the framing header on decode.
        //
        // TreeId (#1331): WAL-sourced re-replay entries (WalGrainReReplaySource)
        // arrive here with an empty TreeId - the durable WAL codec strips that
        // redundant slot on encode and the LatticeMutation read-back does not
        // restore it. An entry that reaches the receiver with an empty TreeId
        // is rejected by the applier ("WalRecord.TreeId must be non-empty") and
        // cannot even be quarantined (the per-tree dead-letter/high-water-mark
        // grains cannot be keyed on an empty id), so a single such entry wedges
        // the peer in an unbounded re-ship loop and blocks convergence.
        //
        // Mode (#1334): the WAL codec likewise omits the Mode slot whenever it
        // holds the enum default (LwwRegister), and the storage read-back has
        // no framing header to restore it - so WAL-sourced re-replay entries,
        // and bootstrap-fallback entries re-derived from the live projection
        // (which are constructed with no Mode), surface with Mode=LwwRegister.
        // For any tree whose fixed merge mode is not LwwRegister (every CRDT
        // mode, and the internal sys-replication-config OR-Map tree), the
        // receiver's merge-mode gate rejects a wire mode that disagrees with
        // its locally resolved mode, so anti-entropy could never heal those
        // trees. Resolving the mode once per batch from the tree name mirrors
        // the ordinary ChangeFeed shipper's re-stamp exactly and is correct
        // because the envelope is strictly per-tree: every entry in the batch
        // belongs to treeName, and a tree has exactly one fixed merge mode.
        var resolvedMode = modeResolver.Resolve(treeName) ?? LatticeMergeMode.LwwRegister;
        var stamped = new WalRecord[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            stamped[i] = entries[i] with { TreeId = treeName, Mode = resolvedMode };
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
