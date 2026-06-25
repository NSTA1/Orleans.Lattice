using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="IWalRecordEncoder"/> implementation that writes
/// <see cref="WalRecord"/> bytes through the canonical
/// <see cref="Serializer{T}"/> from <c>Orleans.Serialization</c>. Has
/// no per-call state beyond the bytes the caller supplies through the
/// destination <see cref="IBufferWriter{T}"/>; the underlying
/// serializer is itself thread-safe and stateless.
/// <para>
/// Registered as a singleton from
/// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> so the
/// codec stays warm across every WAL append. Hosts that wish to
/// substitute a different wire format register their own
/// implementation before that call (the default registration uses
/// <c>TryAddSingleton</c>).
/// </para>
/// </summary>
public sealed class OrleansBinaryWalRecordEncoder(Serializer<WalRecord> serializer) : IWalRecordEncoder
{
    private readonly Serializer<WalRecord> _serializer = serializer
        ?? throw new ArgumentNullException(nameof(serializer));

    /// <inheritdoc />
    public void Encode(in WalRecord record, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        // Strip the redundant TreeId slot (since v4) before
        // serialisation: every storage and transport seam recovers
        // the tree id from surrounding context (storage partition
        // key, framing header TreeName tail, shipper grain key), so
        // persisting it on every entry duplicates ~25-35 bytes per
        // entry for production tree names.
        //
        // Strip the redundant Value slot on CRDT-mode Set entries
        // that carry a typed Delta: the receiver-side apply
        // path dispatches every typed CRDT mode (OrSet, PnCounter,
        // VersionVector, MvRegister, OrMap) through WalRecord.Delta
        // + the primitive's MergeDelta, so the full-state Value byte
        // payload is pure overhead on both the storage WAL and the
        // cross-cluster wire. Skip-serialise Value when Mode is a
        // typed CRDT mode and Delta is non-null; LwwRegister entries
        // (whose Value remains the canonical payload) and CRDT
        // entries that for whatever reason ship without a Delta (a
        // legacy producer, a hand-constructed entry in a test) keep
        // Value verbatim. Prepared saga entries (IsPrepared) also keep
        // Value: the receiver buckets the merged-state value into its
        // per-tx pending bucket and folds the typed Delta into the
        // visible state only on the terminal commit, so the prepared
        // apply path requires a non-null Value on the wire. The producer's in-grain WalRecord instance
        // still carries Value in memory and the leaf store continues
        // to hold the canonical post-merge state; this strip is
        // scoped to the encoded bytes only.
        //
        // Mode is NOT stripped here: since wire id 26 the WalRecord type
        // tags the slot with [Id(26)], so the canonical Orleans
        // serializer persists it (omitting the bytes when it holds the
        // enum default LwwRegister). Persisting the mode is what makes a
        // delta-only CRDT record self-describing on the durable storage
        // replay path, which - unlike the cross-cluster ship path - has
        // no per-batch framing header to recover it from (issue #926).
        // The cross-cluster receiver still re-stamps the mode from the
        // framing header via Decode(span, treeId, mode); that override is
        // now idempotent because the decoded record already carries the
        // same mode from its own bytes.
        var stripValue = record.Op == MutationKind.Set
            && record.Mode != LatticeMergeMode.LwwRegister
            && record.Delta is not null
            && !record.IsPrepared;
        var stripTreeId = record.TreeId.Length != 0;
        if (!stripValue && !stripTreeId)
        {
            _serializer.Serialize(record, writer);
            return;
        }
        var stripped = record with
        {
            TreeId = stripTreeId ? string.Empty : record.TreeId,
            Value = stripValue ? null : record.Value,
        };
        _serializer.Serialize(stripped, writer);
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded)
    {
        // Serializer<T> exposes a span overload of Deserialize that
        // avoids copying the bytes; we delegate directly. The returned
        // record carries TreeId == string.Empty (stripped on encode) but
        // Mode is now recovered verbatim from the bytes (wire id 26;
        // LwwRegister for records whose mode was the default or that
        // pre-date the tagged slot). Forensic tooling that calls this
        // single-argument overload accepts the empty-TreeId invariant.
        // Call sites with the tree id in hand should call the
        // Decode(span, treeId) overload to restore it.
        return _serializer.Deserialize(encoded);
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = _serializer.Deserialize(encoded);
        // Re-stamp TreeId from the caller-supplied context. The
        // producer's Encode stripped this slot; this overload is the
        // seam where it is restored. Mode is recovered verbatim from the
        // bytes (wire id 26), so the durable storage replay path gets the
        // authored merge mode with no resolver dependency. Cross-cluster
        // apply seams that carry the framing header's Mode forward should
        // still use the Decode(span, treeId, mode) overload; that
        // override is idempotent against the now-durable slot.
        return record with { TreeId = treeId };
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId, LatticeMergeMode mode)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = _serializer.Deserialize(encoded);
        // Re-stamp both TreeId and Mode from the caller-supplied
        // batch-level context. The framing header carries Mode once
        // per batch since wire version 5; the override is retained for
        // that path and is idempotent against the now-durable Mode slot
        // (wire id 26) because the decoded record already carries the
        // same authored mode from its own bytes.
        return record with { TreeId = treeId, Mode = mode };
    }
}
