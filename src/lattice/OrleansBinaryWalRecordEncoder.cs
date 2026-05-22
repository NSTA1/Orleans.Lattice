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
        // Value verbatim. The producer's in-grain WalRecord instance
        // still carries Value in memory and the leaf store continues
        // to hold the canonical post-merge state; this strip is
        // scoped to the encoded bytes only.
        //
        // Mode is not stripped here because the WalRecord type itself
        // no longer marks the slot with [Id] - the canonical Orleans
        // serializer never writes the field, so there is nothing to
        // strip. The merge mode is hoisted into the framing header
        // (EncodedBatchHeader.Mode) once per batch and re-stamped on
        // decoded records by the Decode(span, treeId, mode) overload
        // below.
        var stripValue = record.Op == MutationKind.Set
            && record.Mode != LatticeMergeMode.LwwRegister
            && record.Delta is not null;
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
        // record carries TreeId == string.Empty (stripped on encode)
        // and Mode == LwwRegister (the field is not serialised, so it
        // always decodes to the enum default). Forensic tooling that
        // calls this single-argument overload accepts those
        // invariants. Call sites with the tree id and merge mode in
        // hand should call the Decode(span, treeId, mode) overload
        // instead.
        return _serializer.Deserialize(encoded);
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = _serializer.Deserialize(encoded);
        // Re-stamp TreeId from the caller-supplied context. The
        // producer's Encode stripped this slot; this overload is the
        // seam where it is restored. Mode is left at its enum default
        // (LwwRegister) - call sites that carry the framing header's
        // Mode field through (the receiver-side replication apply
        // seam) should use the Decode(span, treeId, mode) overload
        // instead.
        return record with { TreeId = treeId };
    }

    /// <inheritdoc />
    public WalRecord Decode(ReadOnlySpan<byte> encoded, string treeId, LatticeMergeMode mode)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var record = _serializer.Deserialize(encoded);
        // Re-stamp both TreeId and Mode from the caller-supplied
        // batch-level context. The framing header carries Mode once
        // per batch since wire version 5, and the WalRecord.Mode slot
        // is not serialised so it must be supplied here for the
        // apply path's mode-dispatch switch to work.
        return record with { TreeId = treeId, Mode = mode };
    }
}
