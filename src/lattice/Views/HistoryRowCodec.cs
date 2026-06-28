using Orleans.Serialization;

namespace Orleans.Lattice.Views;

/// <summary>
/// Binary (de)serialisation for a <see cref="HistoryRow"/> stored as the value of
/// a durable history-view entry. Uses the Orleans serializer (not a manual
/// encoding) because the row is part of the <b>public</b> wire surface: the
/// history read path and the State API decode the stored bytes back into a
/// <see cref="HistoryRow"/> with the same generated serializer, so the stored
/// shape must be the Orleans wire format.
/// <para>
/// Registered as a singleton by <c>AddLatticeViews</c>; resolved by the history
/// projection (to encode the row it emits) and by the view maintainer (to decode,
/// reshape per the retention mode, and re-encode each emitted row at drain time).
/// </para>
/// </summary>
internal sealed class HistoryRowCodec(Serializer<HistoryRow> serializer)
{
    /// <summary>Serialises <paramref name="row"/> to its Orleans wire bytes.</summary>
    public byte[] Encode(in HistoryRow row) => serializer.SerializeToArray(row);

    /// <summary>Deserialises a <see cref="HistoryRow"/> produced by <see cref="Encode"/>.</summary>
    public HistoryRow Decode(byte[] bytes) => serializer.Deserialize(bytes);
}
