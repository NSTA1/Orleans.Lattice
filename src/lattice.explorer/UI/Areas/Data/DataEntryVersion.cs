using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The version of an entry the value editor opened, compared again at save to
/// detect a write in between: whether the key held a live value, and the
/// hybrid-logical-clock stamp of its latest write (a delete included).
/// </summary>
/// <param name="Exists">Whether the key held a live value.</param>
/// <param name="Hlc">The stamp of the key's latest write or delete; the default for a key never written.</param>
internal readonly record struct DataEntryVersion(bool Exists, HybridLogicalClock Hlc)
{
    /// <summary>The version of <paramref name="entry"/>, or of an absent key when it is <see langword="null"/>.</summary>
    /// <param name="entry">The entry as the state API returned it.</param>
    public static DataEntryVersion Of(DataEntry? entry) =>
        entry is null ? default : new DataEntryVersion(!entry.IsTombstone, entry.Hlc);

    /// <summary>The body a person edits: the stored bytes with any schema version envelope stripped.</summary>
    /// <param name="value">The stored bytes.</param>
    public static byte[] Body(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return LatticeSchemaEnvelope.IsEnveloped(value) ? LatticeSchemaEnvelope.StripToBody(value) : value;
    }
}
