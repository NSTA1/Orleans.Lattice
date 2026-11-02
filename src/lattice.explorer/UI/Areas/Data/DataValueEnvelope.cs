using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Strips the schema-version envelope from stored bytes the Explorer previews.
/// Reads through the data API return a value's body, because the cluster's read
/// decoder strips the envelope at the client-facing boundary. Surfaces that carry
/// the stored form instead - a key's history, dead letters, a remediation's
/// offending value - would otherwise show the 10-byte header and draw the value
/// as hexadecimal (#5583). The strip is mechanical and never upcasts: a revision
/// is shown in the shape it was written in, and its schema version is reported
/// alongside rather than lost.
/// </summary>
internal static class DataValueEnvelope
{
    /// <summary>The body of <paramref name="value"/>: the bytes with any schema-version envelope stripped.</summary>
    /// <param name="value">The stored bytes, or a preview of them that holds the whole header.</param>
    /// <returns>The body, or <paramref name="value"/> itself when it carries no envelope.</returns>
    public static byte[] Body(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return LatticeSchemaEnvelope.IsEnveloped(value) ? LatticeSchemaEnvelope.StripToBody(value) : value;
    }

    /// <summary>
    /// Projects a history revision for display, its value preview stripped of any
    /// envelope, its size that of the body, and its schema version recorded on the row.
    /// </summary>
    /// <param name="record">The revision as the state API returned it.</param>
    /// <returns>The display row.</returns>
    public static HistoryRevisionRow Revision(EntryRevisionRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.ValuePreview is not { } preview
            || !LatticeSchemaEnvelope.TryReadHeader(preview, out _, out var version))
        {
            return HistoryRevisionRow.From(record);
        }

        var body = record with
        {
            ValuePreview = LatticeSchemaEnvelope.StripToBody(preview),
            ValueLength = Math.Max(0, record.ValueLength - LatticeSchemaEnvelope.HeaderLength),
        };

        return HistoryRevisionRow.From(body) with { SchemaVersion = version };
    }
}
