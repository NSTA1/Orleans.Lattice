namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The file format a data export is written in.</summary>
internal enum DataExportFormat
{
    /// <summary>Newline-delimited JSON: one entry object per line, then the trailer line.</summary>
    Ndjson = 0,

    /// <summary>RFC 4180 CSV with a <c>key,value,encoding</c> header, then one row per entry and the trailer row.</summary>
    Csv = 1,
}
