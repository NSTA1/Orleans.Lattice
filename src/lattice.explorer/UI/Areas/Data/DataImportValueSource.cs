namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>Where an import takes each entry's value from (issue #5365).</summary>
internal enum DataImportValueSource
{
    /// <summary>NDJSON: the line's bytes, verbatim. CSV: the row as a JSON object of header name to field.</summary>
    Record,

    /// <summary>NDJSON: the JSON value at a pointer. CSV: one column's raw text.</summary>
    Field,
}
