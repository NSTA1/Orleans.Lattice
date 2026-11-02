namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The file format an import reads (issue #5365).</summary>
internal enum DataImportFormat
{
    /// <summary>Newline-delimited JSON: one JSON value per line, the key from a pointer or a template.</summary>
    Ndjson,

    /// <summary>RFC 4180 CSV with a header row, the key from a column or a template.</summary>
    Csv,

    /// <summary>A <c>lattice-export/1</c> NDJSON file, as the Export dialog writes it: re-imports exactly.</summary>
    LatticeExport,

    /// <summary><c>key=value</c> lines, the value stored as UTF-8 text.</summary>
    KeyValue,
}
