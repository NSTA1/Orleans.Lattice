namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// What an import reads and how (issue #5365): the format, the mode, where each key and
/// value comes from, and the CSV options.
/// </summary>
internal sealed record DataImportSettings
{
    /// <summary>The file format.</summary>
    public DataImportFormat Format { get; init; } = DataImportFormat.Ndjson;

    /// <summary>How the import writes into its tree.</summary>
    public DataImportMode Mode { get; init; } = DataImportMode.Upsert;

    /// <summary>
    /// Where each key comes from: a JSON Pointer or pointer template (NDJSON), or a column
    /// or column template (CSV). Ignored for <see cref="DataImportFormat.LatticeExport"/>
    /// and <see cref="DataImportFormat.KeyValue"/>, whose keys are fixed by the format.
    /// </summary>
    public string? KeyTemplate { get; init; }

    /// <summary>Where each value comes from.</summary>
    public DataImportValueSource ValueSource { get; init; } = DataImportValueSource.Record;

    /// <summary>The JSON Pointer (NDJSON) or column (CSV) the value is read from when <see cref="ValueSource"/> is <see cref="DataImportValueSource.Field"/>.</summary>
    public string? ValueField { get; init; }

    /// <summary>The CSV field delimiter.</summary>
    public char Delimiter { get; init; } = ',';

    /// <summary>
    /// For a CSV row written as a JSON object, whether a field that reads as a JSON number
    /// or <c>true</c>/<c>false</c> is written as one rather than as a string.
    /// </summary>
    public bool InferTypes { get; init; }

    /// <summary>Whether the format reads keys through <see cref="KeyTemplate"/>.</summary>
    public bool UsesKeyTemplate => Format is DataImportFormat.Ndjson or DataImportFormat.Csv;

    /// <summary>The delimiters the CSV reader accepts.</summary>
    internal static IReadOnlyList<char> Delimiters { get; } = [',', ';', '\t', '|'];

    /// <summary>Checks the settings, returning what is wrong or <see langword="null"/>.</summary>
    /// <returns>A sentence a person can act on, or <see langword="null"/>.</returns>
    public string? Validate()
    {
        if (UsesKeyTemplate && !DataImportKeyTemplate.TryParse(KeyTemplate, Format == DataImportFormat.Ndjson, out _, out var keyError))
        {
            return keyError;
        }

        if (UsesKeyTemplate && ValueSource == DataImportValueSource.Field)
        {
            if (string.IsNullOrWhiteSpace(ValueField))
            {
                return Format == DataImportFormat.Ndjson
                    ? "Say which value to store: a JSON Pointer such as /payload."
                    : "Say which column to store as the value.";
            }

            if (Format == DataImportFormat.Ndjson && !DataImportJsonPointer.TryParse(ValueField, out _, out var pointerError))
            {
                return pointerError;
            }
        }

        if (Format == DataImportFormat.Csv && !Delimiters.Contains(Delimiter))
        {
            return "Choose a comma, semicolon, tab or vertical bar as the delimiter.";
        }

        return null;
    }
}
