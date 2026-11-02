using System.Text;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// What a user asked to export: which tree, which of its keys, and in which format.
/// It carries values only, because the download it becomes runs outside the circuit.
/// </summary>
internal sealed record DataExportRequest
{
    /// <summary>The longest file-name stem the export suggests.</summary>
    internal const int MaxFileStem = 80;

    /// <summary>The tree id the Data API reads, as the caller names it under their tenant.</summary>
    public required string TreeId { get; init; }

    /// <summary>The tree's name as the user reads it, which the file is named after.</summary>
    public required string TreeName { get; init; }

    /// <summary>Which keys to export.</summary>
    public DataExportScopeKind Scope { get; init; }

    /// <summary>The key prefix, for <see cref="DataExportScopeKind.Prefix"/>.</summary>
    public string? Prefix { get; init; }

    /// <summary>The inclusive lower key, for <see cref="DataExportScopeKind.Range"/>, or <see langword="null"/> to start at the first key.</summary>
    public string? From { get; init; }

    /// <summary>The exclusive upper key, for <see cref="DataExportScopeKind.Range"/>, or <see langword="null"/> to run to the last key.</summary>
    public string? To { get; init; }

    /// <summary>The file format.</summary>
    public DataExportFormat Format { get; init; }

    /// <summary>
    /// The half-open key range the scope reads: <c>[Start, End)</c>, either end
    /// <see langword="null"/> for unbounded. A prefix reads up to the first key that
    /// no longer starts with it.
    /// </summary>
    public (string? Start, string? End) Bounds => Scope switch
    {
        DataExportScopeKind.Prefix when !string.IsNullOrEmpty(Prefix) => (Prefix, DataReader.PrefixUpperBound(Prefix)),
        DataExportScopeKind.Range => (string.IsNullOrEmpty(From) ? null : From, string.IsNullOrEmpty(To) ? null : To),
        _ => (null, null),
    };

    /// <summary>The response content type for <see cref="Format"/>.</summary>
    public string ContentType => Format == DataExportFormat.Csv ? "text/csv; charset=utf-8" : "application/x-ndjson";

    /// <summary>The file name the browser saves the export as: the tree's name, made safe, with the format's extension.</summary>
    public string FileName => SafeFileStem(TreeName) + (Format == DataExportFormat.Csv ? ".csv" : ".ndjson");

    /// <summary>
    /// A file-name stem made from a tree's name: ASCII letters, digits, <c>-</c>, <c>_</c> and
    /// <c>.</c> kept, anything else written as <c>-</c>, at most <see cref="MaxFileStem"/>
    /// characters, and <c>export</c> when nothing is left.
    /// </summary>
    /// <param name="treeName">The tree's name.</param>
    /// <returns>The stem.</returns>
    internal static string SafeFileStem(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        var stem = new StringBuilder(Math.Min(treeName.Length, MaxFileStem));
        foreach (var c in treeName)
        {
            if (stem.Length == MaxFileStem)
            {
                break;
            }

            stem.Append(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '-');
        }

        var name = stem.ToString().Trim('-', '.');
        return name.Length == 0 ? "export" : name;
    }

    /// <summary>Why the request cannot run, in plain words, or <see langword="null"/> when it can.</summary>
    /// <returns>The problem, or <see langword="null"/>.</returns>
    public string? Validate() => Scope switch
    {
        DataExportScopeKind.Prefix when string.IsNullOrEmpty(Prefix) => "Enter the prefix the keys start with.",
        DataExportScopeKind.Range when string.IsNullOrEmpty(From) && string.IsNullOrEmpty(To) => "Enter a first key, a last key, or both.",
        DataExportScopeKind.Range when !string.IsNullOrEmpty(From) && !string.IsNullOrEmpty(To) && string.CompareOrdinal(From, To) >= 0
            => "The range ends before it starts: the first key must sort before the key the range stops at.",
        _ => null,
    };
}
