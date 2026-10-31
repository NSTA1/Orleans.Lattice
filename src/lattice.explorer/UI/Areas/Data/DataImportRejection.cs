namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// One line an import did not write (issue #5365): the line it was on, when it is known,
/// the key it named, when it had one, why it was refused, and the refusal's code.
/// </summary>
/// <param name="Line">The 1-based line, or <see langword="null"/> when the write it refers to cannot be traced to one.</param>
/// <param name="Key">The key, or <see langword="null"/> when the line named none.</param>
/// <param name="Code">The code: <c>format</c> for a line the import could not read, or the Data API's refusal code.</param>
/// <param name="Reason">Why, as a sentence a person can act on.</param>
internal sealed record DataImportRejection(long? Line, string? Key, string Code, string Reason)
{
    /// <summary>The code of a line the import could not read.</summary>
    public const string FormatCode = "format";
}
