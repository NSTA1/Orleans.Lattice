namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// One record an import read from its file (issue #5365): the entry it yields, or why the
/// line yields none. <see cref="Line"/> is the 1-based line the record starts on.
/// </summary>
/// <param name="Line">The 1-based line the record starts on.</param>
/// <param name="Entry">The entry, or <see langword="null"/> when the line is refused.</param>
/// <param name="Error">Why the line is refused, or <see langword="null"/>.</param>
internal readonly record struct DataImportRecord(long Line, Orleans.Lattice.Api.Data.DataEntry? Entry, string? Error);
