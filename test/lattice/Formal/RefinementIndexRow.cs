namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One mapping or exclusion row of a refinement note, as the refinement index
/// (contract C6 of #5148) records it.
/// </summary>
/// <param name="Section">The <c>##</c> heading the row sits under, for example <c>Action mapping</c>.</param>
/// <param name="Label">The row's first cell exactly as written, for example <c>`FoldBegin`</c>.</param>
/// <param name="Production">
/// The production symbols the row names outside its label and <c>Detector</c>
/// cells: every backticked <c>Type.Member</c> the existing extractor reads,
/// plus every bare backticked type name that a type under <c>src/</c>
/// declares. Sorted ordinally, without duplicates.
/// </param>
/// <param name="Detectors">
/// The <c>Fixture.Method</c> references in the row's <c>Detector</c> cell.
/// Sorted ordinally, without duplicates; empty when the table has no such column.
/// </param>
internal sealed record RefinementIndexRow(
    string Section,
    string Label,
    IReadOnlyList<string> Production,
    IReadOnlyList<string> Detectors);
