namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One module's entry in the refinement index (contract C6 of #5148).
/// </summary>
/// <param name="Module">The TLA+ module name, for example <c>ShardOwnership</c>.</param>
/// <param name="Note">The module's refinement note, repository-relative with forward slashes.</param>
/// <param name="Rows">The note's mapping and exclusion rows, ordered by section then label.</param>
/// <param name="Gaps">The note's deliberate abstraction gaps, ordered by title.</param>
internal sealed record RefinementIndexModule(
    string Module,
    string Note,
    IReadOnlyList<RefinementIndexRow> Rows,
    IReadOnlyList<RefinementIndexGap> Gaps);
