namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How much of each bundled file the <c>repocontext_context</c> tool packs into a
/// budgeted context bundle. Richer levels cost more tokens per file, so the level
/// trades bundle breadth against per-file depth under a fixed ceiling.
/// <para>
/// <see cref="Auto"/> is a request, not an emitted level: the bundle service tries
/// the richest level that yields a non-empty bundle (slices, then outline, then
/// paths) and reports the concrete level it settled on, so the caller always learns
/// which of <see cref="Paths"/>, <see cref="Outline"/>, or <see cref="Slices"/>
/// actually ran.
/// </para>
/// </summary>
public enum RepoContextContextDetail
{
    /// <summary>
    /// Let the service pick the richest level that fits the budget with a non-empty
    /// bundle, degrading slices to outline to paths as the budget tightens. The
    /// result reports the concrete level chosen; <see cref="Auto"/> is never itself
    /// the reported level.
    /// </summary>
    Auto = 0,

    /// <summary>Pack only each file's repository-relative path - the cheapest level, a pure ranked file list.</summary>
    Paths = 1,

    /// <summary>Pack each file's structural skeleton (its declared symbols with signatures and spans) rather than its body.</summary>
    Outline = 2,

    /// <summary>Pack each file's bounded body text - the richest and most expensive level.</summary>
    Slices = 3,
}
