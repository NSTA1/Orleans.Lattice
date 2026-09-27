namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The app-scoped backup plan for a repository-context install, computed from the
/// manifest's <c>rebuildable</c> tree classification: which trees a recovery must restore
/// from backup bytes, and which it may instead rederive once the restored trees are back.
/// Trees are named by their effective physical ids.
/// </summary>
/// <param name="TreesToRestore">The store-of-record trees whose bytes a recovery restores, in manifest order.</param>
/// <param name="TreesToRederive">The derived trees a recovery rederives rather than restores, in manifest order.</param>
internal sealed record RepoContextAppBackupPlan(
    IReadOnlyList<string> TreesToRestore,
    IReadOnlyList<string> TreesToRederive);
