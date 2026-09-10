namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One environment-derived setting, paired with the value this package resolves when
/// nothing is supplied, so a host can state what it actually ran without restating what
/// the defaults are.
/// </summary>
/// <remarks>
/// Both values are already rendered for display. The pair is the unit rather than the
/// resolved value alone because "is this overridden?" is the question the report exists to
/// answer, and it is not answerable from a resolved value on its own.
/// </remarks>
/// <param name="Name">The environment-variable name.</param>
/// <param name="Resolved">The value this package resolved, rendered for display.</param>
/// <param name="Default">The value this package resolves when nothing is supplied.</param>
public readonly record struct RepoContextSettingSnapshot(string Name, string Resolved, string Default);
