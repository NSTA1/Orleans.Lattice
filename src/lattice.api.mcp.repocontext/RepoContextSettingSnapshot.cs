namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One environment-derived setting, paired with the value this package resolves when
/// nothing is supplied, so a host can state what it actually ran without restating what
/// the defaults are.
/// </summary>
/// <remarks>
/// <para>
/// Both values are already rendered for display. The pair is the unit rather than the
/// resolved value alone because "is this overridden?" is the question the report exists to
/// answer, and it is not answerable from a resolved value on its own.
/// </para>
/// <para>
/// <b><see cref="WasDeclared"/> is a third, independent fact (issue #2586).</b> "Is this
/// overridden?" and "did anybody set this?" are different questions and their answers come
/// apart in both directions: a value can equal the default because an operator declared
/// exactly the default, and a value can differ from the default because a neighbouring
/// setting moved it while this one was never declared. A report that carried only the
/// value pair had to answer the second question by inference from the first, and got it
/// wrong in the case that mattered - a defaulted grace period printed in the same shape as
/// a declared one, seconds after a warning saying it was unset.
/// </para>
/// <para>
/// It is captured here, beside the resolved value, rather than probed later by the host,
/// so that the declaration claim and the value it qualifies are read from the same source.
/// A host probing its own <c>IConfiguration</c> to describe a value this package read from
/// the process environment would be asserting a provenance it did not observe.
/// </para>
/// </remarks>
/// <param name="Name">The environment-variable name.</param>
/// <param name="Resolved">The value this package resolved, rendered for display.</param>
/// <param name="Default">The value this package resolves when nothing is supplied.</param>
/// <param name="WasDeclared">
/// Whether anything actually supplied this setting, as read from the same environment the
/// resolved value came from.
/// </param>
public readonly record struct RepoContextSettingSnapshot(
    string Name,
    string Resolved,
    string Default,
    bool WasDeclared);
