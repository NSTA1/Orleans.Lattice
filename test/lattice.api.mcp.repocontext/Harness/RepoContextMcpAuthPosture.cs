namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// The three canonical authorization postures the
/// <see cref="RepoContextMcpHarness"/> can serve a session under, so every
/// repository-context tool sub-issue asserts the fail-closed discovery seam
/// (issue #1428) uniformly rather than reinventing credential / grant wiring.
/// The posture is driven deterministically through stub collaborators (a stub
/// credential bridge and a stub permission resolver), so it never depends on a
/// real Auth policy tree or its eventual-consistency compile step.
/// </summary>
public enum RepoContextMcpAuthPosture
{
    /// <summary>
    /// No recognisable credential: the credential bridge resolves the session to
    /// anonymous, so the fail-closed discovery core offers the caller nothing -
    /// not even the <c>lattice_capabilities</c> meta-tool. Use this posture to
    /// assert that an unauthenticated caller can neither see nor invoke any
    /// repository-context tool (default-denied).
    /// </summary>
    Unauthenticated,

    /// <summary>
    /// An authenticated caller granted the repository-context group but <b>not</b>
    /// the host-side write opt-in. The read-only tools (including
    /// <c>repocontext_health</c>) are advertised; no mutating repository-context
    /// tool is offered. Use this posture to assert that a reader is never shown a
    /// write tool.
    /// </summary>
    Reader,

    /// <summary>
    /// An authenticated caller granted the repository-context group <b>with</b>
    /// the host-side write opt-in. The read-only tools are advertised and, once
    /// the mutating repository-context tools land in later work, they are offered
    /// too - each annotated with the correct destructive / read-only hints. Use
    /// this posture to assert a writer sees the mutating surface.
    /// </summary>
    Writer,
}
