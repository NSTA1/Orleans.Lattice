namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Whether the host can serve <b>semantic</b> repository-context retrieval right now.
/// This is deliberately separate from the container's lifecycle phase: a silo can have
/// joined, proven its durable stores writable, and be serving MCP traffic while its
/// vector plane is still replaying and cannot answer a single semantic query.
/// </summary>
public enum RepoContextRetrievalReadinessPhase
{
    /// <summary>
    /// An embedding provider is bound, so the host is configured for semantic
    /// retrieval, but the vector plane has not been proven able to serve it - it is
    /// still building (cold start, WAL replay, re-derivation back-fill) or it is
    /// unavailable. <b>Not ready.</b>
    /// </summary>
    Building = 0,

    /// <summary>
    /// The vector plane has been proven able to serve semantic retrieval and no
    /// outstanding fault has outlived its hold-down window. <b>Ready.</b>
    /// </summary>
    Serving = 1,

    /// <summary>
    /// No embedding provider is bound at all, so keyword recall is the intended steady
    /// state rather than a degradation. There is no vector plane to wait for, so the
    /// host is legitimately <b>ready</b> - this is the arm that stops a keyword-only
    /// deployment from deadlocking on a readiness signal it can never satisfy.
    /// </summary>
    KeywordOnly = 2,

    /// <summary>
    /// No repository is indexed at all, so the vector plane holds nothing it could fail
    /// to serve. The host is legitimately <b>ready</b> - blocking here would wedge a
    /// fresh box before its first repository could ever be onboarded - but nothing has
    /// been <b>demonstrated</b>, which is exactly why this is a phase of its own rather
    /// than <see cref="Serving"/>.
    /// <para>
    /// It is a startup premise, not evidence, so it is <b>not sticky</b>: it does not
    /// earn the fault hold-down grace <see cref="Serving"/> earns, it does not block a
    /// later <see cref="KeywordOnly"/> observation, and the first real query reporting
    /// the plane unavailable falsifies the premise and returns the host to
    /// <see cref="Building"/>.
    /// </para>
    /// </summary>
    NothingRegistered = 3,
}
