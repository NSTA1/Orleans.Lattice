namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Answers the one question a build coordinator cannot answer for itself when its
/// corpus read came back empty: was the range actually admitted?
/// <para>
/// The seam exists because the answer has to come from the access gate on the
/// vector-metadata tree, and the coordinator reaches its corpus through
/// <see cref="IRepoContextVectorSource"/>, which is deliberately a store-of-record
/// <i>view</i> and knows nothing about authorization. Widening that interface
/// would put a gate concern on the streaming hot path; a separate cold-path probe
/// keeps it exactly where it is needed and nowhere else.
/// </para>
/// </summary>
internal interface IRepoContextCorpusGateProbe
{
    /// <summary>
    /// Classifies how much of one repository's vector prefix the read-path access
    /// gate admits.
    /// <para>
    /// Called <b>only</b> when a build reached <c>Ready</c> holding nothing and is
    /// about to act on that emptiness, which is the contract
    /// <see cref="ILattice.GetRangeReadGateCoverageAsync"/> states for itself. The
    /// ordinary non-empty path never calls it, so the streaming hot path pays
    /// nothing.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose vector prefix to classify. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>
    /// The coverage class. An implementation that cannot answer must report
    /// <see cref="RepoContextAnnBuildCorpusCoverage.Unknown"/> rather than
    /// <see cref="RepoContextAnnBuildCorpusCoverage.Unrestricted"/>: a probe that
    /// failed is not permission granted.
    /// </returns>
    Task<RepoContextAnnBuildCorpusCoverage> ProbeAsync(string repoId, CancellationToken cancellationToken);
}
