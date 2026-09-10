using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The production corpus gate probe: asks the reserved vector-metadata tree how
/// much of one repository's vector prefix the read-path access gate admits.
/// <para>
/// The bounds are the same prefix and upper bound
/// <see cref="RepoContextVectorSource"/> streams and counts over, which is what
/// makes the answer about <i>this</i> build's corpus rather than about the tree in
/// general. <see cref="ILattice.GetRangeReadGateCoverageAsync"/> requires the
/// bounds to match the read whose emptiness is being interpreted, and a probe over
/// a wider or narrower range would classify something the build did not do.
/// </para>
/// <para>
/// <b>Runs inside the caller's credential scope, and must.</b> The coordinator
/// opens its run authority's scope for the whole tick before it takes any step, so
/// this probe resolves under the same subject the corpus read used. Probing under
/// a different identity would classify a range nobody read.
/// </para>
/// </summary>
internal sealed class LatticeRepoContextCorpusGateProbe(
    IGrainFactory grainFactory,
    ILogger<LatticeRepoContextCorpusGateProbe> logger)
    : IRepoContextCorpusGateProbe
{
    /// <inheritdoc />
    public async Task<RepoContextAnnBuildCorpusCoverage> ProbeAsync(
        string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        try
        {
            var tree = grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
            var prefix = RepoContextKeys.VectorsPrefix(repoId);
            var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);

            var coverage = await tree
                .GetRangeReadGateCoverageAsync(prefix, endExclusive, cancellationToken)
                .ConfigureAwait(false);

            return coverage switch
            {
                LatticeRangeReadGateCoverage.Unrestricted => RepoContextAnnBuildCorpusCoverage.Unrestricted,
                LatticeRangeReadGateCoverage.Filtered => RepoContextAnnBuildCorpusCoverage.Filtered,
                LatticeRangeReadGateCoverage.Denied => RepoContextAnnBuildCorpusCoverage.Denied,

                // A member this build does not recognise is not evidence of
                // permission. Fail closed onto Unknown, which the coordinator
                // treats as a reason to retry rather than as a reason to converge.
                _ => RepoContextAnnBuildCorpusCoverage.Unknown,
            };
        }
        catch (Exception ex)
        {
            // Deliberately swallowed and classified rather than propagated. The
            // probe is a diagnostic on a path that has already finished its work,
            // and letting it throw would turn an unanswered question into a failed
            // build - which inverts the blast radius the probe exists to reduce.
            // Unknown is still counted and still withholds convergence up to the
            // terminal threshold, so a probe that never answers is loud, not
            // ignored.
            logger.LogWarning(
                ex,
                "Repository-context approximate index for {RepoId} could not classify the access-gate coverage of "
                + "its vector prefix after an empty corpus read, so the emptiness cannot be attributed. It is "
                + "counted on {Instrument} as coverage={Coverage}.",
                repoId,
                RepoContextAnnBuildCorpusReporter.CorpusInstrumentName,
                RepoContextAnnBuildCorpusReporter.CoverageUnknownTag);
            return RepoContextAnnBuildCorpusCoverage.Unknown;
        }
    }
}
