namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// A corpus coverage probe that always answers <c>Unrestricted</c>, for fixtures
/// whose subject is something other than the access gate.
/// <para>
/// This is the correct answer for an in-process host with no access gate
/// configured at all, which is what every one of those fixtures runs: the whole
/// vector prefix is readable, so an empty build means an empty repository. It is
/// deliberately <b>not</b> the grain's default - the coordinator takes the probe as
/// a required dependency precisely so that a host cannot silently acquire the
/// permissive answer by omitting the registration, which is the shape of failure
/// issue #2426 was about in the first place.
/// </para>
/// </summary>
internal sealed class UnrestrictedCorpusGateProbe : IRepoContextCorpusGateProbe
{
    /// <summary>The shared instance. The probe holds no state.</summary>
    public static readonly UnrestrictedCorpusGateProbe Instance = new();

    /// <inheritdoc />
    public Task<RepoContextAnnBuildCorpusCoverage> ProbeAsync(
        string repoId, CancellationToken cancellationToken)
        => Task.FromResult(RepoContextAnnBuildCorpusCoverage.Unrestricted);
}
