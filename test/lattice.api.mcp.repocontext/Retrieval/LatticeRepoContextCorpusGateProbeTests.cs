using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="LatticeRepoContextCorpusGateProbe"/>, the production
/// probe that attributes an empty approximate-index corpus read to either a
/// genuinely empty repository or an access gate that withheld it.
/// <para>
/// <b>Why every arm here matters.</b> The probe exists to stop an empty corpus
/// read being read as a converged empty repository when it was really a
/// default-deny gate returning nothing. Each mapped arm below is the difference
/// between the coordinator converging and the coordinator retrying, so an arm
/// that silently mapped to the wrong member would reintroduce exactly the
/// silent-empty failure of issues #2277, #2406, #2426 and #2480.
/// </para>
/// <para>
/// The bounds assertions are equally load-bearing.
/// <see cref="ILattice.GetRangeReadGateCoverageAsync"/> only answers about the
/// range it is handed, so a probe over a wider or narrower range would classify
/// something the build never read - a wrong answer that no arm mapping could
/// detect.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeRepoContextCorpusGateProbeTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// Builds the probe over a substituted vector-metadata tree, returning both so a
    /// test can assert on the call the probe made as well as on what it returned.
    /// </summary>
    private static (LatticeRepoContextCorpusGateProbe Probe, ILattice Tree) CreateProbe()
    {
        var tree = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata, null).Returns(tree);

        var probe = new LatticeRepoContextCorpusGateProbe(
            grainFactory, NullLogger<LatticeRepoContextCorpusGateProbe>.Instance);

        return (probe, tree);
    }

    [Test]
    public void ProbeAsync_throws_for_a_null_repository_id()
    {
        var (probe, _) = CreateProbe();

        Assert.That(
            async () => await probe.ProbeAsync(null!, TestContext.CurrentContext.CancellationToken),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ProbeAsync_maps_an_unrestricted_gate_to_unrestricted_coverage()
    {
        var (probe, tree) = CreateProbe();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(LatticeRangeReadGateCoverage.Unrestricted);

        var coverage = await probe.ProbeAsync(RepoId, TestContext.CurrentContext.CancellationToken);

        Assert.That(coverage, Is.EqualTo(RepoContextAnnBuildCorpusCoverage.Unrestricted));
    }

    [Test]
    public async Task ProbeAsync_maps_a_filtered_gate_to_filtered_coverage()
    {
        var (probe, tree) = CreateProbe();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(LatticeRangeReadGateCoverage.Filtered);

        var coverage = await probe.ProbeAsync(RepoId, TestContext.CurrentContext.CancellationToken);

        Assert.That(coverage, Is.EqualTo(RepoContextAnnBuildCorpusCoverage.Filtered));
    }

    [Test]
    public async Task ProbeAsync_maps_a_denied_gate_to_denied_coverage()
    {
        var (probe, tree) = CreateProbe();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(LatticeRangeReadGateCoverage.Denied);

        var coverage = await probe.ProbeAsync(RepoId, TestContext.CurrentContext.CancellationToken);

        Assert.That(coverage, Is.EqualTo(RepoContextAnnBuildCorpusCoverage.Denied));
    }

    /// <summary>
    /// A coverage member this build does not recognise must fail closed onto
    /// <c>Unknown</c> rather than onto a permissive answer. The cast is the point:
    /// it stands in for a newer core library reporting a member this assembly was
    /// not compiled against, which is the only way the discard arm is ever reached.
    /// </summary>
    [Test]
    public async Task ProbeAsync_fails_closed_onto_unknown_for_an_unrecognised_gate_member()
    {
        var (probe, tree) = CreateProbe();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((LatticeRangeReadGateCoverage)int.MaxValue);

        var coverage = await probe.ProbeAsync(RepoId, TestContext.CurrentContext.CancellationToken);

        Assert.That(coverage, Is.EqualTo(RepoContextAnnBuildCorpusCoverage.Unknown));
    }

    /// <summary>
    /// A throwing tree is classified, not propagated: the probe is a diagnostic on a
    /// path that has already finished its work, and letting it throw would turn an
    /// unanswered question into a failed build.
    /// </summary>
    [Test]
    public async Task ProbeAsync_classifies_a_throwing_tree_as_unknown_rather_than_propagating()
    {
        var (probe, tree) = CreateProbe();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("the gate is unreachable"));

        var coverage = await probe.ProbeAsync(RepoId, TestContext.CurrentContext.CancellationToken);

        Assert.That(coverage, Is.EqualTo(RepoContextAnnBuildCorpusCoverage.Unknown));
    }

    /// <summary>
    /// The probe must ask about exactly the prefix and upper bound
    /// <see cref="RepoContextVectorSource"/> streams over. Asserting the captured
    /// bounds - and asserting that a capture happened at all - is what stops this
    /// degrading into a test that passes because the call was never matched.
    /// </summary>
    [Test]
    public async Task ProbeAsync_asks_about_exactly_this_repositorys_vector_prefix()
    {
        var (probe, tree) = CreateProbe();

        var captured = new List<(string Start, string End)>();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured.Add((call.ArgAt<string>(0), call.ArgAt<string>(1)));
                return LatticeRangeReadGateCoverage.Unrestricted;
            });

        await probe.ProbeAsync(RepoId, TestContext.CurrentContext.CancellationToken);

        var expectedPrefix = RepoContextKeys.VectorsPrefix(RepoId);
        Assert.Multiple(() =>
        {
            Assert.That(captured, Has.Count.EqualTo(1), "the probe must make exactly one gate call");
            Assert.That(captured[0].Start, Is.EqualTo(expectedPrefix));
            Assert.That(
                captured[0].End,
                Is.EqualTo(RepoContextPortability.PrefixUpperBound(expectedPrefix)));
        });
    }

    /// <summary>
    /// Two repositories must be classified over disjoint ranges. A probe that reused
    /// one repository's bounds for another would answer confidently about a range the
    /// build never read, which no arm-mapping assertion above could catch.
    /// </summary>
    [Test]
    public async Task ProbeAsync_scopes_the_gate_question_to_the_requested_repository()
    {
        var (probe, tree) = CreateProbe();

        var starts = new List<string>();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                starts.Add(call.ArgAt<string>(0));
                return LatticeRangeReadGateCoverage.Unrestricted;
            });

        await probe.ProbeAsync("alpha", TestContext.CurrentContext.CancellationToken);
        await probe.ProbeAsync("beta", TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(starts, Has.Count.EqualTo(2));
            Assert.That(starts[0], Is.Not.EqualTo(starts[1]));
            Assert.That(starts[0], Is.EqualTo(RepoContextKeys.VectorsPrefix("alpha")));
            Assert.That(starts[1], Is.EqualTo(RepoContextKeys.VectorsPrefix("beta")));
        });
    }

    /// <summary>
    /// The caller's cancellation token must reach the tree. A probe that dropped it
    /// would keep a shutting-down host waiting on a diagnostic it no longer needs.
    /// </summary>
    [Test]
    public async Task ProbeAsync_passes_the_callers_cancellation_token_through_to_the_tree()
    {
        var (probe, tree) = CreateProbe();
        using var cts = new CancellationTokenSource();

        var captured = new List<CancellationToken>();
        tree.GetRangeReadGateCoverageAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured.Add(call.ArgAt<CancellationToken>(2));
                return LatticeRangeReadGateCoverage.Unrestricted;
            });

        await probe.ProbeAsync(RepoId, cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(captured, Has.Count.EqualTo(1));
            Assert.That(captured[0], Is.EqualTo(cts.Token));
        });
    }
}
