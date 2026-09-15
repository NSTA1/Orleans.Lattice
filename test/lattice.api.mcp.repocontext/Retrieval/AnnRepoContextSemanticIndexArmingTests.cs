using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests that <see cref="AnnRepoContextSemanticIndex"/> tells the shared readiness
/// state which path inside the plane actually answered, so
/// <see cref="RepoContextRetrievalReadinessState.Arming"/> reports a demonstrated
/// fact.
/// <para>
/// This fixture exists because of the shape of issue #2441, which is worth stating
/// so a later reader does not "simplify" it away. Nothing on this path was ever
/// wrong: the caller-facing declaration is a deliberate per-index under-promise,
/// the normalisation that collapses onto it is a deliberate fail-closed policy, and
/// every line of both is true. The defect was that readiness had exactly one input
/// and that input was a constant, so an armed plane and an unarmed one produced
/// byte-identical readiness output. A signal whose inputs cannot vary carries no
/// information however truthful it is, which is why these tests assert on the
/// <b>variation</b> between two runs rather than on any single value being correct.
/// </para>
/// </summary>
[TestFixture]
public sealed class AnnRepoContextSemanticIndexArmingTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 3, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task A_plane_answering_from_its_trained_partitioning_reports_armed()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var index = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
            readiness);

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(readiness.Arming, Is.EqualTo(RepoContextRetrievalArming.Armed));
    }

    [Test]
    public async Task A_plane_answering_by_exhaustive_scan_reports_unarmed()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var index = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/A.cs")),
            readiness);

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(readiness.Arming, Is.EqualTo(RepoContextRetrievalArming.Unarmed));
            Assert.That(readiness.IsReady, Is.False,
                "Arming must not latch readiness on its own. Readiness is still driven by the search "
                + "service observing a retrieval path, and this test drove the index directly.");
        });
    }

    [Test]
    public async Task The_two_serving_states_produce_different_arming_from_the_same_query()
    {
        // The whole point of the issue: before this signal existed, these two runs
        // were indistinguishable on every readiness and retrieval-path surface. If
        // this assertion ever passes with the two equal, the signal has gone back to
        // being a constant and the defect has returned.
        using var armedReadiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        using var unarmedReadiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());

        await Create(
                PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
                armedReadiness)
            .SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await Create(
                PlaneReturning(Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/A.cs")),
                unarmedReadiness)
            .SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(
            armedReadiness.Arming,
            Is.Not.EqualTo(unarmedReadiness.Arming),
            "An armed plane and an unarmed one must not report the same arming. This is the exact "
            + "indistinguishability issue #2441 was filed over.");
    }

    [Test]
    public async Task A_query_the_plane_could_not_answer_does_not_erase_an_arming_observation()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var armed = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
            readiness);
        var bootstrapping = Create(BootstrappingPlaneWithCorpus(0), readiness);

        await armed.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await bootstrapping.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(
            readiness.Arming,
            Is.EqualTo(RepoContextRetrievalArming.Armed),
            "Bootstrapping means the plane did not answer at all, which is evidence about the fallback "
            + "ladder and none whatever about whether a partitioning exists. Letting it overwrite a real "
            + "observation would report 'unknown' for a plane already demonstrated armed.");
    }

    [Test]
    public async Task A_plane_that_loses_its_partitioning_stops_reporting_armed()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());

        await Create(
                PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
                readiness)
            .SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await Create(
                PlaneReturning(Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/A.cs")),
                readiness)
            .SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(
            readiness.Arming,
            Is.EqualTo(RepoContextRetrievalArming.Unarmed),
            "Arming is a statement about the plane's current partitioning, not a latch. A rebuild can "
            + "legitimately return an armed plane to unarmed, and latching would assert a partitioning "
            + "that no longer exists.");
    }

    [Test]
    public async Task An_index_with_no_readiness_bound_still_serves()
    {
        var index = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
            readiness: null);

        var hits = await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(hits, Has.Count.EqualTo(1),
            "Readiness is optional on this seam, so a host that binds none loses the arming report and "
            + "nothing else.");
    }

    [Test]
    public void Only_a_state_the_plane_answered_carries_an_arming_observation()
        => Assert.Multiple(() =>
        {
            Assert.That(
                AnnRepoContextSemanticIndex.ArmingOf(RepoContextAnnServingState.Approximate),
                Is.EqualTo(RepoContextRetrievalArming.Armed));
            Assert.That(
                AnnRepoContextSemanticIndex.ArmingOf(RepoContextAnnServingState.Exhaustive),
                Is.EqualTo(RepoContextRetrievalArming.Unarmed));
            Assert.That(
                AnnRepoContextSemanticIndex.ArmingOf(RepoContextAnnServingState.Bootstrapping),
                Is.EqualTo(RepoContextRetrievalArming.Unknown),
                "Bootstrapping is the plane declining to answer, not evidence that it holds no "
                + "partitioning.");
        });

    private static AnnRepoContextSemanticIndex Create(
        IRepoContextAnnIndex plane,
        RepoContextRetrievalReadinessState? readiness)
        => new(
            plane,
            ExactReturning("repo/acme/file/src/A.cs"),
            RepoContextExactScanBudgets.Default(),
            new RepoContextExactScanBreaker(),
            new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero),
            new LoggerFactory().CreateLogger<AnnRepoContextSemanticIndex>(),
            readiness);

    private static IRepoContextAnnIndex PlaneReturning(RepoContextAnnSearchOutcome outcome)
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RepoContextAnnSearchOutcome>(outcome));
        return plane;
    }

    private static IRepoContextAnnIndex BootstrappingPlaneWithCorpus(int corpus)
    {
        var plane = PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping);
        plane.KnownVectorCount(Arg.Any<string>()).Returns(corpus);
        return plane;
    }

    private static IRepoContextSemanticIndex ExactReturning(params string[] keys)
    {
        var exact = Substitute.For<IRepoContextSemanticIndex>();
        exact.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);
        exact.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                keys.Select((k, i) => new RepoContextVectorMatch($"ann-{i}", k, 1d)).ToArray()));
        return exact;
    }

    private static RepoContextAnnSearchOutcome Answer(
        RepoContextAnnServingState state, params string[] keys)
        => new(state, keys.Select((k, i) => new RepoContextVectorMatch($"ann-{i}", k, 1d)).ToArray());
}
