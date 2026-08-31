using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="AnnRepoContextSemanticIndex"/>, the semantic index
/// the search service binds by default. Its whole job is a two-way choice - the
/// approximate plane when it can answer, the exact scan when it cannot - plus one
/// invariant that the epic exists to protect: it declares the weaker recall
/// guarantee unconditionally, so no answer it serves can over-promise.
/// </summary>
[TestFixture]
public sealed class AnnRepoContextSemanticIndexTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 3, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static AnnRepoContextSemanticIndex Create(
        IRepoContextAnnIndex plane, IRepoContextSemanticIndex exact)
        => new(plane, exact, NullLogger<AnnRepoContextSemanticIndex>.Instance);

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

    private static IRepoContextSemanticIndex ExactReturning(params string[] sourceKeys)
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);

        var matches = new List<RepoContextVectorMatch>(sourceKeys.Length);
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            matches.Add(new RepoContextVectorMatch($"exact-{i}", sourceKeys[i], 1d - (i * 0.1)));
        }

        index.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(matches));
        return index;
    }

    private static RepoContextAnnSearchOutcome Answer(
        RepoContextAnnServingState state, params string[] sourceKeys)
    {
        var matches = new List<RepoContextVectorMatch>(sourceKeys.Length);
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            matches.Add(new RepoContextVectorMatch($"ann-{i}", sourceKeys[i], 1d - (i * 0.1)));
        }

        return new RepoContextAnnSearchOutcome(state, matches);
    }

    [Test]
    public void The_declared_retrieval_path_is_always_semantic_approximate()
    {
        var serving = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "k")), ExactReturning("k"));
        var bootstrapping = Create(
            PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping), ExactReturning("k"));
        var exhaustive = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Exhaustive, "k")), ExactReturning("k"));

        Assert.Multiple(() =>
        {
            Assert.That(serving.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate));
            Assert.That(bootstrapping.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "Declaring the exact guarantee while the exact scan happens to be answering would be unsound: "
                + "the declaration is per index, and two repositories can be in different states at once.");
            Assert.That(exhaustive.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
                "An exhaustive answer from the plane is exact, and is still declared as the weaker claim.");
        });
    }

    [Test]
    public void The_declared_retrieval_path_normalizes_to_itself()
    {
        var index = Create(
            PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping), ExactReturning("k"));

        Assert.That(
            RepoContextRetrievalPath.NormalizeSemantic(index.RetrievalPath),
            Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
            "The search service re-validates the declaration, and must arrive at the same value.");
    }

    [Test]
    public async Task A_bootstrapping_plane_is_served_by_the_exact_scan()
    {
        var exact = ExactReturning("repo/acme/file/src/A.cs");
        var index = Create(PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping), exact);

        var matches = await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Has.Count.EqualTo(1),
                "Retrieval keeps working while the index builds, with complete recall.");
            Assert.That(matches[0].SourceKey, Is.EqualTo("repo/acme/file/src/A.cs"));
        });
    }

    [Test]
    public async Task A_serving_plane_answers_without_touching_the_exact_scan()
    {
        var exact = ExactReturning("repo/acme/file/src/Exact.cs");
        var index = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/Ann.cs")), exact);

        var matches = await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches[0].SourceKey, Is.EqualTo("repo/acme/file/src/Ann.cs"));
            Assert.That(
                exact.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(exact.SearchAsync)),
                Is.False,
                "Falling through to the full prefix scan when the index can answer would defeat the change.");
        });
    }

    [Test]
    public async Task A_plane_answering_exhaustively_still_answers_for_itself()
    {
        var exact = ExactReturning("repo/acme/file/src/Exact.cs");
        var index = Create(
            PlaneReturning(Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/Warm.cs")), exact);

        var matches = await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(matches[0].SourceKey, Is.EqualTo("repo/acme/file/src/Warm.cs"),
            "A warming index that holds the whole corpus answers exactly; it is not a reason to rescan the store.");
    }

    [Test]
    public async Task A_serving_plane_with_no_match_reports_an_empty_answer_rather_than_rescanning()
    {
        var exact = ExactReturning("repo/acme/file/src/Exact.cs");
        var index = Create(PlaneReturning(Answer(RepoContextAnnServingState.Approximate)), exact);

        var matches = await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(matches, Is.Empty,
            "An index that answered and found nothing has answered; the search service turns that into the "
            + "vector-plane-unavailable keyword cause, which is the honest report.");
    }

    [Test]
    public void Progress_is_forwarded_from_the_plane()
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.TryGetProgress(RepoId, Space, out Arg.Any<Vector.Persistence.VectorIndexBuildProgress>())
            .Returns(false);
        var index = Create(plane, ExactReturning("k"));

        Assert.That(index.TryGetProgress(RepoId, Space, out _), Is.False,
            "No index for the pair is itself the honest answer: nothing has been built.");
    }

    [Test]
    public void Constructing_with_a_null_dependency_is_rejected()
    {
        var plane = PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping);
        var exact = ExactReturning("k");

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new AnnRepoContextSemanticIndex(
                    null!, exact, NullLogger<AnnRepoContextSemanticIndex>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new AnnRepoContextSemanticIndex(
                    plane, null!, NullLogger<AnnRepoContextSemanticIndex>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new AnnRepoContextSemanticIndex(plane, exact, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Search_rejects_invalid_arguments()
    {
        var index = Create(PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping), ExactReturning("k"));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await index.SearchAsync(null!, new float[] { 1f, 0f, 0f }, Space, 5, Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 0, Ct),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void The_bootstrapping_outcome_is_a_cached_empty_answer()
    {
        var first = RepoContextAnnSearchOutcome.Bootstrapping;
        var second = RepoContextAnnSearchOutcome.Bootstrapping;

        Assert.Multiple(() =>
        {
            Assert.That(first.State, Is.EqualTo(RepoContextAnnServingState.Bootstrapping));
            Assert.That(first.Matches, Is.Empty);
            Assert.That(ReferenceEquals(first.Matches, second.Matches), Is.True,
                "Reporting the common no-index case must not allocate on the per-query path.");
        });
    }
}
