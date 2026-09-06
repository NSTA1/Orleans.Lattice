using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextVectorSource"/>, the store-of-record view
/// the approximate index derives itself from. Its three obligations are all
/// correctness-critical: it must yield only the query's embedding space, it must
/// stream in ascending identifier order and resume strictly after a supplied
/// identifier, and it must resolve identifiers back to canonical source keys from
/// the store of record rather than from anything the index holds.
/// </summary>
[TestFixture]
public sealed class RepoContextVectorSourceTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 4, VectorNormalization.UnitL2);

    private static readonly EmbeddingSpaceTag OtherSpace =
        new("other-model", 4, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static (SubstitutedVectorTrees Trees, RepoContextVectorSource Source) Create()
    {
        var trees = new SubstitutedVectorTrees(Serializer);
        return (trees, new RepoContextVectorSource(trees.GrainFactory, Serializer, RepoId, Space));
    }

    private static float[] Vector(int ordinal) => [ordinal, 1f, 0f, 0f];

    private static async Task<List<string>> IdsAsync(
        RepoContextVectorSource source, string? after, CancellationToken cancellationToken)
    {
        var ids = new List<string>();
        await foreach (var entry in source.EnumerateAsync(after, cancellationToken))
        {
            ids.Add(entry.Id);
        }

        return ids;
    }

    [Test]
    public void Dimensions_come_from_the_embedding_space()
    {
        var (_, source) = Create();

        Assert.That(source.Dimensions, Is.EqualTo(Space.Dimension));
    }

    [Test]
    public async Task Enumeration_yields_every_vector_in_ascending_identifier_order()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-c", "repo/acme/file/src/C.cs", Space, Vector(3));
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", Space, Vector(2));

        var ids = await IdsAsync(source, null, Ct);

        Assert.That(ids, Is.EqualTo(new[] { "vec-a", "vec-b", "vec-c" }).AsCollection,
            "Ascending key order is ascending identifier order, which is what makes a build resumable.");
    }

    [Test]
    public async Task Enumeration_resumes_strictly_after_the_supplied_identifier()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", Space, Vector(2));
        trees.Write(RepoId, "vec-c", "repo/acme/file/src/C.cs", Space, Vector(3));

        var ids = await IdsAsync(source, "vec-a", Ct);

        Assert.That(ids, Is.EqualTo(new[] { "vec-b", "vec-c" }).AsCollection,
            "A resumed build must neither repeat the identifier it checkpointed nor skip the next one.");
    }

    [Test]
    public async Task A_vector_from_another_embedding_space_is_never_yielded()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", OtherSpace, Vector(2));

        var ids = await IdsAsync(source, null, Ct);

        Assert.That(ids, Is.EqualTo(new[] { "vec-a" }).AsCollection,
            "The space guard lives at this seam, so an index built from the view can never mix two spaces.");
    }

    [Test]
    public async Task A_vector_whose_payload_is_missing_is_dropped_rather_than_yielded_empty()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", Space, Vector(2));
        trees.DropPayload(RepoId, Vector(2));

        var ids = await IdsAsync(source, null, Ct);

        Assert.That(ids, Is.EqualTo(new[] { "vec-a" }).AsCollection,
            "The index may lag in the missing direction; it may never hold a vector it could not read.");
    }

    [Test]
    public async Task Counting_reports_the_stored_vectors()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", Space, Vector(2));

        Assert.That(await source.CountAsync(Ct), Is.EqualTo(2));
    }

    [Test]
    public async Task Containment_follows_the_store_of_record()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await source.ContainsAsync("vec-a", Ct), Is.True);
            Assert.That(await source.ContainsAsync("vec-missing", Ct), Is.False);
        });
    }

    [Test]
    public async Task Source_keys_resolve_from_the_store_of_record()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", Space, Vector(2));

        var resolved = await source.ResolveSourceKeysAsync(["vec-a", "vec-b"], Ct);

        Assert.Multiple(() =>
        {
            Assert.That(resolved["vec-a"], Is.EqualTo("repo/acme/file/src/A.cs"));
            Assert.That(resolved["vec-b"], Is.EqualTo("repo/acme/file/src/B.cs"));
        });
    }

    [Test]
    public async Task A_retired_identifier_does_not_resolve()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-a", "repo/acme/file/src/A.cs", Space, Vector(1));
        trees.RetireMetadata(RepoId, "vec-a");

        var resolved = await source.ResolveSourceKeysAsync(["vec-a"], Ct);

        Assert.That(resolved, Is.Empty,
            "The store of record settles every disagreement, so a hit it will not stand behind is never hydrated.");
    }

    [Test]
    public async Task An_identifier_written_under_another_space_does_not_resolve()
    {
        var (trees, source) = Create();
        trees.Write(RepoId, "vec-b", "repo/acme/file/src/B.cs", OtherSpace, Vector(2));

        var resolved = await source.ResolveSourceKeysAsync(["vec-b"], Ct);

        Assert.That(resolved, Is.Empty,
            "Resolution is fail-closed on embedding space too, not only enumeration.");
    }

    [Test]
    public async Task Resolving_an_empty_batch_touches_nothing()
    {
        var (_, source) = Create();

        Assert.That(await source.ResolveSourceKeysAsync([], Ct), Is.Empty);
    }

    [Test]
    public void Constructing_and_calling_with_a_null_argument_is_rejected()
    {
        var (trees, source) = Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextVectorSource(null!, Serializer, RepoId, Space),
                Throws.ArgumentNullException);
            Assert.That(() => new RepoContextVectorSource(trees.GrainFactory, null!, RepoId, Space),
                Throws.ArgumentNullException);
            Assert.That(() => new RepoContextVectorSource(trees.GrainFactory, Serializer, null!, Space),
                Throws.ArgumentNullException);
            Assert.That(async () => await source.ContainsAsync(null!, Ct), Throws.ArgumentNullException);
            Assert.That(async () => await source.ResolveSourceKeysAsync(null!, Ct), Throws.ArgumentNullException);
        });
    }
}
