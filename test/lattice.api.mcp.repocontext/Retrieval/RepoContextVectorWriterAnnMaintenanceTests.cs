using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the write-through maintenance
/// <see cref="RepoContextVectorWriter"/> performs against the approximate
/// retrieval plane, at the same seam that invalidates the warm candidate cache.
/// <para>
/// This is what keeps the index coherent without a scan, and the ordering is the
/// load-bearing part: the store of record lands first, the superseded identifiers
/// are known before they are handed over, and the plane is told about them, so the
/// index is never ahead of the source and a replaced vector can never be returned
/// alongside its replacement.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextVectorWriterAnnMaintenanceTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpace Space = new("test-model", 4, normalized: true);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static (RepoContextVectorWriter Writer, RecordingAnnIndex Plane, SubstitutedVectorTrees Trees)
        Create(bool bindPlane = true)
    {
        var trees = new SubstitutedVectorTrees(Serializer);
        var plane = new RecordingAnnIndex();
        var writer = new RepoContextVectorWriter(
            trees.GrainFactory,
            Serializer,
            Substitute.For<ILatticeReplicationContext>(),
            new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
            RepoContextVectorPlaneTestDoubles.ReDeriver(trees.GrainFactory),
            bindPlane ? plane : null);

        return (writer, plane, trees);
    }

    private static string VectorId(string sourceKey, int unit, float[] vector)
        => RepoContextVectorWriter.FormatVectorId(
            VectorCodec.SourceId(sourceKey),
            unit,
            VectorCodec.ContentAddress(VectorCodec.Encode(vector)));

    [Test]
    public async Task Storing_a_source_hands_every_passage_to_the_plane()
    {
        var (writer, plane, _) = Create();
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        float[] first = [1f, 0f, 0f, 0f];
        float[] second = [0f, 1f, 0f, 0f];

        await writer.StoreAsync(
            RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { first, second }, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(plane.Writes, Has.Count.EqualTo(1), "One maintenance call per store, not one per passage.");
            Assert.That(
                plane.Writes[0].Upserts.Select(update => update.VectorId),
                Is.EqualTo(new[] { VectorId(sourceKey, 0, first), VectorId(sourceKey, 1, second) }).AsCollection,
                "A source embeds as several passages, and the index must hold all of them.");
            Assert.That(plane.Writes[0].Upserts[0].SourceKey, Is.EqualTo(sourceKey),
                "Each update carries the canonical key, so the index never has to re-derive it.");
            Assert.That(plane.Writes[0].Space, Is.EqualTo(EmbeddingSpaceTag.FromSpace(Space)),
                "The write is routed to the index for the space it was written under, never to another.");
            Assert.That(plane.Writes[0].Retired, Is.Empty, "A first store supersedes nothing.");
        });
    }

    [Test]
    public async Task Re_embedding_a_source_retires_the_superseded_passage()
    {
        var (writer, plane, _) = Create();
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        float[] original = [1f, 0f, 0f, 0f];
        float[] replacement = [0f, 0f, 1f, 0f];

        await writer.StoreAsync(RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { original }, Ct);
        await writer.StoreAsync(RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { replacement }, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(plane.Writes, Has.Count.EqualTo(2));
            Assert.That(
                plane.Writes[1].Retired,
                Is.EqualTo(new[] { VectorId(sourceKey, 0, original) }).AsCollection,
                "The superseded content address is retired, or the index would return both versions.");
            Assert.That(
                plane.Writes[1].Upserts.Select(update => update.VectorId),
                Is.EqualTo(new[] { VectorId(sourceKey, 0, replacement) }).AsCollection);
        });
    }

    [Test]
    public async Task Shrinking_a_source_retires_the_passages_it_no_longer_has()
    {
        var (writer, plane, _) = Create();
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        float[] first = [1f, 0f, 0f, 0f];
        float[] second = [0f, 1f, 0f, 0f];

        await writer.StoreAsync(
            RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { first, second }, Ct);
        await writer.StoreAsync(RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { first }, Ct);

        Assert.That(
            plane.Writes[1].Retired,
            Is.EqualTo(new[] { VectorId(sourceKey, 1, second) }).AsCollection,
            "A file that lost a passage must lose its vector too, not keep an orphan in the index.");
    }

    [Test]
    public async Task Retiring_a_source_hands_every_one_of_its_identifiers_to_the_plane()
    {
        var (writer, plane, _) = Create();
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        float[] first = [1f, 0f, 0f, 0f];
        float[] second = [0f, 1f, 0f, 0f];
        await writer.StoreAsync(
            RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { first, second }, Ct);

        await writer.RetireAsync(RepoId, sourceKey, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(plane.Retirements, Has.Count.EqualTo(1));
            Assert.That(
                plane.Retirements[0].Retired,
                Is.EquivalentTo(new[] { VectorId(sourceKey, 0, first), VectorId(sourceKey, 1, second) }),
                "A pruned file drops every passage it had, or the index keeps a ghost of a deleted file.");
        });
    }

    [Test]
    public async Task Retiring_a_source_with_no_vectors_tells_the_plane_nothing()
    {
        var (writer, plane, _) = Create();

        await writer.RetireAsync(RepoId, RepoContextKeys.File(RepoId, "src/Never.cs"), Ct);

        Assert.That(plane.Retirements, Is.Empty,
            "Retiring a source that was never embedded is a no-op, not an empty maintenance round trip.");
    }

    [Test]
    public async Task With_no_plane_bound_the_write_path_is_unchanged()
    {
        var (writer, _, trees) = Create(bindPlane: false);
        var sourceKey = RepoContextKeys.File(RepoId, "src/A.cs");
        float[] vector = [1f, 0f, 0f, 0f];

        await writer.StoreAsync(RepoId, sourceKey, Space, new ReadOnlyMemory<float>[] { vector }, Ct);

        Assert.That(
            trees.MetadataKeys,
            Is.EqualTo(new[] { RepoContextKeys.Vector(RepoId, VectorId(sourceKey, 0, vector)) }).AsCollection,
            "A host configured for the exact scan maintains no index, and its store of record is written "
            + "exactly as it was before.");
    }

    /// <summary>
    /// Records what the write seam handed the approximate plane, so the maintenance
    /// contract can be asserted without an index or a silo.
    /// </summary>
    private sealed class RecordingAnnIndex : IRepoContextAnnIndex
    {
        public List<(EmbeddingSpaceTag Space, IReadOnlyList<RepoContextAnnVectorUpdate> Upserts,
            IReadOnlyList<string> Retired)> Writes { get; } = [];

        public List<(string RepoId, IReadOnlyList<string> Retired)> Retirements { get; } = [];

        public ValueTask<RepoContextAnnSearchOutcome> SearchAsync(
            string repoId,
            ReadOnlyMemory<float> query,
            EmbeddingSpaceTag space,
            int k,
            CancellationToken cancellationToken)
            => new(RepoContextAnnSearchOutcome.Bootstrapping);

        public bool TryGetProgress(
            string repoId, EmbeddingSpaceTag space, out Vector.Persistence.VectorIndexBuildProgress progress)
        {
            progress = default;
            return false;
        }

        public Task ApplyWriteAsync(
            string repoId,
            EmbeddingSpaceTag space,
            IReadOnlyList<RepoContextAnnVectorUpdate> upserts,
            IReadOnlyList<string> retired,
            CancellationToken cancellationToken)
        {
            Writes.Add((space, [.. upserts], [.. retired]));
            return Task.CompletedTask;
        }

        public Task ApplyRetirementAsync(
            string repoId, IReadOnlyList<string> retired, CancellationToken cancellationToken)
        {
            Retirements.Add((repoId, [.. retired]));
            return Task.CompletedTask;
        }
    }
}
