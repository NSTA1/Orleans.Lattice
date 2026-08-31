using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Vector;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A silo-free rig for the approximate retrieval plane: an in-memory store of
/// record, an in-memory durable store, and a registry wired to both, with the
/// background build disabled so every test drives the build itself and no
/// assertion depends on a clock, a delay, or a race with a background task.
/// <para>
/// Rebuilding the registry over the same <see cref="Factory"/> is a process
/// restart: the new index opens onto exactly the records the previous one
/// committed.
/// </para>
/// </summary>
internal sealed class AnnPlaneFixture : IDisposable
{
    /// <summary>The repository every helper on this fixture addresses.</summary>
    internal const string RepoId = "acme";

    /// <summary>The embedding space every helper on this fixture addresses.</summary>
    internal static readonly EmbeddingSpaceTag Space =
        new("test-model", 8, VectorNormalization.UnitL2);

    private RepoContextAnnIndexRegistry _registry;

    /// <summary>
    /// Creates the rig. The defaults train a partitioning over a deliberately tiny
    /// corpus so a test can reach the approximate path without seeding thousands of
    /// vectors; a test that wants the untrained exhaustive path raises
    /// <see cref="RepoContextAnnOptions.MinimumTrainingCount"/> above its corpus.
    /// </summary>
    /// <param name="options">Plane options, or <see langword="null"/> for the rig defaults.</param>
    public AnnPlaneFixture(RepoContextAnnOptions? options = null)
    {
        Options = options ?? DefaultOptions();
        Factory = new InMemoryAnnBackingFactory();
        _registry = Create();
    }

    /// <summary>The backing doubles, addressable so a test can seed and inspect them.</summary>
    public InMemoryAnnBackingFactory Factory { get; }

    /// <summary>The plane options in force.</summary>
    public RepoContextAnnOptions Options { get; }

    /// <summary>The registry under test.</summary>
    public RepoContextAnnIndexRegistry Registry => _registry;

    /// <summary>The store-of-record view for the fixture's repository and space.</summary>
    public InMemoryRepoContextVectorSource Source => Factory.For(RepoId, Space).Source;

    /// <summary>The durable store for the fixture's repository and space.</summary>
    public InMemoryVectorIndexStore Store => Factory.For(RepoId, Space).Store;

    /// <summary>
    /// The rig's default plane options: no background build, and a training
    /// threshold low enough that a handful of vectors produces a real partitioning.
    /// </summary>
    public static RepoContextAnnOptions DefaultOptions() => new()
    {
        AutoBuild = false,
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 16,
        MaxItemsPerChunk = 8,
        RetrainAfterUpdateFraction = 0d,
    };

    /// <summary>
    /// Replaces the registry with a fresh one over the same durable store, which is
    /// exactly what a process restart does to the plane.
    /// </summary>
    public void Restart()
    {
        _registry.Dispose();
        _registry = Create();
    }

    /// <summary>Seeds one vector into the store of record.</summary>
    /// <param name="id">The vector identifier.</param>
    /// <param name="sourceKey">The canonical source key.</param>
    /// <param name="vector">The vector components.</param>
    public void Seed(string id, string sourceKey, float[] vector) => Source.Set(id, sourceKey, vector);

    /// <summary>
    /// Seeds <paramref name="count"/> unit-length vectors spread over the space's
    /// first two axes, so nearest-neighbour order is a predictable function of the
    /// angle and a test can assert which identifier must rank first.
    /// </summary>
    /// <param name="count">How many vectors to seed.</param>
    public void SeedRing(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var angle = 2d * Math.PI * i / count;
            var vector = new float[Space.Dimension];
            vector[0] = (float)Math.Cos(angle);
            vector[1] = (float)Math.Sin(angle);
            Seed(Id(i), RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
        }
    }

    /// <summary>The zero-padded identifier the ring seeder gives to one vector.</summary>
    /// <param name="ordinal">The vector's ordinal in the ring.</param>
    public static string Id(int ordinal) => $"vec-{ordinal:D6}";

    /// <summary>Drives the build for the fixture's repository and space until it serves.</summary>
    /// <param name="cancellationToken">Cancels the build.</param>
    public Task BuildAsync(CancellationToken cancellationToken)
        => _registry.EnsureBuiltAsync(RepoId, Space, cancellationToken);

    /// <summary>Searches the plane for the fixture's repository and space.</summary>
    /// <param name="query">The query vector.</param>
    /// <param name="k">The maximum number of matches.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    public ValueTask<RepoContextAnnSearchOutcome> SearchAsync(
        float[] query, int k, CancellationToken cancellationToken)
        => _registry.SearchAsync(RepoId, query, Space, k, cancellationToken);

    /// <inheritdoc />
    public void Dispose() => _registry.Dispose();

    private RepoContextAnnIndexRegistry Create() => new(
        Factory, Options, NullLogger<RepoContextAnnIndexRegistry>.Instance);
}
