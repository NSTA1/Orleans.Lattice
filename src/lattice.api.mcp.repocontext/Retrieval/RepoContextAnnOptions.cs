using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How the approximate retrieval plane shapes, builds, and maintains one
/// persisted index. Every default is chosen so an existing deployment picks the
/// plane up with no configuration at all: the index shapes itself from the corpus
/// (an automatic partition count and probe budget), builds itself in bounded
/// slices behind live traffic, and persists itself in bounded records.
/// </summary>
internal sealed class RepoContextAnnOptions
{
    /// <summary>
    /// The Lattice key prefix, inside the dedicated index tree, that every
    /// repository's index sits under. Each <c>(repository, embedding space)</c>
    /// gets its own sub-prefix beneath it, because the index's recovery path
    /// deletes whole key ranges under its own prefix and must never be able to
    /// reach another index - or any store of record.
    /// </summary>
    internal const string KeyPrefixRoot = "vidx/";

    /// <summary>
    /// How many source vectors one background build slice consumes before it
    /// checkpoints and yields. Bounds the work a single turn does, so the build
    /// never blocks a query for longer than one slice.
    /// </summary>
    public int IngestBatchSize { get; init; } = 4_096;

    /// <summary>
    /// The largest number of centroids or vectors one persisted record carries,
    /// so no record grows with the corpus.
    /// </summary>
    public int MaxItemsPerChunk { get; init; } = 1_024;

    /// <summary>
    /// How many applied maintenance updates accumulate before the plane flushes
    /// the dirty cells to durable storage. A flush costs one write per dirty cell
    /// plus the manifest, so batching keeps a bulk re-embed from rewriting the
    /// same cell once per vector.
    /// </summary>
    public int FlushAfterUpdates { get; init; } = 256;

    /// <summary>
    /// How many updates may accumulate since the partitioning was last computed,
    /// as a fraction of the corpus, before the plane retrains off the request
    /// path. Incremental maintenance keeps the index correct forever but cannot
    /// keep the cells descriptive once the corpus drifts away from the
    /// distribution they were trained on, and that loss is quiet - every record
    /// stays valid. This is the repair trigger for it.
    /// </summary>
    public double RetrainAfterUpdateFraction { get; init; } = 0.25;

    /// <summary>
    /// The distance metric the index ranks with. Cosine reproduces the exact
    /// path's ordering under both normalization conventions: for a unit-L2 space
    /// the cosine similarity and the dot product the exact ranker uses are the
    /// same quantity, and for an unnormalized space the exact ranker computes the
    /// cosine similarity too.
    /// </summary>
    public VectorDistanceMetric Metric { get; init; } = VectorDistanceMetric.Cosine;

    /// <summary>
    /// The number of partitions the index trains, or <c>0</c> to let it choose
    /// from the corpus size. Leave it at the default: a fixed partition count
    /// makes query cost linear in the corpus again for a corpus large enough,
    /// which is the whole property this plane exists to break.
    /// </summary>
    public int PartitionCount { get; init; }

    /// <summary>
    /// The number of partitions a query probes, or <c>0</c> to let the index
    /// choose. Leave it at the default: the automatic budget deliberately scans a
    /// <i>shrinking</i> fraction of the corpus as the corpus grows, which a fixed
    /// fraction of partitions would not.
    /// </summary>
    public int Probes { get; init; }

    /// <summary>
    /// The seed for the training pass, so a rebuild of the same corpus produces
    /// the same partitioning and a recall measurement is reproducible.
    /// </summary>
    public ulong Seed { get; init; } = 20_260_101;

    /// <summary>
    /// The smallest corpus the index will train a partitioning for. Below it the
    /// index legitimately finishes its build with no partitioning and answers
    /// exactly by exhaustive scan, which is correct and is reported as
    /// <see cref="RepoContextAnnServingState.Exhaustive"/> rather than as
    /// approximate.
    /// </summary>
    public int MinimumTrainingCount { get; init; } = 1_024;

    /// <summary>
    /// Projects these options onto the durable index configuration for one
    /// embedding space, whose dimensionality fixes the index's own.
    /// </summary>
    /// <param name="space">The embedding space the index covers.</param>
    /// <param name="keyPrefix">The key prefix this index owns exclusively. Must not be <see langword="null"/>.</param>
    /// <returns>The durable index configuration.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="keyPrefix"/> is null.</exception>
    internal DurableVectorIndexOptions ToDurableOptions(EmbeddingSpaceTag space, string keyPrefix)
    {
        ArgumentNullException.ThrowIfNull(keyPrefix);

        return new DurableVectorIndexOptions
        {
            KeyPrefix = keyPrefix,
            IngestBatchSize = IngestBatchSize,
            MaxItemsPerChunk = MaxItemsPerChunk,
            Index = new VectorIndexOptions
            {
                Dimensions = space.Dimension,
                Metric = Metric,
                PartitionCount = PartitionCount,
                Probes = Probes,
                Seed = Seed,
                MinimumTrainingCount = MinimumTrainingCount,
            },
        };
    }
}
