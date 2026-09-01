using Orleans.Lattice.Vector;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the approximate plane's configuration surface: the shaping
/// options it projects onto a durable index, and the exclusive key prefix each
/// <c>(repository, embedding space)</c> pair owns.
/// </summary>
[TestFixture]
public sealed class RepoContextAnnOptionsTests
{
    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 384, VectorNormalization.UnitL2);

    [Test]
    public void The_defaults_shape_the_index_from_the_corpus_rather_than_fixing_it()
    {
        var options = new RepoContextAnnOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.PartitionCount, Is.Zero,
                "Zero means the index chooses from the corpus size; a fixed count makes query cost linear again.");
            Assert.That(options.Probes, Is.Zero,
                "Zero means the index chooses its probe budget, which scans a shrinking fraction as it grows.");
            Assert.That(options.AutoBuild, Is.True,
                "An existing deployment must heal itself with no operator action.");
            Assert.That(options.Metric, Is.EqualTo(VectorDistanceMetric.Cosine),
                "Cosine reproduces the exact ranker's ordering under both normalization conventions.");
            Assert.That(options.IngestBatchSize, Is.GreaterThan(0));
            Assert.That(options.MaxItemsPerChunk, Is.GreaterThan(0));
            Assert.That(options.FlushAfterUpdates, Is.GreaterThan(0));
            Assert.That(options.MinimumTrainingCount, Is.GreaterThan(0));
            Assert.That(options.RetrainAfterUpdateFraction, Is.GreaterThan(0d));
        });
    }

    [Test]
    public void The_durable_projection_takes_its_dimensionality_from_the_embedding_space()
    {
        var options = new RepoContextAnnOptions { PartitionCount = 7, Probes = 3, Seed = 11UL };

        var durable = options.ToDurableOptions(Space, "repo/acme/vidx/abc/");

        Assert.Multiple(() =>
        {
            Assert.That(durable.Index.Dimensions, Is.EqualTo(Space.Dimension),
                "An index that disagreed with its space would reject every vector the source yields.");
            Assert.That(durable.KeyPrefix, Is.EqualTo("repo/acme/vidx/abc/"));
            Assert.That(durable.Index.PartitionCount, Is.EqualTo(7));
            Assert.That(durable.Index.Probes, Is.EqualTo(3));
            Assert.That(durable.Index.Seed, Is.EqualTo(11UL));
            Assert.That(durable.IngestBatchSize, Is.EqualTo(options.IngestBatchSize));
            Assert.That(durable.MaxItemsPerChunk, Is.EqualTo(options.MaxItemsPerChunk));
        });
    }

    [Test]
    public void The_durable_projection_rejects_a_null_key_prefix()
    {
        Assert.That(() => new RepoContextAnnOptions().ToDurableOptions(Space, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Each_repository_and_space_pair_owns_a_distinct_key_prefix()
    {
        var other = new EmbeddingSpaceTag("other-model", 384, VectorNormalization.UnitL2);
        var rescaled = new EmbeddingSpaceTag("test-model", 768, VectorNormalization.UnitL2);
        var unnormalized = new EmbeddingSpaceTag("test-model", 384, VectorNormalization.None);

        var acme = LatticeRepoContextAnnBackingFactory.KeyPrefix("acme", Space);

        Assert.Multiple(() =>
        {
            Assert.That(acme, Is.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix("acme", Space)),
                "The prefix is stable, or a restart would open onto a different index than it persisted.");
            Assert.That(acme, Is.Not.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix("other", Space)),
                "Two repositories must not share a prefix: recovery deletes whole key ranges under it.");
            Assert.That(acme, Is.Not.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix("acme", other)));
            Assert.That(acme, Is.Not.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix("acme", rescaled)));
            Assert.That(acme, Is.Not.EqualTo(LatticeRepoContextAnnBackingFactory.KeyPrefix("acme", unnormalized)),
                "A different normalization convention is a different embedding space, so a different index.");
            Assert.That(acme, Does.StartWith("repo/acme/"), "The prefix is repository-scoped.");
            Assert.That(acme, Does.EndWith("/"), "A prefix that is not a boundary can capture a sibling's keys.");
            Assert.That(acme, Does.Not.Contain(Space.ModelId),
                "The model id is fingerprinted rather than carried verbatim into a key.");
        });
    }

    [Test]
    public void The_key_prefix_rejects_a_null_repository()
    {
        Assert.That(() => LatticeRepoContextAnnBackingFactory.KeyPrefix(null!, Space),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_index_tree_is_deliberately_outside_the_replicated_layout()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTrees.VectorIndex, Is.EqualTo("repo-context-vector-index"));
            Assert.That(RepoContextTrees.All, Does.Not.Contain(RepoContextTrees.VectorIndex),
                "The index is derived and local: replicating it would ship what each cluster builds more cheaply, "
                + "and would interleave two clusters' generations under a layout whose recovery deletes key ranges.");
            Assert.That(RepoContextTrees.IsRebuildableVectorTree(RepoContextTrees.VectorIndex), Is.False,
                "The self-healing re-deriver's allow-list is a closed set and this tree is not on it.");
        });
    }
}
