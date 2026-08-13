using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the compaction wiring metadata: the churn-tree set excludes the
/// write-once payload tree, and the per-tree compaction constants are finite and
/// positive so re-embed / prune tombstones are actually reaped.
/// </summary>
[TestFixture]
public sealed class RepoContextCompactionTests
{
    [Test]
    public void ChurnTrees_are_the_delete_bearing_trees()
        => Assert.That(
            RepoContextHostTrees.ChurnTrees,
            Is.EquivalentTo(new[]
            {
                RepoContextHostTrees.Memory,
                RepoContextHostTrees.VectorMembership,
                RepoContextHostTrees.VectorMetadata,
                RepoContextHostTrees.Structural,
                RepoContextHostTrees.Symbol,
            }));

    [Test]
    public void ChurnTrees_exclude_the_write_once_vector_payload_tree()
        => Assert.That(RepoContextHostTrees.ChurnTrees, Does.Not.Contain(RepoContextHostTrees.VectorPayload));

    [Test]
    public void All_lists_every_repository_context_tree()
        => Assert.That(
            RepoContextHostTrees.All,
            Is.EquivalentTo(new[]
            {
                RepoContextHostTrees.Structural,
                RepoContextHostTrees.Symbol,
                RepoContextHostTrees.Memory,
                RepoContextHostTrees.VectorMembership,
                RepoContextHostTrees.VectorMetadata,
                RepoContextHostTrees.VectorPayload,
            }));

    [Test]
    public void Tree_name_literals_match_the_package_conventions()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextHostTrees.Structural, Is.EqualTo("repo-context-structural"));
            Assert.That(RepoContextHostTrees.Symbol, Is.EqualTo("repo-context-symbol"));
            Assert.That(RepoContextHostTrees.Memory, Is.EqualTo("repo-context-memory"));
            Assert.That(RepoContextHostTrees.VectorMembership, Is.EqualTo("repo-context-vector-membership"));
            Assert.That(RepoContextHostTrees.VectorMetadata, Is.EqualTo("repo-context-vector-metadata"));
            Assert.That(RepoContextHostTrees.VectorPayload, Is.EqualTo("repo-context-vector-payload"));
        });

    [Test]
    public void Compaction_grace_period_is_finite_never_infinite()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextCompaction.ChurnTombstoneGracePeriod, Is.Not.EqualTo(Timeout.InfiniteTimeSpan));
            Assert.That(RepoContextCompaction.ChurnTombstoneGracePeriod, Is.GreaterThan(TimeSpan.Zero));
        });

    [Test]
    public void Compaction_ratio_and_leaf_triggers_are_positive()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextCompaction.ChurnMinTombstoneRatio, Is.GreaterThan(0.0));
            Assert.That(RepoContextCompaction.ChurnMaxLeafEntriesBeforeForcedCompaction, Is.GreaterThan(0));
        });

    [Test]
    public void ConfigureRepoContextCompaction_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextCompaction.ConfigureRepoContextCompaction(null!),
            Throws.ArgumentNullException);

    [Test]
    public void VectorTrees_are_the_three_derived_embedding_trees()
        => Assert.That(
            RepoContextHostTrees.VectorTrees,
            Is.EquivalentTo(new[]
            {
                RepoContextHostTrees.VectorMembership,
                RepoContextHostTrees.VectorMetadata,
                RepoContextHostTrees.VectorPayload,
            }));

    [Test]
    public void VectorTrees_exclude_the_store_of_record_trees()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextHostTrees.VectorTrees, Does.Not.Contain(RepoContextHostTrees.Structural));
            Assert.That(RepoContextHostTrees.VectorTrees, Does.Not.Contain(RepoContextHostTrees.Symbol));
            Assert.That(RepoContextHostTrees.VectorTrees, Does.Not.Contain(RepoContextHostTrees.Memory));
        });

    [Test]
    public void ConfigureRepoContextVectorProjectionRecovery_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextCompaction.ConfigureRepoContextVectorProjectionRecovery(null!),
            Throws.ArgumentNullException);
}
