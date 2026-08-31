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
                RepoContextHostTrees.Content,
                RepoContextHostTrees.CrossReference,
                RepoContextHostTrees.Session,

                // The approximate index rewrites a cell's chunks on every flush and
                // range-deletes a whole superseded generation on every retrain or
                // rebuild, so it bears deletes like any other churn tree.
                RepoContextHostTrees.VectorIndex,
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
                RepoContextHostTrees.Content,
                RepoContextHostTrees.Memory,
                RepoContextHostTrees.VectorMembership,
                RepoContextHostTrees.VectorMetadata,
                RepoContextHostTrees.VectorPayload,
                RepoContextHostTrees.CrossReference,
                RepoContextHostTrees.Session,
                RepoContextHostTrees.VectorIndex,
            }));

    [Test]
    public void All_covers_every_library_tree_that_holds_repository_data()
        => Assert.That(
            RepoContextHostTrees.All,
            Is.EquivalentTo(RepoContextTrees.AllIncludingLocalDerived),
            "the host's local-agent grant list must cover exactly the library's trees that hold a "
            + "repository's data; a tree present in one but not the other means either a new "
            + "tree is unauthorised for the local agent (its writes are denied) or a grant "
            + "outlives its tree. It is compared against AllIncludingLocalDerived rather than All "
            + "because All is the REPLICATION enrolment contract, which deliberately excludes a "
            + "wholly derived local tree - being unreplicated does not make a tree ungranted.");

    [Test]
    public void The_grant_list_covers_the_local_derived_trees_replication_excludes()
        => Assert.That(
            RepoContextHostTrees.All,
            Is.SupersetOf(RepoContextTrees.LocalDerived),
            "This is the trap the split creates, so it is asserted directly: a tree left out of the "
            + "replication list must still be granted, or it fails closed on every read and write "
            + "against the default-deny gate - and does so invisibly, because the retrieval plane "
            + "degrades to the exact scan and simply never finishes building.");

    [Test]
    public void Tree_name_literals_match_the_package_conventions()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextHostTrees.Structural, Is.EqualTo("repo-context-structural"));
            Assert.That(RepoContextHostTrees.Symbol, Is.EqualTo("repo-context-symbol"));
            Assert.That(RepoContextHostTrees.Content, Is.EqualTo("repo-context-content"));
            Assert.That(RepoContextHostTrees.Memory, Is.EqualTo("repo-context-memory"));
            Assert.That(RepoContextHostTrees.VectorMembership, Is.EqualTo("repo-context-vector-membership"));
            Assert.That(RepoContextHostTrees.VectorMetadata, Is.EqualTo("repo-context-vector-metadata"));
            Assert.That(RepoContextHostTrees.VectorPayload, Is.EqualTo("repo-context-vector-payload"));
            Assert.That(RepoContextHostTrees.CrossReference, Is.EqualTo("repo-context-xref"));
            Assert.That(RepoContextHostTrees.Session, Is.EqualTo("repo-context-session"));
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
}
