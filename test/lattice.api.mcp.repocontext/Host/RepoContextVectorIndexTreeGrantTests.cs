using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Guards the one thing that makes the persisted approximate index reachable at
/// all on the container host: its tree must carry a local-agent grant.
/// <para>
/// The box runs a fail-closed default-deny access gate and seeds exactly one Allow
/// rule per <see cref="RepoContextHostTrees.All"/> entry. A tree missing from that
/// list is denied on every read and write - and the resulting failure is close to
/// invisible, because the retrieval plane is deliberately fault-tolerant: the build
/// would fault, be caught, logged, and re-armed, retrieval would keep serving
/// correctly through the exact scan, and the index would simply never finish
/// building. Every unit test would still pass, because none of them go through the
/// gate. That is exactly the shape of silent degradation this work exists to
/// remove, so it is asserted here rather than left to a deployment to discover.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextVectorIndexTreeGrantTests
{
    [Test]
    public void The_index_tree_is_granted_to_the_local_agent()
    {
        Assert.That(RepoContextHostTrees.All, Does.Contain(RepoContextHostTrees.VectorIndex),
            "The host seeds one Allow rule per tree in this list. Omitting the index tree leaves it denied by "
            + "the default-deny gate, and the plane then never finishes building while retrieval quietly keeps "
            + "answering from the exact scan.");
    }

    [Test]
    public void The_index_tree_is_compacted_as_a_churn_tree()
    {
        Assert.That(RepoContextHostTrees.ChurnTrees, Does.Contain(RepoContextHostTrees.VectorIndex),
            "The index rewrites a cell's chunks on every flush and range-deletes a whole superseded generation "
            + "on every retrain, so its tombstones must be reaped like any other churn tree.");
    }

    [Test]
    public void The_index_tree_name_matches_the_package_constant_it_mirrors()
    {
        // The host cannot reference the package-internal constant, so the literal is
        // duplicated there. A drift between the two would point the grant and the
        // compaction overrides at a tree that does not exist, which fails exactly as
        // silently as omitting them. This test project sees both, so it can hold the
        // two in step directly rather than by eye.
        Assert.That(RepoContextHostTrees.VectorIndex, Is.EqualTo(RepoContextTrees.VectorIndex));
    }

    [Test]
    public void Every_host_tree_literal_still_matches_a_real_package_tree()
    {
        // The same drift hazard applies to the whole mirrored list, and the index tree
        // is the first entry added to it since the mirror was written.
        Assert.That(
            RepoContextHostTrees.All,
            Is.SubsetOf(RepoContextTrees.AllIncludingLocalDerived),
            "A host literal with no package counterpart grants access to a tree nothing ever writes to.");
    }

    [Test]
    public void The_index_tree_is_swept_when_a_repository_is_removed()
    {
        // The index sits under the same repo/{repoId}/ prefix as everything else, so a
        // removal that skipped its tree would leave the whole index behind - and a
        // repository later registered under the same id would load it.
        Assert.That(
            RepoContextTrees.AllIncludingLocalDerived,
            Does.Contain(RepoContextTrees.VectorIndex),
            "Repository teardown iterates this list, so a tree missing from it outlives the repository.");
    }

    [Test]
    public void The_index_tree_is_excluded_from_the_replication_enrolment_list()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTrees.All, Does.Not.Contain(RepoContextTrees.VectorIndex),
                "Replicating a derived index ships what each cluster rebuilds more cheaply, and interleaves "
                + "two clusters' generations under a layout whose recovery deletes whole key ranges.");
            Assert.That(RepoContextTrees.LocalDerived, Does.Contain(RepoContextTrees.VectorIndex),
                "The exclusion is deliberate and named, not an omission.");
        });
    }
}
