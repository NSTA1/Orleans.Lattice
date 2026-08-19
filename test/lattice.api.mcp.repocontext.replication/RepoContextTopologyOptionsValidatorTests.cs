using Orleans.Lattice.Api.Mcp.RepoContext;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication.Tests;

/// <summary>
/// Coverage for <see cref="RepoContextTopologyOptionsValidator"/>, the startup guard
/// that fails fast when a resolved repository-context replication topology is
/// inconsistent with the hub-and-spoke invariant (a single indexer, multi-master
/// memory, add-wins membership). The validator engages only when a repository-context
/// tree is enrolled, so an unrelated replication configuration passes untouched.
/// </summary>
[TestFixture]
public sealed class RepoContextTopologyOptionsValidatorTests
{
    private static LatticeReplicationOptions Options(IReadOnlyDictionary<string, LatticeMergeMode>? trees) =>
        new() { ReplicatedTrees = trees };

    private static Dictionary<string, LatticeMergeMode> Enrolment() =>
        new(RepoContextReplicatedTrees.BuildEnrolmentMap(), StringComparer.Ordinal);

    [Test]
    public void Validate_passes_the_reserved_enrolment_map()
    {
        var result = new RepoContextTopologyOptionsValidator()
            .Validate(null, Options(RepoContextReplicatedTrees.BuildEnrolmentMap()));

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_passes_when_no_repo_context_tree_is_enrolled()
    {
        var trees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            ["some.other.tree"] = LatticeMergeMode.LwwRegister,
        };

        var result = new RepoContextTopologyOptionsValidator().Validate(null, Options(trees));

        Assert.That(result.Succeeded, Is.True, "The guard says nothing about an unrelated replication configuration.");
    }

    [Test]
    public void Validate_passes_when_replicated_trees_is_null()
    {
        var result = new RepoContextTopologyOptionsValidator().Validate(null, Options(null));

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_rejects_a_last_writer_wins_memory_tree()
    {
        var trees = Enrolment();
        trees[RepoContextTrees.Memory] = LatticeMergeMode.LwwRegister;

        var result = new RepoContextTopologyOptionsValidator().Validate(null, Options(trees));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(RepoContextTrees.Memory));
            Assert.That(result.FailureMessage, Does.Contain("multi-master"));
        });
    }

    [Test]
    public void Validate_rejects_a_last_writer_wins_membership_tree()
    {
        var trees = Enrolment();
        trees[RepoContextTrees.VectorMembership] = LatticeMergeMode.LwwRegister;

        var result = new RepoContextTopologyOptionsValidator().Validate(null, Options(trees));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(RepoContextTrees.VectorMembership));
            Assert.That(result.FailureMessage, Does.Contain("add-wins"));
        });
    }

    [Test]
    public void Validate_rejects_a_crdt_index_plane_tree_as_implied_active_active_indexing()
    {
        // A CRDT merge mode on a single-writer index-plane tree implies more than one
        // cluster mutating source-derived index state - active-active indexing - which
        // the single-indexer hub-and-spoke topology forbids.
        var trees = Enrolment();
        trees[RepoContextTrees.Structural] = LatticeMergeMode.MvRegister;

        var result = new RepoContextTopologyOptionsValidator().Validate(null, Options(trees));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(RepoContextTrees.Structural));
            Assert.That(result.FailureMessage, Does.Contain("active-active"));
        });
    }

    [Test]
    public void Validate_reports_every_inconsistent_tree_at_once()
    {
        var trees = Enrolment();
        trees[RepoContextTrees.Memory] = LatticeMergeMode.LwwRegister;
        trees[RepoContextTrees.VectorMembership] = LatticeMergeMode.LwwRegister;
        trees[RepoContextTrees.Symbol] = LatticeMergeMode.OrFlag;

        var result = new RepoContextTopologyOptionsValidator().Validate(null, Options(trees));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(RepoContextTrees.Memory));
            Assert.That(result.FailureMessage, Does.Contain(RepoContextTrees.VectorMembership));
            Assert.That(result.FailureMessage, Does.Contain(RepoContextTrees.Symbol));
        });
    }

    [Test]
    public void Validate_rejects_a_null_options()
    {
        Assert.That(
            () => new RepoContextTopologyOptionsValidator().Validate(null, null!),
            Throws.ArgumentNullException);
    }
}
