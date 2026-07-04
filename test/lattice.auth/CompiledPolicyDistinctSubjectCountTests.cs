using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledPolicy.DistinctSubjectCount"/> - the count
/// of distinct members (users and groups) any rule references, which backs the
/// <c>orleans.lattice.auth.snapshot.subjects</c> observable gauge.
/// </summary>
[TestFixture]
public sealed class CompiledPolicyDistinctSubjectCountTests
{
    private static LatticeAuthorizationRule User(string id, string user, string tree) =>
        new(id, LatticeSubjectSelector.User(user), LatticeScope.Tree(tree), LatticeOperation.Read, LatticeEffect.Allow);

    private static LatticeAuthorizationRule Group(string id, string group, string tree) =>
        new(id, LatticeSubjectSelector.Group(group), LatticeScope.Tree(tree), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void Empty_policy_has_no_subjects()
    {
        Assert.That(CompiledPolicy.Compile(Array.Empty<LatticeAuthorizationRule>()).DistinctSubjectCount, Is.EqualTo(0));
        Assert.That(CompiledPolicy.Empty.DistinctSubjectCount, Is.EqualTo(0));
    }

    [Test]
    public void Distinct_users_and_groups_each_count_once()
    {
        var rules = new[]
        {
            User("r1", "alice", "t"),
            Group("r2", "admins", "t"),
        };

        Assert.That(CompiledPolicy.Compile(rules).DistinctSubjectCount, Is.EqualTo(2));
    }

    [Test]
    public void The_same_subject_across_many_rules_and_trees_counts_once()
    {
        var rules = new[]
        {
            User("r1", "alice", "t1"),
            User("r2", "alice", "t2"),
            User("r3", "alice", "t1"),
        };

        Assert.That(CompiledPolicy.Compile(rules).DistinctSubjectCount, Is.EqualTo(1));
    }

    [Test]
    public void A_user_and_a_group_sharing_an_id_count_separately()
    {
        var rules = new[]
        {
            User("r1", "shared", "t"),
            Group("r2", "shared", "t"),
        };

        Assert.That(CompiledPolicy.Compile(rules).DistinctSubjectCount, Is.EqualTo(2));
    }
}
