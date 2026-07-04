using System.Collections.Generic;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSubject"/>: constructor validation,
/// group-closure defaulting, the anonymity predicate, and the two well-known
/// singletons.
/// </summary>
[TestFixture]
public class LatticeSubjectTests
{
    [Test]
    public void Constructor_null_subject_id_throws()
    {
        Assert.That(() => new LatticeSubject(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_with_subject_only_defaults_groups_to_empty_and_claims_null()
    {
        var subject = new LatticeSubject("alice");

        Assert.That(subject.SubjectId, Is.EqualTo("alice"));
        Assert.That(subject.GroupIds, Is.Not.Null);
        Assert.That(subject.GroupIds, Is.Empty);
        Assert.That(subject.Claims, Is.Null);
    }

    [Test]
    public void Constructor_records_supplied_groups_and_claims()
    {
        var groups = new[] { "admins", "readers" };
        var claims = new Dictionary<string, string> { ["dept"] = "eng" };

        var subject = new LatticeSubject("alice", groups, claims);

        Assert.That(subject.GroupIds, Is.EquivalentTo(groups));
        Assert.That(subject.Claims, Is.EqualTo(claims));
    }

    [Test]
    public void Null_groups_are_treated_as_empty_set()
    {
        var subject = new LatticeSubject("alice", groupIds: null);

        Assert.That(subject.GroupIds, Is.Empty);
    }

    [Test]
    public void IsAnonymous_is_true_only_for_the_anonymous_subject_id()
    {
        Assert.That(new LatticeSubject(LatticeSubject.AnonymousSubjectId).IsAnonymous, Is.True);
        Assert.That(new LatticeSubject("alice").IsAnonymous, Is.False);
    }

    [Test]
    public void Anonymous_singleton_is_anonymous_with_no_groups_or_claims()
    {
        var anonymous = LatticeSubject.Anonymous;

        Assert.That(anonymous.SubjectId, Is.EqualTo(LatticeSubject.AnonymousSubjectId));
        Assert.That(anonymous.IsAnonymous, Is.True);
        Assert.That(anonymous.GroupIds, Is.Empty);
        Assert.That(anonymous.Claims, Is.Null);
    }

    [Test]
    public void System_singleton_carries_the_system_subject_id_and_is_not_anonymous()
    {
        var system = LatticeSubject.System;

        Assert.That(system.SubjectId, Is.EqualTo(LatticeSubject.SystemSubjectId));
        Assert.That(system.IsAnonymous, Is.False);
    }

    [Test]
    public void Value_equality_holds_for_equal_subject_ids_and_default_state()
    {
        Assert.That(new LatticeSubject("alice"), Is.EqualTo(new LatticeSubject("alice")));
    }
}
