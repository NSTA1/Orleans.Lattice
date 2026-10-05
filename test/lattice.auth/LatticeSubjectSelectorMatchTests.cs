using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeSubjectSelector.Matches"/>, the one subject
/// match the access-administration introspection surfaces (operator and tenant
/// tier) share: a user selector matches its user id ordinally, a group selector
/// matches through the subject's resolved group closure, and an unknown kind
/// matches nothing.
/// </summary>
[TestFixture]
public sealed class LatticeSubjectSelectorMatchTests
{
    private static readonly HashSet<string> Groups = new(StringComparer.Ordinal) { "admins", "ops" };

    [Test]
    public void A_user_selector_matches_only_its_own_user_id_ordinally()
    {
        var selector = LatticeSubjectSelector.User("alice");

        Assert.Multiple(() =>
        {
            Assert.That(selector.Matches("alice", Groups), Is.True);
            Assert.That(selector.Matches("Alice", Groups), Is.False);
            Assert.That(selector.Matches("bob", Groups), Is.False);
        });
    }

    [Test]
    public void A_user_selector_does_not_match_through_a_group_of_the_same_id()
    {
        Assert.That(LatticeSubjectSelector.User("admins").Matches("bob", Groups), Is.False);
    }

    [Test]
    public void A_group_selector_matches_through_the_group_closure()
    {
        var selector = LatticeSubjectSelector.Group("ops");

        Assert.Multiple(() =>
        {
            Assert.That(selector.Matches("bob", Groups), Is.True);
            Assert.That(selector.Matches("bob", new HashSet<string>(StringComparer.Ordinal)), Is.False);
            Assert.That(LatticeSubjectSelector.Group("bob").Matches("bob", Groups), Is.False);
        });
    }

    [Test]
    public void An_unknown_selector_kind_matches_nothing()
    {
        var selector = new LatticeSubjectSelector((LatticeSubjectSelectorKind)99, "alice");

        Assert.That(selector.Matches("alice", Groups), Is.False);
    }
}
