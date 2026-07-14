namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpAccessSet"/>, the immutable bitmask
/// the discovery core uses to represent the facade groups a caller may use.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpAccessSetTests
{
    [Test]
    public void None_grants_no_group_and_is_empty()
    {
        var set = LatticeApiMcpAccessSet.None;

        Assert.Multiple(() =>
        {
            Assert.That(set.IsEmpty, Is.True);
            Assert.That(set.Contains(LatticeApiMcpGroup.State), Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Data), Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Backup), Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public void With_adds_the_group_and_leaves_the_others_absent()
    {
        var set = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data);

        Assert.Multiple(() =>
        {
            Assert.That(set.IsEmpty, Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Data), Is.True);
            Assert.That(set.Contains(LatticeApiMcpGroup.State), Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Backup), Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public void With_is_idempotent_for_a_repeated_group()
    {
        var once = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Auth);
        var twice = once.With(LatticeApiMcpGroup.Auth);

        Assert.That(twice, Is.EqualTo(once));
    }

    [Test]
    public void With_accumulates_multiple_groups()
    {
        var set = LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.State)
            .With(LatticeApiMcpGroup.Backup);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(LatticeApiMcpGroup.State), Is.True);
            Assert.That(set.Contains(LatticeApiMcpGroup.Backup), Is.True);
            Assert.That(set.Contains(LatticeApiMcpGroup.Data), Is.False);
            Assert.That(set.Contains(LatticeApiMcpGroup.Auth), Is.False);
        });
    }

    [Test]
    public void Does_not_mutate_the_source_set()
    {
        var original = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State);
        _ = original.With(LatticeApiMcpGroup.Data);

        Assert.That(original.Contains(LatticeApiMcpGroup.Data), Is.False,
            "With must return a new value and leave the source unchanged.");
    }

    [Test]
    public void Equals_and_hashcode_track_membership()
    {
        var a = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State).With(LatticeApiMcpGroup.Data);
        var b = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data).With(LatticeApiMcpGroup.State);
        var c = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State);

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
            Assert.That(a, Is.Not.EqualTo(c));
            Assert.That(a.Equals((object)b), Is.True);
            Assert.That(a.Equals("not a set"), Is.False);
        });
    }
}
