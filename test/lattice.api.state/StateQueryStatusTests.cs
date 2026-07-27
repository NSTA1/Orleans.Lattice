namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Wire-contract guard for <see cref="StateQueryStatus"/>. The enum is carried
/// by value on the serialized state-query response DTOs (for example
/// <c>EntryGetResponse.Status</c>), so its numeric values are part of the gRPC
/// wire format: renumbering an existing member would silently re-interpret an
/// older client's responses. New members may be appended with new values; the
/// existing ordinals must never move.
/// </summary>
[TestFixture]
public sealed class StateQueryStatusTests
{
    [Test]
    public void Found_is_zero_so_it_is_the_default()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)StateQueryStatus.Found, Is.EqualTo(0));
            Assert.That(default(StateQueryStatus), Is.EqualTo(StateQueryStatus.Found));
        });
    }

    [Test]
    public void TreeNotFound_keeps_its_ordinal()
    {
        Assert.That((int)StateQueryStatus.TreeNotFound, Is.EqualTo(1));
    }

    [Test]
    public void KeyNotFound_keeps_its_ordinal()
    {
        Assert.That((int)StateQueryStatus.KeyNotFound, Is.EqualTo(2));
    }

    [Test]
    public void IndexNotFound_keeps_its_ordinal()
    {
        Assert.That((int)StateQueryStatus.IndexNotFound, Is.EqualTo(3));
    }

    [Test]
    public void The_known_members_are_distinct()
    {
        var known = new[]
        {
            StateQueryStatus.Found,
            StateQueryStatus.TreeNotFound,
            StateQueryStatus.KeyNotFound,
            StateQueryStatus.IndexNotFound,
        };

        Assert.That(known.Cast<int>().Distinct().Count(), Is.EqualTo(known.Length));
    }
}
