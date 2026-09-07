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

    // The tests below cover the granted-operation detail the set carries alongside
    // group membership. A group's coarse capability mask admits the group when ANY
    // Allow rule intersects it, so a data-plane group is reachable on a bare read
    // grant; carrying the caller's actual operations lets the discovery core apply
    // a per-tool minimum inside such a group and withhold its mutating tools.

    [Test]
    public void None_carries_no_operation_detail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeApiMcpAccessSet.None.GrantedOperations, Is.EqualTo(LatticeOperation.None));
            Assert.That(LatticeApiMcpAccessSet.None.CarriesOperationDetail, Is.False);
        });
    }

    [Test]
    public void CarriesOperationDetail_is_false_for_a_group_only_set()
    {
        // A resolver that reports group membership alone keeps the historical
        // behaviour: with no operation detail the discovery core cannot apply a
        // per-tool minimum and must not withhold anything.
        var set = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(LatticeApiMcpGroup.Data), Is.True);
            Assert.That(set.CarriesOperationDetail, Is.False);
        });
    }

    [Test]
    public void WithOperations_unions_operations_and_leaves_membership_intact()
    {
        var set = LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.Data)
            .WithOperations(LatticeOperation.Read)
            .WithOperations(LatticeOperation.RangeRead);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(LatticeApiMcpGroup.Data), Is.True);
            Assert.That(set.Contains(LatticeApiMcpGroup.Auth), Is.False);
            Assert.That(set.CarriesOperationDetail, Is.True);
            Assert.That(
                set.GrantedOperations,
                Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead));
        });
    }

    [Test]
    public void WithOperations_does_not_mutate_the_source_set()
    {
        var original = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State);
        _ = original.WithOperations(LatticeOperation.Write);

        Assert.That(original.GrantedOperations, Is.EqualTo(LatticeOperation.None));
    }

    [Test]
    public void Equals_and_hashcode_track_granted_operations()
    {
        // Equality must include the operation detail, otherwise a read-only and a
        // read-write session for the same groups would compare equal and a cache
        // keyed on the access set could serve one caller's tool list to the other.
        var readOnly = LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.Data)
            .WithOperations(LatticeOperation.Read);
        var readWrite = LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.Data)
            .WithOperations(LatticeOperation.Read | LatticeOperation.Write);

        Assert.Multiple(() =>
        {
            Assert.That(readOnly, Is.Not.EqualTo(readWrite));
            Assert.That(readOnly.GetHashCode(), Is.Not.EqualTo(readWrite.GetHashCode()));
        });
    }

    [Test]
    public void Equals_holds_for_sets_built_in_a_different_order()
    {
        var a = LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.Data)
            .WithOperations(LatticeOperation.Read)
            .With(LatticeApiMcpGroup.State)
            .WithOperations(LatticeOperation.Write);
        var b = LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.State)
            .WithOperations(LatticeOperation.Write)
            .With(LatticeApiMcpGroup.Data)
            .WithOperations(LatticeOperation.Read);

        Assert.That(a, Is.EqualTo(b));
    }
}
