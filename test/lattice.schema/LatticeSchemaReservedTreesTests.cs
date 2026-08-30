namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaReservedTrees"/>: the public
/// reserved-namespace guard applications use to keep their own tree ids clear of
/// the schema store's <c>sys-schema-*</c> namespace.
/// </summary>
public class LatticeSchemaReservedTreesTests
{
    [Test]
    public void Prefix_and_reserved_tree_ids_are_exposed()
    {
        Assert.That(LatticeSchemaReservedTrees.Prefix, Is.EqualTo("sys-schema-"));
        Assert.That(LatticeSchemaReservedTrees.PolicyTreeId, Is.EqualTo("sys-schema-policy"));
        Assert.That(LatticeSchemaReservedTrees.DeadLetterTreeId, Is.EqualTo("sys-schema-dlq"));
        Assert.That(LatticeSchemaReservedTrees.VersionConfigTreeId, Is.EqualTo("sys-schema-version"));
        Assert.That(SchemaConstants.AllTrees, Is.EquivalentTo(new[]
        {
            LatticeSchemaReservedTrees.PolicyTreeId,
            LatticeSchemaReservedTrees.DeadLetterTreeId,
            LatticeSchemaReservedTrees.VersionConfigTreeId,
        }));
    }

    [Test]
    public void IsReserved_true_for_reserved_prefix()
    {
        Assert.That(LatticeSchemaReservedTrees.IsReserved("sys-schema-policy"), Is.True);
        Assert.That(LatticeSchemaReservedTrees.IsReserved("sys-schema-anything"), Is.True);
    }

    [Test]
    public void IsReserved_false_for_application_tree()
    {
        Assert.That(LatticeSchemaReservedTrees.IsReserved("orders"), Is.False);
    }

    [Test]
    public void IsReserved_null_throws()
    {
        Assert.That(() => LatticeSchemaReservedTrees.IsReserved(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ThrowIfReserved_throws_for_reserved_and_empty()
    {
        Assert.That(() => LatticeSchemaReservedTrees.ThrowIfReserved("sys-schema-x"), Throws.ArgumentException);
        Assert.That(() => LatticeSchemaReservedTrees.ThrowIfReserved(string.Empty), Throws.ArgumentException);
        Assert.That(() => LatticeSchemaReservedTrees.ThrowIfReserved(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ThrowIfReserved_uses_supplied_parameter_name()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => LatticeSchemaReservedTrees.ThrowIfReserved("sys-schema-x", "tree"));

        Assert.That(ex!.ParamName, Is.EqualTo("tree"));
    }

    [Test]
    public void ThrowIfReserved_returns_for_application_tree()
    {
        Assert.That(() => LatticeSchemaReservedTrees.ThrowIfReserved("orders"), Throws.Nothing);
    }
}
