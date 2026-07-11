namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaVersionConfig"/> and
/// <see cref="LatticeSchemaDescriptor"/>: constructor validation and value
/// semantics.
/// </summary>
public sealed class LatticeSchemaVersionConfigTests
{
    [Test]
    public void Constructor_sets_all_members()
    {
        var config = new LatticeSchemaVersionConfig(schemaId: 7, targetVersion: 3, strictIngest: true);

        Assert.That(config.SchemaId, Is.EqualTo(7u));
        Assert.That(config.TargetVersion, Is.EqualTo(3u));
        Assert.That(config.StrictIngest, Is.True);
    }

    [Test]
    public void Constructor_defaults_strict_ingest_off()
    {
        var config = new LatticeSchemaVersionConfig(1, 1);

        Assert.That(config.StrictIngest, Is.False);
    }

    [Test]
    public void Constructor_zero_target_version_throws()
    {
        Assert.That(
            () => new LatticeSchemaVersionConfig(1, targetVersion: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void With_advances_target_version()
    {
        var config = new LatticeSchemaVersionConfig(1, 1);

        var advanced = config with { TargetVersion = 5 };

        Assert.That(advanced.TargetVersion, Is.EqualTo(5u));
        Assert.That(advanced.SchemaId, Is.EqualTo(1u));
    }

    [Test]
    public void Descriptor_constructor_sets_members()
    {
        var descriptor = new LatticeSchemaDescriptor(2, 4, "orders-v4");

        Assert.That(descriptor.SchemaId, Is.EqualTo(2u));
        Assert.That(descriptor.Version, Is.EqualTo(4u));
        Assert.That(descriptor.Name, Is.EqualTo("orders-v4"));
    }

    [Test]
    public void Descriptor_empty_name_throws()
    {
        Assert.That(() => new LatticeSchemaDescriptor(1, 1, string.Empty), Throws.ArgumentException);
        Assert.That(() => new LatticeSchemaDescriptor(1, 1, null!), Throws.InstanceOf<ArgumentException>());
    }
}
