using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Issue #3985: a version config read as the value-type default (family 0 at
/// version 0) is the unversioned sentinel, not versioning.
/// </summary>
[TestFixture]
public sealed class SchemaVersioningTests
{
    [Test]
    public void The_default_config_is_unversioned() =>
        Assert.That(SchemaVersioning.Effective(default(LatticeSchemaVersionConfig)), Is.Null);

    [Test]
    public void No_config_is_unversioned() =>
        Assert.That(SchemaVersioning.Effective(null), Is.Null);

    [Test]
    public void A_config_with_a_target_version_is_kept_as_it_is()
    {
        var config = new LatticeSchemaVersionConfig(0, 1, strictIngest: true);

        Assert.That(SchemaVersioning.Effective(config), Is.EqualTo(config));
    }
}
