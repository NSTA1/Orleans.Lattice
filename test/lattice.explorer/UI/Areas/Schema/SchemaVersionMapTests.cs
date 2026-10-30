using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Issue #5234: the version map reads a tree's catalog against its config the way the
/// cluster's read path does - a value below the target follows the hop registered for
/// its version, then the next, until it reaches the target.
/// </summary>
[TestFixture]
public sealed class SchemaVersionMapTests
{
    private static LatticeSchemaRuntimeUpcaster Hop(uint family, uint from, uint to, string? transformId = null) => new()
    {
        SchemaId = family,
        FromVersion = from,
        ToVersion = to,
        Transform = transformId is null ? LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("a" + from, "a" + to)) : null,
        TransformId = transformId,
    };

    private static LatticeSchemaRuntimeRegistration Catalog() => new()
    {
        Descriptors = [new(7, 1, "Initial"), new(7, 2, "Renamed"), new(7, 3, "Orders"), new(7, 4, "Split"), new(7, 6, "Future"), new(9, 1, "Other")],
        Upcasters = [Hop(7, 1, 2), Hop(7, 2, 3, "orders-v3"), Hop(9, 1, 2)],
        SupportedTransformIds = ["orders-v3"],
    };

    [Test]
    public void Each_version_of_the_configured_family_reads_as_the_cluster_reads_it()
    {
        var map = SchemaVersionMap.Build(Catalog(), new LatticeSchemaVersionConfig(7, 3));

        var family = map.Families[0];
        Assert.Multiple(() =>
        {
            Assert.That(family.SchemaId, Is.EqualTo(7u));
            Assert.That(family.IsConfigured, Is.True);
            Assert.That(family.Versions.Select(row => (row.Version, row.Reach)), Is.EqualTo(new[]
            {
                (1u, SchemaVersionReach.Upgraded),
                (2u, SchemaVersionReach.Upgraded),
                (3u, SchemaVersionReach.Target),
                (4u, SchemaVersionReach.AboveTarget),
                (6u, SchemaVersionReach.AboveTarget),
            }));
            Assert.That(family.Versions[0].Route, Is.EqualTo(new uint[] { 1, 2, 3 }));
            Assert.That(family.Versions[0].Name, Is.EqualTo("Initial"));
        });
    }

    [Test]
    public void A_version_with_no_chain_to_the_target_cannot_be_read()
    {
        var map = SchemaVersionMap.Build(Catalog(), new LatticeSchemaVersionConfig(7, 6));

        Assert.That(map.Families[0].Versions.Select(row => (row.Version, row.Reach)), Is.EqualTo(new[]
        {
            (1u, SchemaVersionReach.NoPath),
            (2u, SchemaVersionReach.NoPath),
            (3u, SchemaVersionReach.NoPath),
            (4u, SchemaVersionReach.NoPath),
            (6u, SchemaVersionReach.Target),
        }));
    }

    [Test]
    public void A_hop_past_the_target_is_not_a_route_to_it()
    {
        // The read path refuses a hop whose destination overshoots the target.
        var catalog = new LatticeSchemaRuntimeRegistration { Upcasters = [Hop(7, 1, 4)] };

        var map = SchemaVersionMap.Build(catalog, new LatticeSchemaVersionConfig(7, 3));

        Assert.Multiple(() =>
        {
            Assert.That(map.CanUpgrade(7, 1, 3), Is.False);
            Assert.That(map.CanUpgrade(7, 1, 4), Is.True);
            Assert.That(map.CanUpgrade(7, 3, 3), Is.True);
            Assert.That(map.CanUpgrade(7, 4, 3), Is.False);
        });
    }

    [Test]
    public void The_configured_family_comes_first_and_others_are_not_judged()
    {
        var map = SchemaVersionMap.Build(Catalog(), new LatticeSchemaVersionConfig(9, 2));

        Assert.Multiple(() =>
        {
            Assert.That(map.Families.Select(family => family.SchemaId), Is.EqualTo(new uint[] { 9, 7 }));
            Assert.That(map.Families[0].Versions.Select(row => row.Reach), Is.EqualTo(new[] { SchemaVersionReach.Upgraded, SchemaVersionReach.Target }));
            Assert.That(map.Families[0].Versions[1].Name, Is.Null, "the target is listed although no descriptor names it");
            Assert.That(map.Families[1].Versions.Select(row => row.Reach).Distinct(), Is.EqualTo(new[] { SchemaVersionReach.Unconfigured }));
        });
    }

    [Test]
    public void An_unversioned_tree_judges_no_family()
    {
        var map = SchemaVersionMap.Build(Catalog(), config: null);

        Assert.Multiple(() =>
        {
            Assert.That(map.Families.All(family => !family.IsConfigured), Is.True);
            Assert.That(map.StrandedBy(5), Is.Empty);
        });
    }

    [Test]
    public void Hops_say_what_they_change_or_which_installed_code_they_run()
    {
        var catalog = Catalog() with { SupportedTransformIds = [] };

        var hops = SchemaVersionMap.Build(catalog, new LatticeSchemaVersionConfig(7, 3)).Families[0].Hops;

        Assert.Multiple(() =>
        {
            Assert.That(hops[0], Is.EqualTo(hops[0] with { FromVersion = 1, ToVersion = 2, TransformId = null, Installed = true }));
            Assert.That(hops[0].Steps, Is.EqualTo(new[] { "Renames a1 to a2." }));
            Assert.That(hops[1].TransformId, Is.EqualTo("orders-v3"));
            Assert.That(hops[1].Steps, Is.Empty);
            Assert.That(hops[1].Installed, Is.False, "the answering host does not advertise it");
        });
    }

    [Test]
    public void Advancing_names_the_registered_versions_it_would_strand()
    {
        var map = SchemaVersionMap.Build(Catalog(), new LatticeSchemaVersionConfig(7, 3));

        Assert.Multiple(() =>
        {
            Assert.That(map.StrandedBy(3), Is.Empty);
            Assert.That(map.StrandedBy(4), Is.EqualTo(new uint[] { 1, 2, 3 }));
            Assert.That(map.IsRegistered(7, 4), Is.True);
            Assert.That(map.IsRegistered(7, 5), Is.False);
        });
    }

    [Test]
    public void An_empty_catalog_is_empty_but_still_lists_the_target()
    {
        var map = SchemaVersionMap.Build(new LatticeSchemaRuntimeRegistration(), new LatticeSchemaVersionConfig(7, 3));

        Assert.Multiple(() =>
        {
            Assert.That(map.IsEmpty, Is.True);
            Assert.That(map.Families.Single().Versions.Single().Reach, Is.EqualTo(SchemaVersionReach.Target));
            Assert.That(map.StrandedBy(4), Is.EqualTo(new uint[] { 3 }));
        });
    }

    [Test]
    public void A_null_catalog_is_refused()
    {
        Assert.That(() => SchemaVersionMap.Build(null!, null), Throws.ArgumentNullException);
    }
}
