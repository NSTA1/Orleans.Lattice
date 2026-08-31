using System.Reflection;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="IndexedAttribute"/>: the marker that opts a grain into its
/// declared indexes, and the persistence configuration it carries in place of
/// <see cref="PersistentStateAttribute"/>.
/// </summary>
[TestFixture]
public sealed class IndexedAttributeTests
{
    [Test]
    public void The_attribute_is_an_orleans_facet_so_the_runtime_binds_it()
    {
        Assert.That(typeof(IFacetMetadata).IsAssignableFrom(typeof(IndexedAttribute)), Is.True,
            "The facet interface is what makes Orleans look for a factory mapper for the attribute; "
            + "without it the parameter is just an unresolvable constructor argument.");
    }

    [Test]
    public void The_attribute_configures_persistence_as_well_as_indexing()
    {
        Assert.That(
            typeof(IPersistentStateConfiguration).IsAssignableFrom(typeof(IndexedAttribute)), Is.True,
            "It replaces [PersistentState] on the parameter, so it has to say everything that "
            + "attribute said.");
    }

    [Test]
    public void The_attribute_applies_to_the_state_parameter_and_nowhere_else()
    {
        var usage = typeof(IndexedAttribute).GetCustomAttribute<AttributeUsageAttribute>()!;

        Assert.That(usage.ValidOn, Is.EqualTo(AttributeTargets.Parameter),
            "Marking the state rather than the class is what gives the package a precise hook - the "
            + "grain's own write - instead of an approximation.");
    }

    [Test]
    public void The_state_and_storage_names_are_carried_verbatim()
    {
        var attribute = new IndexedAttribute("user", "Store");

        Assert.Multiple(() =>
        {
            Assert.That(attribute.StateName, Is.EqualTo("user"));
            Assert.That(attribute.StorageName, Is.EqualTo("Store"));
        });
    }

    [Test]
    public void An_unnamed_attribute_leaves_both_names_to_be_defaulted()
    {
        var attribute = new IndexedAttribute();

        Assert.Multiple(() =>
        {
            Assert.That(attribute.StateName, Is.Null,
                "An absent state name is filled in from the parameter's own name when the facet is "
                + "bound, exactly as [PersistentState] does.");
            Assert.That(attribute.StorageName, Is.Null);
        });
    }

    [Test]
    public void A_storage_provider_can_be_chosen_without_naming_the_state()
    {
        Assert.That(new IndexedAttribute(storageName: "Store").StorageName, Is.EqualTo("Store"));
    }
}
