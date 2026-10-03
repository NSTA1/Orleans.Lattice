using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexRegistryTrees"/>: the single internal tree that
/// holds every index's bookkeeping, and the guarantee that it is not addressable
/// as a user tree.
/// </summary>
[TestFixture]
public sealed class GrainIndexRegistryTreesTests
{
    [Test]
    public void The_registry_tree_sits_inside_the_reserved_grain_index_namespace()
    {
        Assert.That(
            GrainIndexTreeNames.IsIndexOwned(GrainIndexRegistryTrees.RegistryTree),
            Is.True,
            "A replication resolver that screens out the reserved namespace must screen out the "
            + "registry along with the per-index trees.");
    }

    [Test]
    public void The_registry_tree_is_the_reserved_prefix_plus_the_registry_segment()
    {
        Assert.That(
            GrainIndexRegistryTrees.RegistryTree,
            Is.EqualTo(GrainIndexTreeNames.ReservedPrefix + GrainIndexRegistryTrees.RegistrySegment));
    }

    [Test]
    public void The_registry_tree_cannot_be_produced_by_naming_an_index_normally()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                GrainIndexTreeNames.ForIndex("users"),
                Is.Not.EqualTo(GrainIndexRegistryTrees.RegistryTree));
            Assert.That(
                GrainIndexRegistryTrees.RegistrySegment, Does.StartWith("."),
                "Leading with a dot keeps the bookkeeping tree out of the ordinary index-name "
                + "space, so only an index literally named '.registry' could collide - which the "
                + "reconciler rejects.");
        });
    }

    [Test]
    public void The_registry_type_is_internal_so_no_public_surface_leaks_the_tree()
    {
        Assert.That(
            typeof(GrainIndexRegistryTrees).IsPublic, Is.False,
            "The registry is an implementation detail: nothing public may name it, or a host "
            + "could address it as one of its own trees.");
    }

    [Test]
    public void No_public_type_in_the_assembly_exposes_the_registry_tree_name()
    {
        var leaks = PublicStringConstants()
            .Where(field => string.Equals(
                (string?)field.GetRawConstantValue(),
                GrainIndexRegistryTrees.RegistryTree,
                StringComparison.Ordinal))
            .Select(field => $"{field.DeclaringType?.FullName}.{field.Name}")
            .ToArray();

        Assert.That(leaks, Is.Empty,
            "The registry tree must not be resolvable or observable as a user tree. Offending "
            + "public constants: " + string.Join(", ", leaks));
    }

    [Test]
    public void The_public_constant_sweep_reaches_the_reserved_prefix()
    {
        // Battery test: the leak gate above is an absence assertion, so a sweep
        // that saw no public constant at all would report the same clean result as
        // one that saw every constant and found no leak. BindingFlags drift or a
        // narrowing of the exported-type set would do exactly that silently.
        var constants = PublicStringConstants()
            .Select(field => (string?)field.GetRawConstantValue())
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(constants, Is.Not.Empty);
            Assert.That(
                constants,
                Does.Contain(GrainIndexTreeNames.ReservedPrefix),
                "the reserved prefix is a public constant of this assembly, so the sweep that "
                + "looks for a leaked registry name must be able to see it.");
        });
    }

    private static System.Reflection.FieldInfo[] PublicStringConstants() =>
        [.. typeof(GrainIndexTreeNames).Assembly
            .GetExportedTypes()
            .SelectMany(type => type.GetFields(
                System.Reflection.BindingFlags.Public
                | System.Reflection.BindingFlags.Static
                | System.Reflection.BindingFlags.FlattenHierarchy))
            .Where(field => field.IsLiteral && field.FieldType == typeof(string))];
}
