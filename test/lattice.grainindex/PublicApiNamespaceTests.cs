namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Guards the single-namespace contract for the grain-index package: every
/// public type in <c>Orleans.Lattice.GrainIndex</c> must live in that exact
/// root namespace so the whole public surface sits behind a single
/// <c>using Orleans.Lattice.GrainIndex;</c>.
/// </summary>
[TestFixture]
public sealed class PublicApiNamespaceTests
{
    private const string Root = "Orleans.Lattice.GrainIndex";

    [Test]
    public void All_public_types_live_in_the_root_namespace()
    {
        var assembly = typeof(TypeAliases).Assembly;

        var strays = assembly.GetExportedTypes()
            .Where(t => t.Namespace is null
                || !t.Namespace.StartsWith("OrleansCodeGen", StringComparison.Ordinal))
            .Where(t => t.Namespace != Root)
            .Select(t => t.FullName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(strays, Is.Empty,
            $"Every public type in {assembly.GetName().Name} must live in the root '{Root}' "
            + "namespace so the whole public surface sits behind a single 'using "
            + $"{Root};'. Move these types (or make them internal): " + string.Join(", ", strays));
    }

    [Test]
    public void Assembly_identity_matches_the_package_id()
    {
        var assembly = typeof(TypeAliases).Assembly;

        Assert.That(assembly.GetName().Name, Is.EqualTo(Root),
            "The assembly name must match the PackageId 'Orleans.Lattice.GrainIndex' so the "
            + "shipped .nupkg, the assembly, and the root namespace all agree.");
    }
}
