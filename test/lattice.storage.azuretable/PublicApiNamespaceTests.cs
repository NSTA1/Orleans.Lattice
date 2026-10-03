using System.Reflection;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Guards the single-namespace contract for the Azure Table WAL package: every public
/// type in <c>Orleans.Lattice.Storage.AzureTable</c> must live in that exact root namespace
/// so the whole public surface sits behind a single <c>using Orleans.Lattice.Storage.AzureTable;</c>.
/// </summary>
[TestFixture]
public class PublicApiNamespaceTests
{
    [Test]
    public void All_public_types_live_in_the_root_namespace()
    {
        const string root = "Orleans.Lattice.Storage.AzureTable";
        var assembly = typeof(AzureTableWalStorageOptions).Assembly;

        var exported = assembly.GetExportedTypes()
            .Where(t => t.Namespace is null
                || !t.Namespace.StartsWith("OrleansCodeGen", StringComparison.Ordinal))
            .ToArray();

        Assert.That(exported, Is.Not.Empty,
            $"Expected at least one hand-written public type in {assembly.GetName().Name}; an "
            + "assembly exporting nothing would satisfy the namespace assertion below without "
            + "testing anything.");

        var strays = exported
            .Where(t => t.Namespace != root)
            .Select(t => t.FullName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(strays, Is.Empty,
            $"Every public type in {assembly.GetName().Name} must live in the root '{root}' "
            + "namespace so the whole public surface sits behind a single 'using "
            + $"{root};'. Move these types (or make them internal): " + string.Join(", ", strays));
    }
}
