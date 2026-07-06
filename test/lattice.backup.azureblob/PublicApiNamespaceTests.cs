using System.Reflection;

namespace Orleans.Lattice.Backup.AzureBlob.Tests;

/// <summary>
/// Guards the single-namespace contract for the Azure Blob backup package: every
/// public type in <c>Orleans.Lattice.Backup.AzureBlob</c> must live in that exact
/// root namespace so the whole public surface sits behind a single
/// <c>using Orleans.Lattice.Backup.AzureBlob;</c>.
/// </summary>
[TestFixture]
public class PublicApiNamespaceTests
{
    [Test]
    public void All_public_types_live_in_the_root_namespace()
    {
        const string root = "Orleans.Lattice.Backup.AzureBlob";
        var assembly = typeof(LatticeBackupAzureBlobOptions).Assembly;

        var strays = assembly.GetExportedTypes()
            .Where(t => t.Namespace is null
                || !t.Namespace.StartsWith("OrleansCodeGen", StringComparison.Ordinal))
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
