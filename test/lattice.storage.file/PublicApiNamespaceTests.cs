using System.Reflection;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Guards the single-namespace contract for the file WAL package: every public
/// type in <c>Orleans.Lattice.Storage.File</c> must live in that exact root namespace
/// so the whole public surface sits behind a single <c>using Orleans.Lattice.Storage.File;</c>.
/// </summary>
[TestFixture]
public class PublicApiNamespaceTests
{
    [Test]
    public void All_public_types_live_in_the_root_namespace()
    {
        const string root = "Orleans.Lattice.Storage.File";
        var assembly = typeof(FileWalStorageOptions).Assembly;

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
