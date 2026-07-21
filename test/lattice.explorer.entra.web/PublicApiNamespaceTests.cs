namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Guards the single-namespace contract for the hosted-web Entra provider: every
/// public type in <c>Orleans.Lattice.Explorer.Entra.Web</c> must live in that
/// exact root namespace so the whole public surface sits behind a single
/// <c>using Orleans.Lattice.Explorer.Entra.Web;</c>.
/// </summary>
[TestFixture]
public sealed class PublicApiNamespaceTests
{
    [Test]
    public void All_public_types_live_in_the_root_namespace()
    {
        const string root = "Orleans.Lattice.Explorer.Entra.Web";
        var assembly = typeof(ExplorerEntraWebOptions).Assembly;

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
