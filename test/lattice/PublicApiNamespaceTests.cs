using System.Reflection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Guards the namespace convention that the public CRDT primitives live in the
/// root <c>Orleans.Lattice</c> namespace (so the whole public surface sits behind
/// a single <c>using Orleans.Lattice;</c>), and that the operationally-internal
/// transaction-registry DTOs are not part of the public surface.
/// </summary>
[TestFixture]
public class PublicApiNamespaceTests
{
    private static readonly Type[] PublicCrdtPrimitives =
    [
        typeof(HybridLogicalClock),
        typeof(ICrdt<>),
        typeof(MvRegister),
        typeof(MvRegisterEntry),
        typeof(OrMap<,>),
        typeof(OrMapEntry<>),
        typeof(OrSet),
        typeof(OrSetDot),
        typeof(PnCounter),
        typeof(Rga),
        typeof(RgaNode),
        typeof(VersionVector),
    ];

    [Test]
    public void Public_crdt_primitives_are_in_the_root_namespace()
    {
        Assert.Multiple(() =>
        {
            foreach (var type in PublicCrdtPrimitives)
            {
                Assert.That(type.Namespace, Is.EqualTo("Orleans.Lattice"),
                    $"{type.Name} must live in the root Orleans.Lattice namespace.");
                Assert.That(type.IsPublic, Is.True, $"{type.Name} must be public.");
            }
        });
    }

    [Test]
    public void All_public_types_live_in_the_root_namespace()
    {
        const string root = "Orleans.Lattice";
        var assembly = typeof(OrSet).Assembly;

        var strays = assembly.GetExportedTypes()
            .Where(t => t.Namespace is null
                || !t.Namespace.StartsWith("OrleansCodeGen", StringComparison.Ordinal))
            .Where(t => t.Namespace != root)
            .Select(t => t.FullName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(strays, Is.Empty,
            $"Every public type in {assembly.GetName().Name} must live in the root '{root}' "
            + "namespace so the whole public surface sits behind a single 'using Orleans.Lattice;'. "
            + "Move these types (or make them internal): " + string.Join(", ", strays));
    }

    [Test]
    public void Public_crdt_primitives_are_no_longer_in_the_Primitives_namespace()
    {
        var assembly = typeof(OrSet).Assembly;
        var stragglers = assembly.GetExportedTypes()
            .Where(t => t.Namespace == "Orleans.Lattice.Primitives")
            .Select(t => t.FullName)
            .ToArray();

        Assert.That(stragglers, Is.Empty,
            "Orleans.Lattice.Primitives must expose no public types; "
            + "public primitives belong in the root Orleans.Lattice namespace.");
    }

    [Test]
    public void Transaction_registry_dtos_are_internal()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(TxStatus).IsVisible, Is.False,
                "TxStatus is an operationally-internal grain-boundary DTO and must not be public.");
            Assert.That(typeof(TerminalTallyResult).IsVisible, Is.False,
                "TerminalTallyResult is an operationally-internal grain-boundary DTO and must not be public.");
        });
    }
}
