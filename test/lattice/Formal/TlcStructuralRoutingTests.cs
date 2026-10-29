using NUnit.Framework.Internal;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>Structural coverage must survive policies that omit exhaustive JVM work.</summary>
[TestFixture]
public sealed class TlcStructuralRoutingTests
{
    [Test]
    public void Structural_case_sources_do_not_carry_TLC_policy_categories()
    {
        var structural = SpecModuleCases.Modules()
            .Concat(SpecModuleCases.Mutations())
            .Concat(SpecModuleCases.Variants());
        var incorrectlyTagged = structural.Where(data =>
            data.Properties.ContainsKey(PropertyNames.Category)
            && data.Properties[PropertyNames.Category].Cast<string>()
                .Any(category => category.StartsWith("TlcShard", StringComparison.Ordinal)
                    || category.StartsWith(TlcCiShard.ModuleCategoryPrefix, StringComparison.Ordinal)))
            .ToList();

        Assert.That(incorrectlyTagged, Is.Empty,
            "generic structural cases must remain in the deterministic rest shard when exhaustive TLC is skipped, and must not be selected by a per-module TLC filter");
    }

    [Test]
    public void Only_JVM_gates_consume_the_TLC_case_sources()
    {
        var gates = SpecModuleGates.All();
        Assert.That(gates, Is.Not.Empty);
        Assert.That(gates.Where(gate => gate.RunsTlc), Is.Not.Empty);
        foreach (var gate in gates)
        {
            Assert.That(gate.Source, Is.Not.Null, gate.Name);
            Assert.That(gate.Source!.SourceName!.StartsWith("Tlc", StringComparison.Ordinal),
                Is.EqualTo(gate.RunsTlc), gate.Name);
        }
    }

    [Test]
    public void Dedicated_TLC_sources_preserve_the_complete_structural_case_population()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Signatures(SpecModuleCases.TlcModules()), Is.EqualTo(Signatures(SpecModuleCases.Modules())));
            Assert.That(Signatures(SpecModuleCases.TlcMutations()), Is.EqualTo(Signatures(SpecModuleCases.Mutations())));
            Assert.That(Signatures(SpecModuleCases.TlcVariants()), Is.EqualTo(Signatures(SpecModuleCases.Variants())));
        });
    }

    private static string[] Signatures(IEnumerable<TestCaseData> cases) =>
        cases.Select(data => string.Join("|", data.Arguments.Select(argument => argument?.ToString())))
            .Order(StringComparer.Ordinal).ToArray();
}
