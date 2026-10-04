namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The test-case sources every per-module Formal gate draws from, each a
/// discovery over <c>spec/</c> expanded by a per-module function.
/// <para>
/// THE CONVENTION, WHICH A CONTROL ENFORCES. A gate is a test whose first
/// parameter is a <see cref="SpecModule"/>. Its <c>[TestCaseSource]</c> names a
/// source on this class, <c>X</c>, and this class also declares the expander
/// <c>XFor(SpecModule)</c> that produces the same cases for one module.
/// <see cref="SpecModuleDiscoveryControlTests"/> reads both by reflection: it
/// checks every gate draws from discovery, and it runs every gate's expander
/// over a synthetic module built in a temp directory, so the control proves
/// the gates check a module they have never seen rather than one somebody
/// remembered to list.
/// </para>
/// </summary>
public static class SpecModuleCases
{
    /// <summary>One case per discovered module.</summary>
    public static IEnumerable<TestCaseData> Modules() => Expand(ModulesFor);

    /// <summary>The single case for <paramref name="module"/>.</summary>
    public static IEnumerable<object[]> ModulesFor(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        yield return [module];
    }

    /// <summary>
    /// One case per mutation of every discovered module, each tagged with the
    /// CI shard category <see cref="TlcCiShard.Of"/> assigns it.
    /// </summary>
    public static IEnumerable<TestCaseData> Mutations() =>
        Expand(MutationsFor).Select(TlcCiShard.Tag);

    /// <summary>One case per mutation of <paramref name="module"/>.</summary>
    public static IEnumerable<object[]> MutationsFor(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        return module.LoadMutations().Select(mutation => new object[] { module, mutation });
    }

    /// <summary>One case per variant configuration of every discovered module.</summary>
    public static IEnumerable<TestCaseData> Variants() => Expand(VariantsFor);

    /// <summary>One case per variant configuration <paramref name="module"/>'s manifest declares.</summary>
    public static IEnumerable<object[]> VariantsFor(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        return module.Manifest.Variants.Keys
            .Order(StringComparer.Ordinal)
            .Select(variant => new object[] { module, variant });
    }

    /// <summary>
    /// One case per property the refinement note covers with a single-name
    /// row, mapped or excluded, of every discovered module: the rows a
    /// negative control can delete one at a time.
    /// </summary>
    public static IEnumerable<TestCaseData> CoveredProperties() => Expand(CoveredPropertiesFor);

    /// <summary>
    /// One case per single-name property row of <paramref name="module"/>'s
    /// mapping and exclusion tables.
    /// </summary>
    public static IEnumerable<object[]> CoveredPropertiesFor(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var tables = module.ReadRefinementTables();
        var sections = new[] { RefinementNote.PropertySection, RefinementPropertyCoverageTests.ExclusionSection };

        return sections
            .Where(tables.ContainsKey)
            .SelectMany(section => tables[section].Rows)
            .Select(row => row.Label)
            .Where(label => label.Length > 2 && label.Count(c => c == '`') == 2 && label[0] == '`' && label[^1] == '`')
            .Select(label => new object[] { module, label[1..^1] });
    }

    private static IEnumerable<TestCaseData> Expand(Func<SpecModule, IEnumerable<object[]>> expander) =>
        SpecModuleCatalogue.Repository()
            .SelectMany(module => expander(module))
            .Select(arguments => new TestCaseData(arguments)
                .SetArgDisplayNames(arguments.Select(a => a.ToString() ?? string.Empty).ToArray()));
}
