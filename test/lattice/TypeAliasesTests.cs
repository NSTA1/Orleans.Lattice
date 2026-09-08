using System.Reflection;
using Orleans.Lattice;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

public class TypeAliasesTests
{
    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        foreach (var (name, value) in GetAliasConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(6),
                $"TypeAliases.{name} = \"{value}\" exceeds 6-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_ol_prefix()
    {
        foreach (var (name, value) in GetAliasConstants())
        {
            // Accept both the canonical "ol." prefix and the legacy
            // "olr." prefix introduced by the replication package
            // before the WAL adapter move pulled WAL types into core.
            // Existing alias string values are part of the wire format
            // and cannot be changed without a coordinated upgrade.
            var hasOlPrefix = value.StartsWith("ol.", StringComparison.Ordinal)
                || value.StartsWith("olr.", StringComparison.Ordinal);
            Assert.That(hasOlPrefix, Is.True,
                $"TypeAliases.{name} = \"{value}\" does not start with \"ol.\" or \"olr.\"");
        }
    }

    [Test]
    public void All_aliases_are_unique()
    {
        var duplicates = GetAliasConstants().Values
            .GroupBy(v => v, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(duplicates, Is.Empty,
            $"Duplicate aliases found: {string.Join(", ", duplicates)}");
    }

    /// <summary>
    /// Regression: every <c>TypeAliases</c> constant must be referenced by at
    /// least one <c>[Alias(...)]</c> attribute somewhere in the assembly,
    /// and every <c>[Alias(...)]</c> attribute value must be equal to one
    /// of the constants. Fails on dead entries (removed type, stale alias)
    /// and on orphan entries (new type using a hard-coded string not
    /// registered in the table).
    /// </summary>
    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var constantValues = GetAliasConstants();

        // Collect every [Alias(...)] value used across both the production
        // and test assemblies (test DTOs may legally carry aliases).
        var prodAssembly = typeof(TypeAliases).Assembly;
        var aliasUsages = prodAssembly.GetTypes()
            .SelectMany(t => t.GetCustomAttributes<AliasAttribute>(false)
                .Select(a => (Type: t, Alias: a.Alias)))
            .ToList();

        var usageByAlias = aliasUsages
            .GroupBy(x => x.Alias, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(x => x.Type.FullName ?? x.Type.Name).ToList(), StringComparer.Ordinal);

        // (1) No orphan usages - every used alias must be declared in TypeAliases.
        var declared = new HashSet<string>(constantValues.Values, StringComparer.Ordinal);
        var orphans = usageByAlias.Keys
            .Where(a => !declared.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in TypeAliases: {string.Join(", ", orphans)}");

        // (2) No dead constants - every declared alias must be used by at
        // least one type.
        var dead = constantValues
            .Where(kvp => !usageByAlias.ContainsKey(kvp.Value))
            .Select(kvp => $"{kvp.Key}=\"{kvp.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"TypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        // (3) No duplicate usages - each alias must map to exactly one type.
        var duplicates = usageByAlias
            .Where(kvp => kvp.Value.Count > 1)
            .Select(kvp => $"\"{kvp.Key}\" used by {string.Join(", ", kvp.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    /// <summary>
    /// The alias table read by reflection, keyed by constant name.
    /// <para>
    /// The non-emptiness guard is load-bearing rather than defensive: every
    /// caller either iterates this table or asserts that a set derived from it
    /// is empty, and both shapes are vacuously satisfied by an empty table. The
    /// reflection query is narrow enough to return nothing for a reason that has
    /// nothing to do with the aliases being correct - a constant demoted from
    /// <c>const</c> to <c>static readonly</c> clears <c>IsLiteral</c>, and the
    /// whole wire-format guard would go green while asserting nothing.
    /// </para>
    /// </summary>
    private static Dictionary<string, string> GetAliasConstants()
    {
        var constants = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .ToDictionary(f => f.Name, f => (string)f.GetValue(null)!, StringComparer.Ordinal);

        Assert.That(constants, Is.Not.Empty,
            "Expected at least one string alias constant on TypeAliases; an empty table would "
            + "satisfy every alias assertion in this fixture without testing anything.");

        return constants;
    }

    /// <summary>
    /// Every grain interface declared in the production assembly must carry an
    /// <c>[Alias(TypeAliases.X)]</c> attribute so the Orleans manifest has a
    /// stable, short wire-format identity independent of CLR type names.
    /// </summary>
    [Test]
    public void All_grain_interfaces_have_alias_attribute()
    {
        var grainInterfaces = GetGrainInterfaces();

        Assert.That(grainInterfaces, Is.Not.Empty,
            "Expected at least one grain interface in the production assembly.");

        var missing = grainInterfaces
            .Where(t => t.GetCustomAttribute<AliasAttribute>(inherit: false) is null)
            .Select(t => t.FullName ?? t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.That(missing, Is.Empty,
            $"Grain interfaces missing [Alias(TypeAliases.X)] attribute: {string.Join(", ", missing)}");
    }

    /// <summary>
    /// Every grain interface's <c>[Alias(...)]</c> value must reference a
    /// constant declared on <see cref="TypeAliases"/> (no hard-coded strings).
    /// </summary>
    [Test]
    public void All_grain_interface_aliases_reference_type_aliases_constants()
    {
        var declared = new HashSet<string>(GetAliasConstants().Values, StringComparer.Ordinal);

        var offenders = GetGrainInterfaces()
            .Select(t => (Type: t, Attr: t.GetCustomAttribute<AliasAttribute>(inherit: false)))
            .Where(x => x.Attr is not null && !declared.Contains(x.Attr!.Alias))
            .Select(x => $"{x.Type.FullName}=\"{x.Attr!.Alias}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        Assert.That(offenders, Is.Empty,
            $"Grain interfaces using hard-coded alias values (not declared in TypeAliases): {string.Join(", ", offenders)}");
    }

    private static List<Type> GetGrainInterfaces()
    {
        var addressable = typeof(IAddressable);
        return typeof(TypeAliases).Assembly
            .GetTypes()
            .Where(t => t.IsInterface
                && addressable.IsAssignableFrom(t)
                && t != addressable
                && t != typeof(IGrain)
                && t != typeof(IGrainWithStringKey)
                && t != typeof(IGrainWithGuidKey)
                && t != typeof(IGrainWithIntegerKey)
                && t != typeof(IGrainWithGuidCompoundKey)
                && t != typeof(IGrainWithIntegerCompoundKey)
                && t != typeof(IGrainObserver))
            .ToList();
    }
}