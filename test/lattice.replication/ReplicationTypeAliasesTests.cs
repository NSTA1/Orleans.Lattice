using System.Reflection;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Mirrors the core <c>TypeAliasesTests</c>: every constant on
/// <see cref="ReplicationTypeAliases"/> must be short, prefixed,
/// unique, and used by exactly one <c>[Alias(...)]</c> attribute on a
/// type in the replication assembly. Catches dead aliases (removed type
/// kept its constant) and orphans (a new type using a hard-coded alias
/// string instead of referencing the table).
/// </summary>
public class ReplicationTypeAliasesTests
{
    private const string AliasPrefix = "olr.";
    private const int MaxAliasLength = 6;

    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"ReplicationTypeAliases.{name} = \"{value}\" exceeds {MaxAliasLength}-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_olr_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"ReplicationTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
        }
    }

    [Test]
    public void All_aliases_are_unique()
    {
        var values = EnumerateConstants().Select(c => c.Value).ToList();
        var duplicates = values
            .GroupBy(v => v, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(duplicates, Is.Empty,
            $"Duplicate aliases found: {string.Join(", ", duplicates)}");
    }

    [Test]
    public void Replication_aliases_do_not_collide_with_core_lattice_aliases()
    {
        // The core library uses the "ol." prefix; the replication package uses
        // "olr.". The check below is belt-and-braces in case a constant slips
        // in with the wrong prefix.
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.Not.StartWith("ol.").And.Not.EqualTo("ol"),
                $"ReplicationTypeAliases.{name} = \"{value}\" collides with the core 'ol.' namespace");
        }
    }

    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants()
            .ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        var prodAssembly = typeof(ReplicationTypeAliases).Assembly;
        var aliasUsages = prodAssembly.GetTypes()
            .SelectMany(t => t.GetCustomAttributes<AliasAttribute>(inherit: false)
                .Select(a => (Type: t, Alias: a.Alias)))
            .ToList();

        var usageByAlias = aliasUsages
            .GroupBy(x => x.Alias, StringComparer.Ordinal)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => x.Type.FullName ?? x.Type.Name).ToList(),
                StringComparer.Ordinal);

        // Filter to aliases owned by this package - the assembly may also
        // re-host core types whose aliases live in the core TypeAliases table.
        var packageAliasUsages = usageByAlias
            .Where(kv => kv.Key.StartsWith(AliasPrefix, StringComparison.Ordinal))
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

        // (1) No orphan usages - every "olr."-prefixed alias used in the
        //     assembly must be declared on ReplicationTypeAliases.
        var declaredValues = new HashSet<string>(declared.Values, StringComparer.Ordinal);
        var orphans = packageAliasUsages.Keys
            .Where(a => !declaredValues.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in ReplicationTypeAliases: {string.Join(", ", orphans)}");

        // (2) No dead constants - every declared alias must be used.
        var dead = declared
            .Where(kv => !packageAliasUsages.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"ReplicationTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        // (3) No duplicate usages - each alias maps to exactly one type.
        var duplicates = packageAliasUsages
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
    {
        return typeof(ReplicationTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
    }
}
