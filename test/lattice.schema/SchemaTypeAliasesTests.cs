using System.Reflection;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Mirrors the core <c>TypeAliasesTests</c> and the sibling
/// <c>AuthTypeAliasesTests</c>: every constant on <see cref="SchemaTypeAliases"/>
/// must be short, prefixed, unique, and used by exactly one <c>[Alias(...)]</c>
/// attribute on a type in the schema assembly. Catches dead aliases (removed
/// type kept its constant) and orphans (a new type using a hard-coded alias
/// string instead of referencing the table).
/// </summary>
public class SchemaTypeAliasesTests
{
    private const string AliasPrefix = "ols.";
    private const int MaxAliasLength = 6;

    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"SchemaTypeAliases.{name} = \"{value}\" exceeds {MaxAliasLength}-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_ols_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"SchemaTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
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
    public void Schema_aliases_do_not_collide_with_core_lattice_aliases()
    {
        // The core library uses the "ol." prefix; the schema package uses "ols.".
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.Not.EqualTo("ol").And.Not.StartWith("ol."),
                $"SchemaTypeAliases.{name} = \"{value}\" collides with the core 'ol.' namespace");
        }
    }

    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants()
            .ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        var prodAssembly = typeof(SchemaTypeAliases).Assembly;
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

        var packageAliasUsages = usageByAlias
            .Where(kv => kv.Key.StartsWith(AliasPrefix, StringComparison.Ordinal))
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

        var declaredValues = new HashSet<string>(declared.Values, StringComparer.Ordinal);
        var orphans = packageAliasUsages.Keys
            .Where(a => !declaredValues.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in SchemaTypeAliases: {string.Join(", ", orphans)}");

        var dead = declared
            .Where(kv => !packageAliasUsages.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"SchemaTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        var duplicates = packageAliasUsages
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
    {
        return typeof(SchemaTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
    }
}
