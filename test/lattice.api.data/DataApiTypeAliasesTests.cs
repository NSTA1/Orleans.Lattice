using System.Reflection;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Mirrors the core <c>TypeAliasesTests</c>: every constant on
/// <see cref="DataApiTypeAliases"/> must be prefixed, unique, and used by exactly
/// one <c>[Alias(...)]</c> attribute on a type in the data-API assembly. Catches
/// dead aliases (removed type kept its constant) and orphans (a new type using a
/// hard-coded alias string instead of referencing the table). The 6-char limit
/// the core / auth tables enforce is intentionally not applied here: this package
/// follows the State-API convention of longer, human-legible tokens.
/// </summary>
public class DataApiTypeAliasesTests
{
    private const string AliasPrefix = "olad.";

    [Test]
    public void All_aliases_start_with_the_package_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"DataApiTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
        }
    }

    [Test]
    public void All_aliases_are_unique()
    {
        var duplicates = EnumerateConstants()
            .Select(c => c.Value)
            .GroupBy(v => v, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(duplicates, Is.Empty, $"Duplicate aliases found: {string.Join(", ", duplicates)}");
    }

    [Test]
    public void Data_aliases_do_not_collide_with_core_or_state_api_prefixes()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.Not.StartWith("ol.").And.Not.StartWith("ola.").And.Not.EqualTo("ol"),
                $"DataApiTypeAliases.{name} = \"{value}\" collides with a reserved prefix");
        }
    }

    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants().ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        var prodAssembly = typeof(DataApiTypeAliases).Assembly;
        var usageByAlias = prodAssembly.GetTypes()
            .SelectMany(t => t.GetCustomAttributes<AliasAttribute>(inherit: false).Select(a => (Type: t, a.Alias)))
            .GroupBy(x => x.Alias, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(x => x.Type.FullName ?? x.Type.Name).ToList(), StringComparer.Ordinal);

        var packageAliasUsages = usageByAlias
            .Where(kv => kv.Key.StartsWith(AliasPrefix, StringComparison.Ordinal))
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

        var declaredValues = new HashSet<string>(declared.Values, StringComparer.Ordinal);
        var orphans = packageAliasUsages.Keys
            .Where(a => !declaredValues.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in DataApiTypeAliases: {string.Join(", ", orphans)}");

        var dead = declared
            .Where(kv => !packageAliasUsages.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"DataApiTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        var duplicates = packageAliasUsages
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty, $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
        => typeof(DataApiTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
}
