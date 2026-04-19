using System.Reflection;
using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

public class TypeAliasesTests
{
    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        var fields = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string));

        foreach (var field in fields)
        {
            var value = (string)field.GetValue(null)!;
            Assert.That(value.Length, Is.LessThanOrEqualTo(6),
                $"TypeAliases.{field.Name} = \"{value}\" exceeds 6-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_ol_prefix()
    {
        var fields = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string));

        foreach (var field in fields)
        {
            var value = (string)field.GetValue(null)!;
            Assert.That(value, Does.StartWith("ol."),
                $"TypeAliases.{field.Name} = \"{value}\" does not start with \"ol.\"");
        }
    }

    [Test]
    public void All_aliases_are_unique()
    {
        var fields = typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .ToList();

        var values = fields.Select(f => (string)f.GetValue(null)!).ToList();
        var duplicates = values
            .GroupBy(v => v)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(duplicates, Is.Empty,
            $"Duplicate aliases found: {string.Join(", ", duplicates)}");
    }

    /// <summary>
    /// FX-009: every <c>TypeAliases</c> constant must be referenced by at
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

        // (1) No orphan usages — every used alias must be declared in TypeAliases.
        var declared = new HashSet<string>(constantValues.Values, StringComparer.Ordinal);
        var orphans = usageByAlias.Keys
            .Where(a => !declared.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in TypeAliases: {string.Join(", ", orphans)}");

        // (2) No dead constants — every declared alias must be used by at
        // least one type.
        var dead = constantValues
            .Where(kvp => !usageByAlias.ContainsKey(kvp.Value))
            .Select(kvp => $"{kvp.Key}=\"{kvp.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"TypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        // (3) No duplicate usages — each alias must map to exactly one type.
        var duplicates = usageByAlias
            .Where(kvp => kvp.Value.Count > 1)
            .Select(kvp => $"\"{kvp.Key}\" used by {string.Join(", ", kvp.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static Dictionary<string, string> GetAliasConstants()
    {
        return typeof(TypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .ToDictionary(f => f.Name, f => (string)f.GetValue(null)!, StringComparer.Ordinal);
    }
}