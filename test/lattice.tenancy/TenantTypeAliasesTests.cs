using System.Reflection;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Mirrors the core <c>TypeAliasesTests</c> and the membership
/// <c>MembershipTypeAliasesTests</c>: every constant on
/// <see cref="TenantTypeAliases"/> must be short, prefixed, unique, and used by
/// exactly one <c>[Alias(...)]</c> attribute on a type in the tenancy assembly.
/// Catches dead aliases and orphaned hard-coded alias strings.
/// </summary>
public class TenantTypeAliasesTests
{
    private const string AliasPrefix = "olt.";
    private const int MaxAliasLength = 7;

    [Test]
    public void All_aliases_are_at_most_seven_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"TenantTypeAliases.{name} = \"{value}\" exceeds {MaxAliasLength}-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_olt_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"TenantTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
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
    public void Tenancy_aliases_do_not_collide_with_core_or_membership_aliases()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.Not.StartWith("ol.").And.Not.EqualTo("ol"),
                $"TenantTypeAliases.{name} = \"{value}\" collides with the core 'ol.' namespace");
            Assert.That(value, Does.Not.StartWith("olm."),
                $"TenantTypeAliases.{name} = \"{value}\" collides with the membership 'olm.' namespace");
        }
    }

    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants()
            .ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        var prodAssembly = typeof(TenantTypeAliases).Assembly;
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
            $"[Alias(...)] values not declared in TenantTypeAliases: {string.Join(", ", orphans)}");

        var dead = declared
            .Where(kv => !packageAliasUsages.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"TenantTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        var duplicates = packageAliasUsages
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
    {
        return typeof(TenantTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
    }
}
