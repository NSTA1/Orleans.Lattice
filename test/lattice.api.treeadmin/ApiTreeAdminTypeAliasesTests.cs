using System.Reflection;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Mirrors the sibling <c>ApiReplicationTypeAliasesTests</c>: every alias constant
/// on <see cref="ApiTreeAdminTypeAliases"/> must be short, prefixed with the
/// reserved <c>oit.</c> namespace, unique, and used by exactly one
/// <c>[Alias(...)]</c> attribute on a type in the abstractions assembly. Catches
/// dead aliases (a removed type kept its constant) and orphans (a new type using a
/// hard-coded alias string instead of referencing the table). The
/// <see cref="ApiTreeAdminTypeAliases.AliasPrefix"/> constant is the
/// reserved-namespace marker rather than a type alias, so it is excluded from the
/// usage check.
/// </summary>
public sealed class ApiTreeAdminTypeAliasesTests
{
    private const string AliasPrefix = "oit.";
    private const int MaxAliasLength = 6;

    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"ApiTreeAdminTypeAliases.{name} = \"{value}\" exceeds {MaxAliasLength}-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_the_reserved_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"ApiTreeAdminTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
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
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants()
            .ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        var prodAssembly = typeof(ApiTreeAdminTypeAliases).Assembly;
        var usageByAlias = prodAssembly.GetTypes()
            .SelectMany(t => t.GetCustomAttributes<AliasAttribute>(inherit: false)
                .Select(a => (Type: t, a.Alias)))
            .Where(x => x.Alias.StartsWith(AliasPrefix, StringComparison.Ordinal))
            .GroupBy(x => x.Alias, StringComparer.Ordinal)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => x.Type.FullName ?? x.Type.Name).ToList(),
                StringComparer.Ordinal);

        var declaredValues = new HashSet<string>(declared.Values, StringComparer.Ordinal);
        var orphans = usageByAlias.Keys
            .Where(a => !declaredValues.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in ApiTreeAdminTypeAliases: {string.Join(", ", orphans)}");

        var dead = declared
            .Where(kv => !usageByAlias.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"ApiTreeAdminTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        var duplicates = usageByAlias
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
    {
        return typeof(ApiTreeAdminTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(ApiTreeAdminTypeAliases.AliasPrefix))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
    }
}
