using System.Reflection;
using Orleans;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Mirrors the core <c>TypeAliasesTests</c> for this package: every constant on
/// <see cref="ScalingTypeAliases"/> must be short, prefixed, unique, and used by
/// exactly one <c>[Alias(...)]</c> attribute on a type in the scaling assembly.
/// Catches dead aliases (removed type kept its constant) and orphans (a new type
/// using a hard-coded alias string instead of referencing the table).
/// <para>
/// The scaling aliases live in this package (not the core
/// <c>Orleans.Lattice.TypeAliases</c> table) because the core gate is scoped to
/// the core assembly; a constant declared there but referenced only from this
/// separate assembly would be flagged as dead. The replication package follows
/// the same pattern with <c>ReplicationTypeAliases</c>.
/// </para>
/// </summary>
[TestFixture]
public sealed class ScalingTypeAliasesTests
{
    private const string AliasPrefix = "ol.";
    private const int MaxAliasLength = 6;

    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"ScalingTypeAliases.{name} = \"{value}\" exceeds {MaxAliasLength}-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_ol_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"ScalingTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
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

        var prodAssembly = typeof(ScalingTypeAliases).Assembly;
        var usageByAlias = prodAssembly.GetTypes()
            .SelectMany(t => t.GetCustomAttributes<AliasAttribute>(inherit: false)
                .Select(a => (Type: t, a.Alias)))
            .GroupBy(x => x.Alias, StringComparer.Ordinal)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => x.Type.FullName ?? x.Type.Name).ToList(),
                StringComparer.Ordinal);

        var declaredValues = new HashSet<string>(declared.Values, StringComparer.Ordinal);

        // (1) No orphan usages - every alias used in the assembly must be declared.
        var orphans = usageByAlias.Keys
            .Where(a => !declaredValues.Contains(a))
            .OrderBy(a => a, StringComparer.Ordinal)
            .ToList();
        Assert.That(orphans, Is.Empty,
            $"[Alias(...)] values not declared in ScalingTypeAliases: {string.Join(", ", orphans)}");

        // (2) No dead constants - every declared alias must be used.
        var dead = declared
            .Where(kv => !usageByAlias.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"ScalingTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        // (3) No duplicate usages - each alias maps to exactly one type.
        var duplicates = usageByAlias
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
    {
        return typeof(ScalingTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
    }
}
