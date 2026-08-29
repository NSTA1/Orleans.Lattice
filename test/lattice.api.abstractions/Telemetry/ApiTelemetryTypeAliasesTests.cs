using System.Reflection;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Mirrors the sibling <c>ApiTenantAdminTypeAliasesTests</c>: every alias constant
/// on <see cref="ApiTelemetryTypeAliases"/> must be short, prefixed with the
/// reserved <c>oitl.</c> namespace, unique, and used by exactly one
/// <c>[Alias(...)]</c> attribute on a type in the abstractions assembly. Catches
/// dead aliases (a removed type kept its constant) and orphans (a new type using a
/// hard-coded alias string instead of referencing the table). The
/// <see cref="ApiTelemetryTypeAliases.AliasPrefix"/> constant is the
/// reserved-namespace marker rather than a type alias, so it is excluded from the
/// usage check.
/// </summary>
public sealed class ApiTelemetryTypeAliasesTests
{
    private const string AliasPrefix = "oitl.";
    private const int MaxAliasLength = 7;

    /// <summary>
    /// The other reserved alias prefixes in the abstractions assembly. The
    /// telemetry namespace must neither contain nor be contained by any of them,
    /// because each package's hygiene audit partitions the alias space by a
    /// <c>StartsWith</c> test: an overlapping prefix would make one group's
    /// aliases look like another group's orphans.
    /// </summary>
    private static readonly string[] SiblingPrefixes =
    [
        "ol.", "ola.", "olad.", "oli.", "olrg.", "olt.",
        "oib.", "oir.", "ois.", "oit.", "oitn.",
    ];

    [Test]
    public void All_aliases_are_at_most_seven_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"ApiTelemetryTypeAliases.{name} = \"{value}\" exceeds {MaxAliasLength}-char limit ({value.Length} chars)");
        }
    }

    [Test]
    public void All_aliases_start_with_the_reserved_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"ApiTelemetryTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
        }
    }

    [Test]
    public void The_reserved_prefix_is_the_expected_namespace()
    {
        Assert.That(ApiTelemetryTypeAliases.AliasPrefix, Is.EqualTo(AliasPrefix));
    }

    [Test]
    public void The_reserved_prefix_does_not_overlap_a_sibling_namespace()
    {
        foreach (var sibling in SiblingPrefixes)
        {
            Assert.Multiple(() =>
            {
                Assert.That(AliasPrefix.StartsWith(sibling, StringComparison.Ordinal), Is.False,
                    $"\"{AliasPrefix}\" is inside the reserved \"{sibling}\" namespace.");
                Assert.That(sibling.StartsWith(AliasPrefix, StringComparison.Ordinal), Is.False,
                    $"\"{sibling}\" is inside the reserved \"{AliasPrefix}\" namespace.");
            });
        }
    }

    [Test]
    public void No_telemetry_alias_is_matched_by_a_sibling_prefix_audit()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            foreach (var sibling in SiblingPrefixes)
            {
                Assert.That(value.StartsWith(sibling, StringComparison.Ordinal), Is.False,
                    $"ApiTelemetryTypeAliases.{name} = \"{value}\" would be swept up by the \"{sibling}\" audit.");
            }
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
    public void The_table_declares_at_least_one_alias()
    {
        Assert.That(EnumerateConstants(), Is.Not.Empty,
            "ApiTelemetryTypeAliases declares no aliases, so every other check here would pass vacuously.");
    }

    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants()
            .ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        var prodAssembly = typeof(ApiTelemetryTypeAliases).Assembly;
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
            $"[Alias(...)] values not declared in ApiTelemetryTypeAliases: {string.Join(", ", orphans)}");

        var dead = declared
            .Where(kv => !usageByAlias.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"ApiTelemetryTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        var duplicates = usageByAlias
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty,
            $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
    {
        return typeof(ApiTelemetryTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(ApiTelemetryTypeAliases.AliasPrefix))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
    }
}
