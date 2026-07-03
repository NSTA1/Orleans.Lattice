using System.Reflection;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Mirrors the core <c>TypeAliasesTests</c>: every constant on
/// <see cref="ApiAuthTypeAliases"/> must be prefixed, unique, at most six
/// characters after nothing (the whole token is capped at six characters to keep
/// the auth-API wire tags compact), and used by exactly one <c>[Alias(...)]</c>
/// attribute on a type in the auth-API assembly. Catches dead aliases (removed
/// type kept its constant) and orphans (a new type using a hard-coded alias
/// string instead of referencing the table).
/// </summary>
public class ApiAuthTypeAliasesTests
{
    private const string AliasPrefix = "oli.";
    private const int MaxAliasLength = 6;

    [Test]
    public void All_aliases_start_with_the_package_prefix()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value, Does.StartWith(AliasPrefix),
                $"ApiAuthTypeAliases.{name} = \"{value}\" does not start with \"{AliasPrefix}\"");
        }
    }

    [Test]
    public void All_aliases_are_at_most_six_characters()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(value.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                $"ApiAuthTypeAliases.{name} = \"{value}\" is longer than {MaxAliasLength} characters");
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
    public void Auth_api_aliases_do_not_collide_with_core_or_sibling_prefixes()
    {
        foreach (var (name, value) in EnumerateConstants())
        {
            Assert.That(
                value,
                Does.Not.StartWith("ol.")
                    .And.Not.StartWith("ola.")
                    .And.Not.StartWith("olad.")
                    .And.Not.StartWith("olz.")
                    .And.Not.EqualTo("ol"),
                $"ApiAuthTypeAliases.{name} = \"{value}\" collides with a reserved prefix");
        }
    }

    [Test]
    public void Every_alias_constant_is_referenced_by_exactly_one_type()
    {
        var declared = EnumerateConstants().ToDictionary(c => c.Name, c => c.Value, StringComparer.Ordinal);

        // The alias registry is shared across the auth-API package and its gRPC
        // transport binding: the grpc wire-type records (request / response
        // envelopes) live in the sibling Orleans.Lattice.Api.Auth.Grpc assembly but
        // draw their stable aliases from this same table. Scan both assemblies so a
        // grpc wire-type's alias is not mis-reported as a dead constant.
        var assemblies = new[]
        {
            typeof(ApiAuthTypeAliases).Assembly,
            typeof(Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiGrpcOptions).Assembly,
        };
        var usageByAlias = assemblies
            .SelectMany(asm => asm.GetTypes())
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
            $"[Alias(...)] values not declared in ApiAuthTypeAliases: {string.Join(", ", orphans)}");

        var dead = declared
            .Where(kv => !packageAliasUsages.ContainsKey(kv.Value))
            .Select(kv => $"{kv.Key}=\"{kv.Value}\"")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.That(dead, Is.Empty,
            $"ApiAuthTypeAliases constants unreferenced by any [Alias(...)] attribute: {string.Join(", ", dead)}");

        var duplicates = packageAliasUsages
            .Where(kv => kv.Value.Count > 1)
            .Select(kv => $"\"{kv.Key}\" used by {string.Join(", ", kv.Value)}")
            .ToList();
        Assert.That(duplicates, Is.Empty, $"Aliases used by multiple types: {string.Join("; ", duplicates)}");
    }

    private static IEnumerable<(string Name, string Value)> EnumerateConstants()
        => typeof(ApiAuthTypeAliases)
            .GetFields(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetValue(null)!));
}
