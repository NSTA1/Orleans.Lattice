using System.Reflection;
using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// Alias hygiene for the gRPC wire-type records this binding adds. They reuse the
/// parent package's <see cref="ApiAuthTypeAliases"/> registry (the <c>oli.</c>
/// prefix, capped at six characters) rather than a new table, mirroring how the
/// State / Data gRPC bindings keep a single per-family alias namespace. This test
/// asserts each grpc wire type carries the exact stable alias the registry
/// declares, that every <c>[Alias(...)]</c> in the grpc assembly is compact and
/// prefixed, and that no two grpc wire types collide. The parent package's own
/// <c>ApiAuthTypeAliasesTests</c> (extended to scan this assembly) covers
/// dead / orphan detection across both assemblies.
/// </summary>
[TestFixture]
public sealed class ApiAuthGrpcTypeAliasesTests
{
    private const string AliasPrefix = "oli.";
    private const int MaxAliasLength = 6;

    private static readonly (Type Type, string ExpectedAlias)[] WireTypes =
    [
        (typeof(AuthUserRef), ApiAuthTypeAliases.AuthUserRef),
        (typeof(AuthGroupRef), ApiAuthTypeAliases.AuthGroupRef),
        (typeof(AuthMemberRef), ApiAuthTypeAliases.AuthMemberRef),
        (typeof(AuthRuleRef), ApiAuthTypeAliases.AuthRuleRef),
        (typeof(AuthMemberEdge), ApiAuthTypeAliases.AuthMemberEdge),
        (typeof(AuthPutRule), ApiAuthTypeAliases.AuthPutRule),
        (typeof(AuthTreeRulesPage), ApiAuthTypeAliases.AuthTreeRulesPage),
        (typeof(AuthExplainQuery), ApiAuthTypeAliases.AuthExplainQuery),
        (typeof(AuthSubjectRef), ApiAuthTypeAliases.AuthSubjectRef),
        (typeof(AuthAck), ApiAuthTypeAliases.AuthAck),
        (typeof(AuthUserResult), ApiAuthTypeAliases.AuthUserResult),
        (typeof(AuthGroupResult), ApiAuthTypeAliases.AuthGroupResult),
        (typeof(AuthRuleResult), ApiAuthTypeAliases.AuthRuleResult),
        (typeof(AuthStringList), ApiAuthTypeAliases.AuthStringList),
        (typeof(AuthRuleRemoved), ApiAuthTypeAliases.AuthRuleRemoved),
        (typeof(AuthPrincipalRef), ApiAuthTypeAliases.AuthPrincipalRef),
        (typeof(AuthDirectoryPrincipalResult), ApiAuthTypeAliases.AuthDirectoryPrincipalResult),
        (typeof(AuthAccessModelQuery), ApiAuthTypeAliases.AuthAccessModelQuery),
    ];

    [Test]
    public void Every_grpc_wire_type_carries_its_registry_alias()
    {
        foreach (var (type, expectedAlias) in WireTypes)
        {
            var attribute = type.GetCustomAttribute<AliasAttribute>(inherit: false);
            Assert.That(attribute, Is.Not.Null, $"{type.Name} is missing an [Alias] attribute");
            Assert.That(attribute!.Alias, Is.EqualTo(expectedAlias),
                $"{type.Name} must carry the registry alias \"{expectedAlias}\"");
        }
    }

    [Test]
    public void Every_grpc_wire_type_is_orleans_serializable()
    {
        foreach (var (type, _) in WireTypes)
        {
            Assert.That(
                type.GetCustomAttributes().Any(a => a.GetType().Name == "GenerateSerializerAttribute"),
                Is.True,
                $"{type.Name} must be [GenerateSerializer]");
        }
    }

    [Test]
    public void All_grpc_aliases_are_prefixed_and_compact()
    {
        foreach (var alias in GrpcAssemblyAliases())
        {
            Assert.Multiple(() =>
            {
                Assert.That(alias, Does.StartWith(AliasPrefix), $"\"{alias}\" is not prefixed with \"{AliasPrefix}\"");
                Assert.That(alias.Length, Is.LessThanOrEqualTo(MaxAliasLength),
                    $"\"{alias}\" is longer than {MaxAliasLength} characters");
            });
        }
    }

    [Test]
    public void All_grpc_aliases_are_unique()
    {
        var duplicates = GrpcAssemblyAliases()
            .GroupBy(a => a, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.That(duplicates, Is.Empty, $"Duplicate grpc aliases: {string.Join(", ", duplicates)}");
    }

    private static IEnumerable<string> GrpcAssemblyAliases()
        => typeof(LatticeAuthApiGrpcOptions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetCustomAttributes<AliasAttribute>(inherit: false))
            .Select(a => a.Alias)
            .Where(a => a.StartsWith(AliasPrefix, StringComparison.Ordinal));
}
