using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Lattice.Schema;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Schema.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire messages (the <c>Model</c> request / response
/// records the binding marshals with the Orleans serializer) to prove the
/// transport contract is coherent across the wire, and asserts alias hygiene:
/// every gRPC wire message carries a unique <c>[Alias]</c> drawn from the
/// <see cref="GrpcSchemaTypeAliases"/> registry under the reserved <c>oisg.</c>
/// prefix. The transport-agnostic facade DTOs are covered in the
/// <c>Orleans.Lattice.Api.Schema</c> test project; this fixture covers the
/// gRPC-only envelopes.
/// </summary>
[TestFixture]
public sealed class SchemaGrpcDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    [Test]
    public void SchemaTreeRequest_round_trips()
    {
        Assert.That(RoundTrip(new SchemaTreeRequest { TreeId = "orders" }).TreeId, Is.EqualTo("orders"));
    }

    [Test]
    public void SetPolicyRequest_round_trips_tree_and_policy()
    {
        var copy = RoundTrip(new SetPolicyRequest { TreeId = "orders", Policy = JsonPolicy() });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Policy.Rules, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void SetVersionConfigRequest_round_trips()
    {
        var copy = RoundTrip(new SetVersionConfigRequest
        {
            TreeId = "orders",
            Config = new LatticeSchemaVersionConfig(3, 4, strictIngest: true),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Config.SchemaId, Is.EqualTo(3u));
            Assert.That(copy.Config.TargetVersion, Is.EqualTo(4u));
            Assert.That(copy.Config.StrictIngest, Is.True);
        });
    }

    [Test]
    public void AdvanceVersionRequest_round_trips()
    {
        var copy = RoundTrip(new AdvanceVersionRequest { TreeId = "orders", NewTargetVersion = 9 });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.NewTargetVersion, Is.EqualTo(9u));
        });
    }

    [Test]
    public void RemediateRequest_round_trips()
    {
        var copy = RoundTrip(new RemediateRequest
        {
            TreeId = "orders",
            Transform = LatticeValueTransform.Passthrough(),
            TargetPolicy = JsonPolicy(),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.TargetPolicy.Rules, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void SchemaAckResponse_round_trips() =>
        Assert.That(RoundTrip(new SchemaAckResponse()), Is.Not.Null);

    [Test]
    public void SchemaRemovedResponse_round_trips()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(new SchemaRemovedResponse { Removed = true }).Removed, Is.True);
            Assert.That(RoundTrip(new SchemaRemovedResponse { Removed = false }).Removed, Is.False);
        });
    }

    [Test]
    public void GetPolicyResponse_round_trips_found_and_not_found()
    {
        var found = RoundTrip(new GetPolicyResponse { Found = true, Policy = JsonPolicy() });
        var missing = RoundTrip(new GetPolicyResponse { Found = false, Policy = null });

        Assert.Multiple(() =>
        {
            Assert.That(found.Found, Is.True);
            Assert.That(found.Policy, Is.Not.Null);
            Assert.That(missing.Found, Is.False);
            Assert.That(missing.Policy, Is.Null);
        });
    }

    [Test]
    public void SchemaCountResponse_round_trips() =>
        Assert.That(RoundTrip(new SchemaCountResponse { Count = 12 }).Count, Is.EqualTo(12));

    [Test]
    public void GetVersionConfigResponse_round_trips()
    {
        var copy = RoundTrip(new GetVersionConfigResponse
        {
            Found = true,
            Config = new LatticeSchemaVersionConfig(1, 2),
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Found, Is.True);
            Assert.That(copy.Config.TargetVersion, Is.EqualTo(2u));
        });
    }

    [Test]
    public void VersionConfigResponse_round_trips()
    {
        var copy = RoundTrip(new VersionConfigResponse { Config = new LatticeSchemaVersionConfig(1, 7) });

        Assert.That(copy.Config.TargetVersion, Is.EqualTo(7u));
    }

    [Test]
    public void SchemaRemediationReportResponse_round_trips()
    {
        var copy = RoundTrip(new SchemaRemediationReportResponse { Report = LatticeSchemaRemediationReport.Idle });

        Assert.That(copy.Report.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Idle));
    }

    [Test]
    public void SchemaComplianceReportResponse_round_trips()
    {
        var report = new LatticeSchemaComplianceReport
        {
            TreeId = "orders",
            HasPolicy = true,
            CompliantCount = 4,
            NonCompliantCount = 1,
            ScannedCount = 5,
            RuleBreakdown = new[] { new LatticeSchemaComplianceRuleCount { Reason = "bad", Count = 1 } },
        };

        var copy = RoundTrip(new SchemaComplianceReportResponse { Report = report });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Report.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Report.NonCompliantCount, Is.EqualTo(1));
            Assert.That(copy.Report.RuleBreakdown[0].Reason, Is.EqualTo("bad"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisementRequest_round_trips() =>
        Assert.That(RoundTrip(new AuthSchemeAdvertisementRequest()), Is.Not.Null);

    [Test]
    public void AuthSchemeDescriptor_round_trips_with_parameters()
    {
        var copy = RoundTrip(new AuthSchemeDescriptor
        {
            SchemeId = "entra",
            DisplayName = "Microsoft Entra",
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal) { ["authority"] = "https://login" },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.SchemeId, Is.EqualTo("entra"));
            Assert.That(copy.DisplayName, Is.EqualTo("Microsoft Entra"));
            Assert.That(copy.Parameters["authority"], Is.EqualTo("https://login"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisement_round_trips_its_schemes()
    {
        var copy = RoundTrip(new AuthSchemeAdvertisement
        {
            Schemes = new[] { new AuthSchemeDescriptor { SchemeId = "basic" } },
        });

        Assert.That(copy.Schemes, Has.Count.EqualTo(1));
        Assert.That(copy.Schemes[0].SchemeId, Is.EqualTo("basic"));
    }

    [Test]
    public void Every_registry_alias_is_unique_and_uses_the_reserved_prefix()
    {
        var aliases = RegistryAliasValues();

        Assert.Multiple(() =>
        {
            Assert.That(GrpcSchemaTypeAliases.AliasPrefix, Is.EqualTo("oisg."));
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Is.All.StartsWith(GrpcSchemaTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Every_grpc_wire_message_carries_a_unique_registry_alias()
    {
        var registry = new HashSet<string>(RegistryAliasValues(), StringComparer.Ordinal);

        var wireMessages = typeof(GrpcSchemaTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Where(t => t.GetCustomAttribute<AliasAttribute>()?.Alias
                is { } alias && alias.StartsWith(GrpcSchemaTypeAliases.AliasPrefix, StringComparison.Ordinal))
            .ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        Assert.That(wireMessages, Is.Not.Empty);
        foreach (var type in wireMessages)
        {
            var alias = type.GetCustomAttribute<AliasAttribute>()!.Alias;
            Assert.Multiple(() =>
            {
                Assert.That(registry, Does.Contain(alias), $"{type.Name} alias '{alias}' is not in GrpcSchemaTypeAliases.");
                Assert.That(seen.Add(alias), Is.True, $"Alias '{alias}' is used by more than one wire message.");
            });
        }
    }

    private static IReadOnlyList<string> RegistryAliasValues() =>
        typeof(GrpcSchemaTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(GrpcSchemaTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
}
