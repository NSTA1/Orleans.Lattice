using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire messages (the <c>Model</c> records the binding
/// marshals with the Orleans serializer) and the transport-agnostic facade DTOs
/// the RPCs carry, proving the transport contract is coherent across the wire.
/// Also asserts alias hygiene: every gRPC wire message carries a unique
/// <c>[Alias]</c> drawn from the <see cref="GrpcTelemetryTypeAliases"/> registry
/// under the reserved <c>oitlg.</c> prefix, which is wire format and can never be
/// renamed or reused.
/// </summary>
[TestFixture]
public sealed class TelemetryGrpcDtoSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() => _services = TelemetryGrpcTestSupport.Serializers();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void TelemetryCatalogRequest_round_trips()
        => Assert.That(RoundTrip(new TelemetryCatalogRequest()), Is.EqualTo(new TelemetryCatalogRequest()));

    [Test]
    public void AuthSchemeAdvertisementRequest_round_trips()
        => Assert.That(RoundTrip(new AuthSchemeAdvertisementRequest()), Is.Not.Null);

    [Test]
    public void AuthSchemeDescriptor_round_trips_with_parameters()
    {
        var copy = RoundTrip(new AuthSchemeDescriptor
        {
            SchemeId = "entra",
            DisplayName = "Microsoft Entra ID",
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["authority"] = "https://login.microsoftonline.com/contoso",
                ["audience"] = "api://lattice",
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.SchemeId, Is.EqualTo("entra"));
            Assert.That(copy.DisplayName, Is.EqualTo("Microsoft Entra ID"));
            Assert.That(copy.Parameters["authority"], Is.EqualTo("https://login.microsoftonline.com/contoso"));
            Assert.That(copy.Parameters["audience"], Is.EqualTo("api://lattice"));
        });
    }

    [Test]
    public void AuthSchemeDescriptor_defaults_to_an_empty_parameter_set()
        => Assert.That(new AuthSchemeDescriptor { SchemeId = "basic" }.Parameters, Is.Empty);

    [Test]
    public void AuthSchemeAdvertisement_round_trips_its_schemes_in_order()
    {
        var copy = RoundTrip(new AuthSchemeAdvertisement
        {
            Schemes =
            [
                new AuthSchemeDescriptor { SchemeId = "entra" },
                new AuthSchemeDescriptor { SchemeId = "basic" },
            ],
        });

        Assert.That(copy.Schemes.Select(s => s.SchemeId), Is.EqualTo(new[] { "entra", "basic" }));
    }

    [Test]
    public void AuthSchemeAdvertisement_defaults_to_advertising_nothing()
        => Assert.That(new AuthSchemeAdvertisement().Schemes, Is.Empty);

    [Test]
    public void The_contract_query_request_round_trips_over_this_binding()
    {
        var copy = RoundTrip(new TelemetryQueryRequest
        {
            QueryId = "lattice.ops.rate",
            Range = TelemetryTimeRange.At(DateTimeOffset.UnixEpoch),
            RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.QueryId, Is.EqualTo("lattice.ops.rate"));
            Assert.That(copy.Range.IsInstant, Is.True);
            Assert.That(copy.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(copy.RequestedTenantId, Is.Null);
        });
    }

    [Test]
    public void The_contract_query_response_round_trips_its_scope_over_this_binding()
    {
        var copy = RoundTrip(new TelemetryQueryResponse
        {
            QueryId = "lattice.ops.rate",
            Scope = TelemetryTenantScope.AtRequestedTenant("beta"),
            ResultKind = TelemetryResultKind.Vector,
            Series = [],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(copy.Scope.TenantId, Is.EqualTo("beta"));
            Assert.That(copy.Scope.WasDowngraded, Is.False);
            Assert.That(copy.IsEmpty, Is.True);
        });
    }

    [Test]
    public void Every_registry_alias_is_unique_and_uses_the_reserved_prefix()
    {
        var aliases = RegistryAliasValues();

        Assert.Multiple(() =>
        {
            Assert.That(GrpcTelemetryTypeAliases.AliasPrefix, Is.EqualTo("oitlg."));
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Is.All.StartsWith(GrpcTelemetryTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void The_binding_prefix_does_not_collide_with_the_contract_prefix()
    {
        // oitl. is the contract's reserved namespace. The binding's oitlg. prefix
        // starts with it, so a contract alias must never be a prefix of a binding
        // alias in the other direction, and the two sets must stay disjoint.
        var bindingAliases = RegistryAliasValues();
        var contractAliases = typeof(ApiTelemetryTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(ApiTelemetryTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(bindingAliases.Intersect(contractAliases, StringComparer.Ordinal), Is.Empty);
            Assert.That(
                contractAliases.Where(a => a.StartsWith(GrpcTelemetryTypeAliases.AliasPrefix, StringComparison.Ordinal)),
                Is.Empty,
                "A contract alias inside the binding's reserved namespace would break the partition.");
        });
    }

    [Test]
    public void Every_grpc_wire_message_carries_a_unique_registry_alias()
    {
        var registry = new HashSet<string>(RegistryAliasValues(), StringComparer.Ordinal);

        var wireMessages = typeof(GrpcTelemetryTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        Assert.That(wireMessages, Is.Not.Empty);
        foreach (var type in wireMessages)
        {
            var alias = type.GetCustomAttribute<AliasAttribute>()?.Alias;
            Assert.Multiple(() =>
            {
                Assert.That(alias, Is.Not.Null, $"{type.Name} is serializable but carries no [Alias].");
                Assert.That(registry, Does.Contain(alias), $"{type.Name} alias '{alias}' is not in GrpcTelemetryTypeAliases.");
                Assert.That(seen.Add(alias!), Is.True, $"Alias '{alias}' is used by more than one wire message.");
            });
        }
    }

    [Test]
    public void Every_registry_alias_is_referenced_by_exactly_one_wire_message()
    {
        var declared = typeof(GrpcTelemetryTypeAliases).Assembly
            .GetTypes()
            .Select(t => t.GetCustomAttribute<AliasAttribute>()?.Alias)
            .Where(alias => alias is not null)
            .ToArray();

        var orphans = RegistryAliasValues()
            .Where(alias => declared.Count(d => string.Equals(d, alias, StringComparison.Ordinal)) != 1)
            .OrderBy(alias => alias, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            orphans,
            Is.Empty,
            "A registry constant with no type behind it is dead wire format nobody can remove later. "
            + "Orphans: " + string.Join(", ", orphans));
    }

    [Test]
    public void Every_wire_message_numbers_its_members_sequentially_from_zero()
    {
        foreach (var type in typeof(GrpcTelemetryTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null))
        {
            var ids = type
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Select(p => p.GetCustomAttribute<IdAttribute>()?.Id)
                .Where(id => id is not null)
                .Select(id => (int)id!)
                .OrderBy(id => id)
                .ToArray();

            Assert.That(
                ids,
                Is.EqualTo(Enumerable.Range(0, ids.Length).ToArray()),
                $"{type.Name} must number its serialized members sequentially from zero.");
        }
    }

    [Test]
    public void Every_wire_message_is_marked_immutable()
    {
        var offenders = typeof(GrpcTelemetryTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Where(t => t.GetCustomAttribute<ImmutableAttribute>() is null)
            .Select(t => t.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.That(
            offenders,
            Is.Empty,
            "Every wire message here is constructed once and never mutated. Offenders: "
            + string.Join(", ", offenders));
    }

    [Test]
    public void The_binding_declares_no_serializable_exception()
    {
        // The binding raises RpcException (not serializable) and re-throws the
        // contract's own exceptions. A [GenerateSerializer] exception introduced
        // here would need to derive directly from Exception or ship a no-op
        // [RegisterCopier], so flag one the moment it appears.
        var offenders = typeof(GrpcTelemetryTypeAliases).Assembly
            .GetTypes()
            .Where(t => typeof(Exception).IsAssignableFrom(t))
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Select(t => t.FullName)
            .ToArray();

        Assert.That(offenders, Is.Empty, "Offenders: " + string.Join(", ", offenders));
    }

    private static IReadOnlyList<string> RegistryAliasValues() =>
        [.. typeof(GrpcTelemetryTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(GrpcTelemetryTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)];
}
