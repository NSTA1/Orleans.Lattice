using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire messages (the <c>Model</c> request / response
/// records the binding marshals with the Orleans serializer) to prove the
/// transport contract is coherent across the wire, and asserts alias hygiene:
/// every gRPC wire message carries a unique <c>[Alias]</c> drawn from the
/// <see cref="GrpcReplicationTypeAliases"/> registry under the reserved
/// <c>oirg.</c> prefix. The transport-agnostic facade DTOs are covered in the
/// <c>Orleans.Lattice.Api.Replication</c> test project; this fixture covers the
/// gRPC-only envelopes.
/// </summary>
[TestFixture]
public sealed class ReplicationGrpcDtoSerializationTests
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

    [Test]
    public void ReplicationEnableRequestMessage_round_trips()
    {
        var original = new ReplicationEnableRequestMessage
        {
            TreeId = "orders",
            Mode = LatticeMergeMode.RwFlag,
            BootstrapSourceClusterId = "cluster-b",
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(copy.BootstrapSourceClusterId, Is.EqualTo("cluster-b"));
        });
    }

    [Test]
    public void ReplicationEnableRequestMessage_round_trips_without_bootstrap()
    {
        var copy = RoundTrip(new ReplicationEnableRequestMessage
        {
            TreeId = "orders",
            Mode = LatticeMergeMode.LwwRegister,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(copy.BootstrapSourceClusterId, Is.Null);
        });
    }

    [Test]
    public void ReplicationEnableResponse_round_trips()
    {
        var original = new ReplicationEnableResponse
        {
            TreeId = "orders",
            Mode = LatticeMergeMode.RwFlag,
            AlreadyEnabled = true,
            BootstrapRequested = true,
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(copy.AlreadyEnabled, Is.True);
            Assert.That(copy.BootstrapRequested, Is.True);
        });
    }

    [Test]
    public void ReplicationDisableRequestMessage_round_trips() =>
        Assert.That(RoundTrip(new ReplicationDisableRequestMessage { TreeId = "orders" }).TreeId, Is.EqualTo("orders"));

    [Test]
    public void ReplicationDisableResponse_round_trips()
    {
        var copy = RoundTrip(new ReplicationDisableResponse { TreeId = "orders", AlreadyDisabled = true });
        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("orders"));
            Assert.That(copy.AlreadyDisabled, Is.True);
        });
    }

    [Test]
    public void ReplicationGetConfigRequest_round_trips() =>
        Assert.That(RoundTrip(new ReplicationGetConfigRequest()), Is.Not.Null);

    [Test]
    public void ReplicationConfigResponse_round_trips_entries()
    {
        var original = new ReplicationConfigResponse
        {
            Trees = new[]
            {
                new ReplicationTreeConfigMessage
                {
                    TreeId = "orders",
                    Enabled = true,
                    HasMode = true,
                    Mode = LatticeMergeMode.RwFlag,
                    Ambiguous = false,
                },
                new ReplicationTreeConfigMessage
                {
                    TreeId = "customers",
                    Enabled = false,
                    HasMode = false,
                    Ambiguous = true,
                },
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Trees.Select(t => t.TreeId), Is.EqualTo(new[] { "orders", "customers" }));
            Assert.That(copy.Trees[0].Enabled, Is.True);
            Assert.That(copy.Trees[0].HasMode, Is.True);
            Assert.That(copy.Trees[0].Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
            Assert.That(copy.Trees[1].HasMode, Is.False);
            Assert.That(copy.Trees[1].Ambiguous, Is.True);
        });
    }

    [Test]
    public void ReplicationConfigResponse_round_trips_empty() =>
        Assert.That(RoundTrip(new ReplicationConfigResponse()).Trees, Is.Empty);

    [Test]
    public void ReplicationTreeConfigMessage_round_trips_every_enrollment_source()
    {
        var original = new ReplicationConfigResponse
        {
            Trees = new[]
            {
                new ReplicationTreeConfigMessage
                {
                    TreeId = "runtime",
                    Enabled = true,
                    HasMode = true,
                    Mode = LatticeMergeMode.OrSet,
                    Source = ReplicationEnrollmentSource.Runtime,
                },
                new ReplicationTreeConfigMessage
                {
                    TreeId = "declared",
                    Enabled = true,
                    HasMode = true,
                    Mode = LatticeMergeMode.LwwRegister,
                    Source = ReplicationEnrollmentSource.Static,
                },
                new ReplicationTreeConfigMessage
                {
                    TreeId = "both",
                    Enabled = true,
                    HasMode = true,
                    Mode = LatticeMergeMode.OrMap,
                    Source = ReplicationEnrollmentSource.RuntimeAndStatic,
                },
            },
        };

        var copy = RoundTrip(original);
        Assert.That(
            copy.Trees.Select(t => t.Source),
            Is.EqualTo(new[]
            {
                ReplicationEnrollmentSource.Runtime,
                ReplicationEnrollmentSource.Static,
                ReplicationEnrollmentSource.RuntimeAndStatic,
            }));
    }

    [Test]
    public void ReplicationTreeConfigMessage_defaults_its_source_to_runtime() =>
        Assert.That(
            RoundTrip(new ReplicationConfigResponse
            {
                Trees = new[] { new ReplicationTreeConfigMessage { TreeId = "orders" } },
            }).Trees[0].Source,
            Is.EqualTo(ReplicationEnrollmentSource.Runtime));

    [Test]
    public void AuthSchemeAdvertisementRequest_round_trips() =>
        Assert.That(RoundTrip(new AuthSchemeAdvertisementRequest()), Is.Not.Null);

    [Test]
    public void AuthSchemeAdvertisement_round_trips_descriptors()
    {
        var original = new AuthSchemeAdvertisement
        {
            Schemes = new[]
            {
                new AuthSchemeDescriptor
                {
                    SchemeId = "entra",
                    DisplayName = "Microsoft Entra",
                    Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["authority"] = "https://login.microsoftonline.com/tenant",
                        ["clientId"] = "abc",
                    },
                },
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Schemes, Has.Count.EqualTo(1));
            Assert.That(copy.Schemes[0].SchemeId, Is.EqualTo("entra"));
            Assert.That(copy.Schemes[0].DisplayName, Is.EqualTo("Microsoft Entra"));
            Assert.That(copy.Schemes[0].Parameters["authority"], Is.EqualTo("https://login.microsoftonline.com/tenant"));
            Assert.That(copy.Schemes[0].Parameters["clientId"], Is.EqualTo("abc"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisement_round_trips_empty() =>
        Assert.That(RoundTrip(new AuthSchemeAdvertisement()).Schemes, Is.Empty);

    [Test]
    public void Every_registry_alias_is_unique_and_uses_the_reserved_prefix()
    {
        var aliases = RegistryAliasValues();

        Assert.Multiple(() =>
        {
            Assert.That(GrpcReplicationTypeAliases.AliasPrefix, Is.EqualTo("oirg."));
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Is.All.StartsWith(GrpcReplicationTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Every_grpc_wire_message_carries_a_unique_registry_alias()
    {
        var registry = new HashSet<string>(RegistryAliasValues(), StringComparer.Ordinal);

        var wireMessages = typeof(GrpcReplicationTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Where(t => t.GetCustomAttribute<AliasAttribute>()?.Alias
                is { } alias && alias.StartsWith(GrpcReplicationTypeAliases.AliasPrefix, StringComparison.Ordinal))
            .ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        Assert.That(wireMessages, Is.Not.Empty);
        foreach (var type in wireMessages)
        {
            var alias = type.GetCustomAttribute<AliasAttribute>()!.Alias;
            Assert.Multiple(() =>
            {
                Assert.That(registry, Does.Contain(alias), $"{type.Name} alias '{alias}' is not in GrpcReplicationTypeAliases.");
                Assert.That(seen.Add(alias), Is.True, $"Alias '{alias}' is used by more than one wire message.");
            });
        }
    }

    private static IReadOnlyList<string> RegistryAliasValues() =>
        typeof(GrpcReplicationTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(GrpcReplicationTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
}
