using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Round-trips the gRPC-layer wire messages (the <c>Model</c> request records the
/// binding marshals with the Orleans serializer) and the transport-agnostic facade
/// result DTOs the RPCs return, proving the transport contract is coherent across
/// the wire. Also asserts alias hygiene: every gRPC wire message carries a unique
/// <c>[Alias]</c> drawn from the <see cref="GrpcTenantAdminTypeAliases"/> registry
/// under the reserved <c>oitng.</c> prefix.
/// </summary>
[TestFixture]
public sealed class TenantAdminGrpcDtoSerializationTests
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
    public void TenantAdminTenantRequest_round_trips()
    {
        Assert.That(RoundTrip(new TenantAdminTenantRequest { TenantId = "acme" }).TenantId, Is.EqualTo("acme"));
    }

    [Test]
    public void TenantAdminCreateRequest_round_trips_its_admin_subjects()
    {
        var copy = RoundTrip(new TenantAdminCreateRequest
        {
            TenantId = "acme",
            AdminSubjects = ["ops@example.com", "sre@example.com"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.AdminSubjects, Is.EqualTo(new[] { "ops@example.com", "sre@example.com" }));
        });
    }

    [Test]
    public void TenantAdminCreateRequest_defaults_to_an_empty_subject_set() =>
        Assert.That(new TenantAdminCreateRequest { TenantId = "acme" }.AdminSubjects, Is.Empty);

    [Test]
    public void AuthSchemeAdvertisementRequest_round_trips() =>
        Assert.That(RoundTrip(new AuthSchemeAdvertisementRequest()), Is.Not.Null);

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
                ["clientId"] = "abc123",
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.SchemeId, Is.EqualTo("entra"));
            Assert.That(copy.DisplayName, Is.EqualTo("Microsoft Entra ID"));
            Assert.That(copy.Parameters["authority"], Is.EqualTo("https://login.microsoftonline.com/contoso"));
            Assert.That(copy.Parameters["clientId"], Is.EqualTo("abc123"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisement_round_trips_its_schemes()
    {
        var copy = RoundTrip(new AuthSchemeAdvertisement
        {
            Schemes = new[]
            {
                new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Basic" },
                new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" },
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Schemes, Has.Count.EqualTo(2));
            Assert.That(copy.Schemes[0].SchemeId, Is.EqualTo("basic"));
            Assert.That(copy.Schemes[1].SchemeId, Is.EqualTo("entra"));
        });
    }

    [Test]
    public void TenantCreationResult_response_round_trips()
    {
        var copy = RoundTrip(new TenantCreationResult
        {
            TenantId = "acme",
            Status = TenantLifecycleStatus.Active,
            AdminSubjects = ["ops@example.com"],
        });
        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Status, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(copy.AdminSubjects, Is.EqualTo(new[] { "ops@example.com" }));
        });
    }

    [Test]
    public void TenantStatusChangeResult_response_round_trips()
    {
        var copy = RoundTrip(new TenantStatusChangeResult
        {
            TenantId = "acme",
            PreviousStatus = TenantLifecycleStatus.Active,
            NewStatus = TenantLifecycleStatus.Suspended,
            Changed = true,
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.PreviousStatus, Is.EqualTo(TenantLifecycleStatus.Active));
            Assert.That(copy.NewStatus, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(copy.Changed, Is.True);
        });
    }

    [Test]
    public void TenantDeletionResult_response_round_trips()
    {
        var copy = RoundTrip(new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 4 });
        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.CascadedTreeCount, Is.EqualTo(4));
        });
    }

    [Test]
    public void TenantSelfCurrentRequest_round_trips() =>
        Assert.That(RoundTrip(new TenantSelfCurrentRequest()), Is.Not.Null);

    [Test]
    public void TenantSelfListRequest_round_trips() =>
        Assert.That(RoundTrip(new TenantSelfListRequest()), Is.Not.Null);

    [Test]
    public void TenantSelfDescriptorList_round_trips_its_descriptors()
    {
        var copy = RoundTrip(new TenantSelfDescriptorList
        {
            Tenants = new[]
            {
                new TenantDescriptor { TenantId = "acme", Status = TenantLifecycleStatus.Active, IsDefault = false },
                new TenantDescriptor { TenantId = "default", Status = TenantLifecycleStatus.Active, IsDefault = true },
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Tenants, Has.Count.EqualTo(2));
            Assert.That(copy.Tenants[0].TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Tenants[1].IsDefault, Is.True);
        });
    }

    [Test]
    public void TenantSelfDescriptorList_defaults_to_an_empty_list() =>
        Assert.That(new TenantSelfDescriptorList().Tenants, Is.Empty);

    [Test]
    public void TenantStatusReport_response_round_trips()
    {
        var copy = RoundTrip(new TenantStatusReport
        {
            TenantId = "acme",
            Status = TenantLifecycleStatus.Suspended,
            IsDefault = false,
            Regions = Array.Empty<TenantRegionStatusDescriptor>(),
            Quotas = new TenantQuotasDescriptor { MaxBytes = 1_000, BurstPercent = 10 },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Status, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(copy.Regions, Is.Empty);
            Assert.That(copy.Quotas.MaxBytes, Is.EqualTo(1_000));
            Assert.That(copy.Quotas.BurstPercent, Is.EqualTo(10));
        });
    }

    [Test]
    public void TenantAdminSetQuotasRequest_round_trips()
    {
        var copy = RoundTrip(new TenantAdminSetQuotasRequest
        {
            TenantId = "acme",
            Quotas = new TenantQuotasDescriptor
            {
                MaxBytes = 1_000_000,
                MaxKeys = 5_000,
                MaxMemoryBytes = 2_000_000,
                MaxTreeCount = 10,
                MaxOpsPerSecond = 250,
                BurstPercent = 20,
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Quotas.MaxBytes, Is.EqualTo(1_000_000));
            Assert.That(copy.Quotas.MaxKeys, Is.EqualTo(5_000));
            Assert.That(copy.Quotas.MaxMemoryBytes, Is.EqualTo(2_000_000));
            Assert.That(copy.Quotas.MaxTreeCount, Is.EqualTo(10));
            Assert.That(copy.Quotas.MaxOpsPerSecond, Is.EqualTo(250));
            Assert.That(copy.Quotas.BurstPercent, Is.EqualTo(20));
            Assert.That(copy.Quotas.IsUnbounded, Is.False);
        });
    }

    [Test]
    public void TenantQuotasUpdateResult_response_round_trips()
    {
        var copy = RoundTrip(new TenantQuotasUpdateResult
        {
            TenantId = "acme",
            Quotas = new TenantQuotasDescriptor { MaxOpsPerSecond = 42 },
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Quotas.MaxOpsPerSecond, Is.EqualTo(42));
            Assert.That(copy.Quotas.MaxBytes, Is.Null);
        });
    }

    [Test]
    public void TenantQuotasDescriptor_unbounded_round_trips()
    {
        var copy = RoundTrip(TenantQuotasDescriptor.Unbounded);

        Assert.That(copy.IsUnbounded, Is.True);
    }

    [Test]
    public void TenantAdminRegionSetRequest_round_trips_its_region_list()
    {
        var copy = RoundTrip(new TenantAdminRegionSetRequest
        {
            TenantId = "acme",
            Regions = ["eu-west", "ap-south"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Regions, Is.EqualTo(new[] { "eu-west", "ap-south" }));
        });
    }

    [Test]
    public void TenantAdminRegionSetRequest_round_trips_an_empty_region_list()
    {
        // The empty set is the meaningful "revoke everything" / "drain out of every
        // region" request, so it must survive the wire as an empty list, not null.
        var copy = RoundTrip(new TenantAdminRegionSetRequest { TenantId = "acme" });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Regions, Is.Not.Null);
            Assert.That(copy.Regions, Is.Empty);
        });
    }

    [Test]
    public void TenantRegionAuthorizationResult_round_trips()
    {
        var copy = RoundTrip(new TenantRegionAuthorizationResult
        {
            TenantId = "acme",
            AllowedRegions = ["eu-west"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.AllowedRegions, Is.EqualTo(new[] { "eu-west" }));
        });
    }

    [Test]
    public void TenantResidencyChangeResult_round_trips_its_deltas_and_rows()
    {
        var copy = RoundTrip(new TenantResidencyChangeResult
        {
            TenantId = "acme",
            AddedRegions = ["ap-south"],
            RemovedRegions = ["eu-west"],
            Regions =
            [
                new TenantRegionStatusDescriptor
                {
                    RegionId = "ap-south",
                    Status = TenantRegionLifecycleStatus.Provisioning,
                    IsAllowed = true,
                },
            ],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.AddedRegions, Is.EqualTo(new[] { "ap-south" }));
            Assert.That(copy.RemovedRegions, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(copy.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
            Assert.That(copy.Regions[0].IsAllowed, Is.True);
        });
    }

    [Test]
    public void TenantRegionStatusReport_round_trips()
    {
        var copy = RoundTrip(new TenantRegionStatusReport
        {
            TenantId = "acme",
            Regions =
            [
                new TenantRegionStatusDescriptor
                {
                    RegionId = "eu-west",
                    Status = TenantRegionLifecycleStatus.Draining,
                    IsAllowed = false,
                },
            ],
        });

        Assert.Multiple(() =>
        {
            Assert.That(copy.TenantId, Is.EqualTo("acme"));
            Assert.That(copy.Regions[0].RegionId, Is.EqualTo("eu-west"));
            Assert.That(copy.Regions[0].Status, Is.EqualTo(TenantRegionLifecycleStatus.Draining));
            Assert.That(copy.Regions[0].IsAllowed, Is.False);
        });
    }

    [Test]
    public void Every_registry_alias_is_unique_and_uses_the_reserved_prefix()
    {
        var aliases = RegistryAliasValues();

        Assert.Multiple(() =>
        {
            Assert.That(GrpcTenantAdminTypeAliases.AliasPrefix, Is.EqualTo("oitng."));
            Assert.That(aliases, Is.Unique);
            Assert.That(aliases, Is.All.StartsWith(GrpcTenantAdminTypeAliases.AliasPrefix));
        });
    }

    [Test]
    public void Every_grpc_wire_message_carries_a_unique_registry_alias()
    {
        var registry = new HashSet<string>(RegistryAliasValues(), StringComparer.Ordinal);

        var wireMessages = typeof(GrpcTenantAdminTypeAliases).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<GenerateSerializerAttribute>() is not null)
            .Where(t => t.GetCustomAttribute<AliasAttribute>()?.Alias
                is { } alias && alias.StartsWith(GrpcTenantAdminTypeAliases.AliasPrefix, StringComparison.Ordinal))
            .ToList();

        var seen = new HashSet<string>(StringComparer.Ordinal);
        Assert.That(wireMessages, Is.Not.Empty);
        foreach (var type in wireMessages)
        {
            var alias = type.GetCustomAttribute<AliasAttribute>()!.Alias;
            Assert.Multiple(() =>
            {
                Assert.That(registry, Does.Contain(alias), $"{type.Name} alias '{alias}' is not in GrpcTenantAdminTypeAliases.");
                Assert.That(seen.Add(alias), Is.True, $"Alias '{alias}' is used by more than one wire message.");
            });
        }
    }

    private static IReadOnlyList<string> RegistryAliasValues() =>
        typeof(GrpcTenantAdminTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Where(f => f.Name != nameof(GrpcTenantAdminTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
}
