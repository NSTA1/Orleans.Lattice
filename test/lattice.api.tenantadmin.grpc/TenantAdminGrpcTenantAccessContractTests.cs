using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Pins the tenant-administration gRPC contract across the delegated tenant access
/// epic (#4154): every pre-epic RPC keeps its name, full method path, request and
/// response message; every pre-epic wire message keeps its alias and field layout
/// except the region-set request, whose new operator-override fields are append-only;
/// the new RPCs are appended, never interleaved; and a <c>SetTenantQuotas</c>
/// payload written by a pre-epic peer still reads, while the four delegated access
/// caps now travel inside it.
/// </summary>
[TestFixture]
public sealed class TenantAdminGrpcTenantAccessContractTests
{
    private const string Svc = "orleans.lattice.api.tenantadmin";

    // Every pre-epic RPC: property, wire name, request message, response message.
    private static readonly (string Property, string Name, Type Request, Type Response)[] PreEpicRpcs =
    [
        ("CreateTenant", "CreateTenant", typeof(TenantAdminCreateRequest), typeof(TenantCreationResult)),
        ("SuspendTenant", "SuspendTenant", typeof(TenantAdminTenantRequest), typeof(TenantStatusChangeResult)),
        ("ResumeTenant", "ResumeTenant", typeof(TenantAdminTenantRequest), typeof(TenantStatusChangeResult)),
        ("DeleteTenant", "DeleteTenant", typeof(TenantAdminTenantRequest), typeof(TenantDeletionResult)),
        ("SetTenantQuotas", "SetTenantQuotas", typeof(TenantAdminSetQuotasRequest), typeof(TenantQuotasUpdateResult)),
        ("GetAuthScheme", "GetAuthScheme", typeof(AuthSchemeAdvertisementRequest), typeof(AuthSchemeAdvertisement)),
        ("GetCurrentTenant", "GetCurrentTenant", typeof(TenantSelfCurrentRequest), typeof(TenantDescriptor)),
        ("ListAccessibleTenants", "ListAccessibleTenants", typeof(TenantSelfListRequest), typeof(TenantSelfDescriptorList)),
        ("GetTenant", "GetTenant", typeof(TenantAdminTenantRequest), typeof(TenantStatusReport)),
        ("AuthorizeAllowedRegions", "AuthorizeAllowedRegions", typeof(TenantAdminRegionSetRequest), typeof(TenantRegionAuthorizationResult)),
        ("SetTenantResidency", "SetTenantResidency", typeof(TenantAdminRegionSetRequest), typeof(TenantResidencyChangeResult)),
        ("GetTenantRegionStatus", "GetTenantRegionStatus", typeof(TenantAdminTenantRequest), typeof(TenantRegionStatusReport)),
        ("GetTenantQuotaUsage", "GetTenantQuotaUsage", typeof(TenantAdminTenantRequest), typeof(TenantQuotaUsageReport)),
        ("ListTenantAdminSubjects", "ListTenantAdminSubjects", typeof(TenantAdminTenantRequest), typeof(TenantAdminSubjectReport)),
        ("AddTenantAdminSubject", "AddTenantAdminSubject", typeof(TenantAdminSubjectRequest), typeof(TenantAdminSubjectChangeResult)),
        ("RemoveTenantAdminSubject", "RemoveTenantAdminSubject", typeof(TenantAdminSubjectRequest), typeof(TenantAdminSubjectChangeResult)),
        ("ListCrossTenantGrants", "ListCrossTenantGrants", typeof(TenantAdminTenantRequest), typeof(TenantGrantReport)),
        ("OfferCrossTenantGrant", "OfferCrossTenantGrant", typeof(TenantAdminGrantOfferRequest), typeof(TenantGrantChangeResult)),
        ("ApproveCrossTenantGrant", "ApproveCrossTenantGrant", typeof(TenantAdminGrantRequest), typeof(TenantGrantChangeResult)),
        ("RejectCrossTenantGrant", "RejectCrossTenantGrant", typeof(TenantAdminGrantRequest), typeof(TenantGrantChangeResult)),
        ("RevokeCrossTenantGrant", "RevokeCrossTenantGrant", typeof(TenantAdminGrantRequest), typeof(TenantGrantChangeResult)),
    ];

    private static readonly (string Name, Type Request, Type Response)[] TenantAccessRpcs =
    [
        ("ListTenantGroups", typeof(TenantAdminAccessListRequest), typeof(TenantGroupPage)),
        ("GetTenantGroup", typeof(TenantAdminGroupRequest), typeof(TenantAdminGroupLookup)),
        ("UpsertTenantGroup", typeof(TenantAdminGroupUpsertRequest), typeof(TenantGroupDescriptor)),
        ("RemoveTenantGroup", typeof(TenantAdminGroupRequest), typeof(TenantGroupRemovalResult)),
        ("ListTenantGroupMembers", typeof(TenantAdminGroupRequest), typeof(TenantAdminGroupMemberList)),
        ("AddTenantGroupMember", typeof(TenantAdminGroupMemberRequest), typeof(TenantMembershipChangeResult)),
        ("RemoveTenantGroupMember", typeof(TenantAdminGroupMemberRequest), typeof(TenantMembershipChangeResult)),
        ("ListTenantMembers", typeof(TenantAdminAccessListRequest), typeof(TenantMemberPage)),
        ("AddTenantMember", typeof(TenantAdminMemberRequest), typeof(TenantMembershipChangeResult)),
        ("RemoveTenantMember", typeof(TenantAdminMemberRequest), typeof(TenantMembershipChangeResult)),
        ("ResolveTenantSubject", typeof(TenantAdminMemberRequest), typeof(TenantSubjectResolution)),
        ("PutTenantRule", typeof(TenantAdminRulePutRequest), typeof(TenantRuleView)),
        ("GetTenantRule", typeof(TenantAdminRuleRequest), typeof(TenantAdminRuleLookup)),
        ("RemoveTenantRule", typeof(TenantAdminRuleRequest), typeof(TenantAdminRuleRemoval)),
        ("ListTenantRules", typeof(TenantAdminAccessListRequest), typeof(TenantRulePage)),
        ("ExplainTenantAccess", typeof(TenantAdminExplainRequest), typeof(TenantExplanation)),
        ("GetTenantEffectivePermissions", typeof(TenantAdminEffectivePermissionsRequest), typeof(TenantEffectivePermissions)),
        ("GetTenantAccessPosture", typeof(TenantAdminTenantRequest), typeof(TenantAccessPosture)),
    ];

    // Every unchanged pre-epic wire message: alias and "[Id] Name" field layout.
    private static readonly (Type Type, string Alias, string[] Fields)[] PreEpicMessages =
    [
        (typeof(TenantAdminTenantRequest), "oitng.tenreq", ["0 TenantId"]),
        (typeof(TenantAdminCreateRequest), "oitng.crtreq", ["0 TenantId", "1 AdminSubjects"]),
        (typeof(TenantAdminSetQuotasRequest), "oitng.setqreq", ["0 TenantId", "1 Quotas"]),
        (typeof(AuthSchemeAdvertisementRequest), "oitng.asreq", []),
        (typeof(AuthSchemeDescriptor), "oitng.asdesc", ["0 SchemeId", "1 DisplayName", "2 Parameters"]),
        (typeof(AuthSchemeAdvertisement), "oitng.asadv", ["0 Schemes"]),
        (typeof(TenantSelfCurrentRequest), "oitng.selfcur", []),
        (typeof(TenantSelfListRequest), "oitng.selflist", []),
        (typeof(TenantSelfDescriptorList), "oitng.selftdl", ["0 Tenants"]),
        (typeof(TenantAdminSubjectRequest), "oitng.subjreq", ["0 TenantId", "1 SubjectId"]),
        (typeof(TenantAdminGrantRequest), "oitng.grntreq", ["0 GranterTenantId", "1 GranteeTenantId", "2 Scope"]),
        (typeof(TenantAdminGrantOfferRequest), "oitng.grntoff", ["0 GranterTenantId", "1 GranteeTenantId", "2 Scope", "3 Operations"]),
    ];

    // A SetTenantQuotas request with every pre-epic quota member set, serialized by the
    // binding as it stood before the epic (bucket base 83b748e4a), when the descriptor
    // had no delegated access caps.
    private const string PreEpicSetQuotasPayload = "IOhACWFjbWUh6AAEJPQBQpxhgIQeAAEpAdIHAVHg4A==";

    private static readonly TenantQuotasDescriptor PreEpicQuotas = new()
    {
        MaxBytes = 1_000_000,
        MaxKeys = 5_000,
        MaxMemoryBytes = 2_000_000,
        MaxTreeCount = 10,
        MaxOpsPerSecond = 250,
        BurstPercent = 20,
    };

    private ServiceProvider _serializers = null!;

    [SetUp]
    public void SetUp() => _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private Dictionary<string, IMethod> Methods()
    {
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        return typeof(LatticeTenantAdminGrpcMethods)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => typeof(IMethod).IsAssignableFrom(p.PropertyType))
            .ToDictionary(p => p.Name, p => (IMethod)p.GetValue(methods)!);
    }

    private static (Type Request, Type Response) MessageTypes(IMethod method)
    {
        var args = method.GetType().GetGenericArguments();
        return (args[0], args[1]);
    }

    [Test]
    public void Every_pre_epic_rpc_keeps_its_name_path_and_messages()
    {
        var methods = Methods();

        Assert.Multiple(() =>
        {
            foreach (var (property, name, request, response) in PreEpicRpcs)
            {
                Assert.That(methods, Does.ContainKey(property), property);
                var method = methods[property];
                Assert.That(method.Name, Is.EqualTo(name), property);
                Assert.That(method.FullName, Is.EqualTo($"/{Svc}/{name}"), property);
                Assert.That(method.Type, Is.EqualTo(MethodType.Unary), property);
                Assert.That(MessageTypes(method), Is.EqualTo((request, response)), property);
            }
        });
    }

    [Test]
    public void The_service_exposes_exactly_the_pre_epic_and_added_rpcs()
    {
        var methods = Methods();
        var expected = PreEpicRpcs.Select(r => r.Name)
            .Concat(TenantAccessRpcs.Select(r => r.Name))
            .Append("AdvanceTenantRegion");

        Assert.That(methods.Values.Select(m => m.Name), Is.EquivalentTo(expected));
    }

    [Test]
    public void AdvanceTenantRegion_is_unary_and_uses_the_region_set_request_and_status_report()
    {
        var method = Methods()["AdvanceTenantRegion"];

        Assert.Multiple(() =>
        {
            Assert.That(method.FullName, Is.EqualTo($"/{Svc}/AdvanceTenantRegion"));
            Assert.That(method.Type, Is.EqualTo(MethodType.Unary));
            Assert.That(MessageTypes(method), Is.EqualTo((typeof(TenantAdminRegionSetRequest), typeof(TenantRegionStatusReport))));
        });
    }

    [Test]
    public void Every_tenant_access_rpc_is_unary_on_the_service_with_its_messages()
    {
        var methods = Methods();

        Assert.Multiple(() =>
        {
            foreach (var (name, request, response) in TenantAccessRpcs)
            {
                Assert.That(methods, Does.ContainKey(name), name);
                var method = methods[name];
                Assert.That(method.FullName, Is.EqualTo($"/{Svc}/{name}"), name);
                Assert.That(method.Type, Is.EqualTo(MethodType.Unary), name);
                Assert.That(MessageTypes(method), Is.EqualTo((request, response)), name);
            }
        });
    }

    [Test]
    public void Every_pre_epic_wire_message_keeps_its_alias_and_field_layout()
    {
        Assert.Multiple(() =>
        {
            foreach (var (type, alias, fields) in PreEpicMessages)
            {
                Assert.That(type.GetCustomAttribute<AliasAttribute>()!.Alias, Is.EqualTo(alias), type.Name);
                var layout = type.GetProperties()
                    .Select(p => (Property: p, Id: p.GetCustomAttribute<IdAttribute>()))
                    .Where(x => x.Id is not null)
                    .OrderBy(x => x.Id!.Id)
                    .Select(x => $"{x.Id!.Id} {x.Property.Name}");
                Assert.That(layout, Is.EqualTo(fields), type.Name);
            }
        });
    }

    [Test]
    public void Region_advance_fields_are_appended_to_the_existing_region_set_request()
    {
        var type = typeof(TenantAdminRegionSetRequest);
        var layout = type.GetProperties()
            .Select(p => (Property: p, Id: p.GetCustomAttribute<IdAttribute>()))
            .Where(x => x.Id is not null)
            .OrderBy(x => x.Id!.Id)
            .Select(x => $"{x.Id!.Id} {x.Property.Name}");

        Assert.Multiple(() =>
        {
            Assert.That(type.GetCustomAttribute<AliasAttribute>()!.Alias, Is.EqualTo("oitng.rgnset"));
            Assert.That(layout, Is.EqualTo(new[]
            {
                "0 TenantId",
                "1 Regions",
                "2 RegionId",
                "3 AcknowledgeDataInPlace",
            }));
        });
    }

    [Test]
    public void The_tenant_access_operations_are_appended_after_every_pre_epic_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)LatticeTenantAdminApiOperation.ListTenantGroups, Is.EqualTo(18));
            Assert.That((int)LatticeTenantAdminApiOperation.GetTenantGroup, Is.EqualTo(19));
            Assert.That((int)LatticeTenantAdminApiOperation.UpsertTenantGroup, Is.EqualTo(20));
            Assert.That((int)LatticeTenantAdminApiOperation.RemoveTenantGroup, Is.EqualTo(21));
            Assert.That((int)LatticeTenantAdminApiOperation.ListTenantGroupMembers, Is.EqualTo(22));
            Assert.That((int)LatticeTenantAdminApiOperation.AddTenantGroupMember, Is.EqualTo(23));
            Assert.That((int)LatticeTenantAdminApiOperation.RemoveTenantGroupMember, Is.EqualTo(24));
            Assert.That((int)LatticeTenantAdminApiOperation.ListTenantMembers, Is.EqualTo(25));
            Assert.That((int)LatticeTenantAdminApiOperation.AddTenantMember, Is.EqualTo(26));
            Assert.That((int)LatticeTenantAdminApiOperation.RemoveTenantMember, Is.EqualTo(27));
            Assert.That((int)LatticeTenantAdminApiOperation.ResolveTenantSubject, Is.EqualTo(28));
            Assert.That((int)LatticeTenantAdminApiOperation.PutTenantRule, Is.EqualTo(29));
            Assert.That((int)LatticeTenantAdminApiOperation.GetTenantRule, Is.EqualTo(30));
            Assert.That((int)LatticeTenantAdminApiOperation.RemoveTenantRule, Is.EqualTo(31));
            Assert.That((int)LatticeTenantAdminApiOperation.ListTenantRules, Is.EqualTo(32));
            Assert.That((int)LatticeTenantAdminApiOperation.ExplainTenantAccess, Is.EqualTo(33));
            Assert.That((int)LatticeTenantAdminApiOperation.GetTenantEffectivePermissions, Is.EqualTo(34));
            Assert.That((int)LatticeTenantAdminApiOperation.GetTenantAccessPosture, Is.EqualTo(35));
            Assert.That((int)LatticeTenantAdminApiOperation.AdvanceTenantRegion, Is.EqualTo(36));
            Assert.That(Enum.GetValues<LatticeTenantAdminApiOperation>(), Has.Length.EqualTo(37));
        });
    }

    [Test]
    public void The_public_client_implements_both_tenant_access_contracts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ILatticeTenantDirectoryAdmin).IsAssignableFrom(typeof(LatticeTenantAdminApiGrpcClient)));
            Assert.That(typeof(ILatticeTenantPolicyAdmin).IsAssignableFrom(typeof(LatticeTenantAdminApiGrpcClient)));
        });
    }

    // ---- SetTenantQuotas and the delegated access caps -------------------

    [Test]
    public void A_pre_epic_set_quotas_payload_reads_with_the_caps_at_their_defaults()
    {
        var serializer = _serializers.GetRequiredService<Serializer<TenantAdminSetQuotasRequest>>();

        var request = serializer.Deserialize(Convert.FromBase64String(PreEpicSetQuotasPayload));

        Assert.Multiple(() =>
        {
            Assert.That(request.TenantId, Is.EqualTo("acme"));
            Assert.That(request.Quotas, Is.EqualTo(PreEpicQuotas));
            Assert.That(request.Quotas.MaxGroups, Is.Null, "null means the D13 default, never unbounded");
            Assert.That(request.Quotas.MaxMembershipEdges, Is.Null);
            Assert.That(request.Quotas.MaxMemberSubjects, Is.Null);
            Assert.That(request.Quotas.MaxTenantRules, Is.Null);
        });
    }

    [Test]
    public void A_pre_epic_set_quotas_payload_round_trips_unchanged_in_meaning()
    {
        var serializer = _serializers.GetRequiredService<Serializer<TenantAdminSetQuotasRequest>>();
        var read = serializer.Deserialize(Convert.FromBase64String(PreEpicSetQuotasPayload));

        var again = serializer.Deserialize(serializer.SerializeToArray(read));

        Assert.That(again, Is.EqualTo(read));
    }

    [Test]
    public void A_set_quotas_request_carries_the_four_caps()
    {
        var serializer = _serializers.GetRequiredService<Serializer<TenantAdminSetQuotasRequest>>();
        var quotas = PreEpicQuotas with { MaxGroups = 10, MaxMembershipEdges = 20, MaxMemberSubjects = 30, MaxTenantRules = 40 };

        var copy = serializer.Deserialize(serializer.SerializeToArray(new TenantAdminSetQuotasRequest { TenantId = "acme", Quotas = quotas }));

        Assert.That(copy.Quotas, Is.EqualTo(quotas));
    }

    [Test]
    public async Task SetTenantQuotas_carries_the_caps_from_the_client_to_the_facade_and_back()
    {
        var facade = new FakeTenantAdmin();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var service = new LatticeTenantAdminGrpcService(
            methods,
            facade,
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Microsoft.Extensions.Options.Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<LatticeTenantAdminGrpcService>.Instance);
        var client = new LatticeTenantAdminApiGrpcClient(new LoopbackCallInvoker(service, _serializers), methods);
        var quotas = new TenantQuotasDescriptor { MaxGroups = 5, MaxMembershipEdges = 50, MaxMemberSubjects = 25, MaxTenantRules = 12 };

        var result = await client.SetTenantQuotasAsync("acme", quotas);

        Assert.Multiple(() =>
        {
            Assert.That(facade.LastQuotas, Is.EqualTo(quotas));
            Assert.That(result.Quotas, Is.EqualTo(quotas));
        });
    }

    [Test]
    public async Task SetTenantQuotas_leaves_an_unset_cap_null_rather_than_zero()
    {
        var facade = new FakeTenantAdmin();
        var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(_serializers);
        var service = new LatticeTenantAdminGrpcService(
            methods,
            facade,
            new FakeTenantSelfService(),
            new NullCredentialBridge(),
            new FixedAuthSchemeSource(new AuthSchemeAdvertisement()),
            Microsoft.Extensions.Options.Options.Create(new LatticeTenantAdminApiGrpcOptions()),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<LatticeTenantAdminGrpcService>.Instance);
        var client = new LatticeTenantAdminApiGrpcClient(new LoopbackCallInvoker(service, _serializers), methods);

        await client.SetTenantQuotasAsync("acme", new TenantQuotasDescriptor { MaxGroups = 7 });

        Assert.Multiple(() =>
        {
            Assert.That(facade.LastQuotas!.Value.MaxGroups, Is.EqualTo(7));
            Assert.That(facade.LastQuotas.Value.MaxMembershipEdges, Is.Null);
            Assert.That(facade.LastQuotas.Value.MaxMemberSubjects, Is.Null);
            Assert.That(facade.LastQuotas.Value.MaxTenantRules, Is.Null);
        });
    }
}
