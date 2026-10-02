using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Serialization round trips for the delegated tenant access wire messages the gRPC
/// binding adds: every field survives the Orleans serializer, optional fields
/// survive as <see langword="null"/>, and the response wrappers carry an absent
/// result and an empty list faithfully.
/// </summary>
[TestFixture]
public sealed class TenantAdminGrpcTenantAccessDtoSerializationTests
{
    private ServiceProvider _serializers = null!;

    [SetUp]
    public void SetUp() => _serializers = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _serializers.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void TenantAdminAccessListRequest_round_trips()
    {
        var value = new TenantAdminAccessListRequest { TenantId = "acme", Page = new TenantAccessPageRequest { PageSize = 7, PageToken = "x" } };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminGroupRequest_round_trips()
    {
        var value = new TenantAdminGroupRequest { TenantId = "acme", GroupName = "ops" };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminGroupUpsertRequest_round_trips()
    {
        var value = new TenantAdminGroupUpsertRequest
        {
            TenantId = "acme",
            Group = new TenantGroupDescriptor { Name = "ops", DisplayName = "Operations" },
        };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminGroupMemberRequest_round_trips()
    {
        var value = new TenantAdminGroupMemberRequest
        {
            TenantId = "acme",
            GroupName = "ops",
            MemberId = "staff",
            MemberKind = TenantSubjectKind.ClusterGroup,
        };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminMemberRequest_round_trips()
    {
        var value = new TenantAdminMemberRequest { TenantId = "acme", SubjectId = "readers", SubjectKind = TenantSubjectKind.TenantGroup };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminRulePutRequest_round_trips()
    {
        var value = new TenantAdminRulePutRequest
        {
            TenantId = "acme",
            Rule = new TenantRuleDraft
            {
                RuleId = "r",
                SubjectId = "readers",
                SubjectKind = TenantSubjectKind.TenantGroup,
                ScopeKind = TenantRuleScopeKind.Key,
                TreeName = "orders",
                KeyOrPrefix = "k1",
                Operations = LatticeOperation.Read | LatticeOperation.Write,
                Effect = LatticeEffect.Deny,
            },
        };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminRuleRequest_round_trips()
    {
        var value = new TenantAdminRuleRequest { TenantId = "acme", RuleId = "r" };

        Assert.That(RoundTrip(value), Is.EqualTo(value));
    }

    [Test]
    public void TenantAdminExplainRequest_round_trips_with_and_without_a_key()
    {
        var keyed = new TenantAdminExplainRequest
        {
            TenantId = "acme",
            SubjectId = "readers",
            TreeName = "orders",
            Key = "k1",
            Operation = LatticeOperation.RangeRead,
            SubjectKind = TenantSubjectKind.TenantGroup,
        };
        var whole = keyed with { Key = null };

        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(keyed), Is.EqualTo(keyed));
            Assert.That(RoundTrip(whole), Is.EqualTo(whole));
        });
    }

    [Test]
    public void TenantAdminEffectivePermissionsRequest_round_trips_with_and_without_a_tree()
    {
        var narrowed = new TenantAdminEffectivePermissionsRequest
        {
            TenantId = "acme",
            SubjectId = "bob",
            TreeName = "orders",
            SubjectKind = TenantSubjectKind.User,
        };
        var all = narrowed with { TreeName = null };

        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(narrowed), Is.EqualTo(narrowed));
            Assert.That(RoundTrip(all), Is.EqualTo(all));
        });
    }

    [Test]
    public void TenantAdminGroupLookup_round_trips_a_found_and_an_absent_group()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(new TenantAdminGroupLookup { Group = new TenantGroupDescriptor { Name = "ops" } }).Group,
                Is.EqualTo(new TenantGroupDescriptor { Name = "ops" }));
            Assert.That(RoundTrip(new TenantAdminGroupLookup()).Group, Is.Null);
        });
    }

    [Test]
    public void TenantAdminGroupMemberList_round_trips_members_and_an_empty_list()
    {
        var members = new TenantAdminGroupMemberList
        {
            Members = [new TenantGroupMember { MemberId = "devs", Kind = TenantSubjectKind.TenantGroup }],
        };

        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(members).Members, Is.EqualTo(members.Members));
            Assert.That(RoundTrip(new TenantAdminGroupMemberList()).Members, Is.Not.Null.And.Empty);
        });
    }

    [Test]
    public void TenantAdminRuleLookup_round_trips_a_found_and_an_absent_rule()
    {
        var view = new TenantRuleView { RuleId = "r", Layer = TenantRuleLayer.Tenant, Origin = TenantRuleOrigin.Tenant, Editable = true };

        Assert.Multiple(() =>
        {
            Assert.That(RoundTrip(new TenantAdminRuleLookup { Rule = view }).Rule, Is.EqualTo(view));
            Assert.That(RoundTrip(new TenantAdminRuleLookup()).Rule, Is.Null);
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public void TenantAdminRuleRemoval_round_trips(bool removed) =>
        Assert.That(RoundTrip(new TenantAdminRuleRemoval { Removed = removed }).Removed, Is.EqualTo(removed));
}
