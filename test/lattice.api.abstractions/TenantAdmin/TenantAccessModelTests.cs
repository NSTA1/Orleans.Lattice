using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Value-shape and wire tests for the delegated tenant-access DTOs (epic #4154):
/// every DTO round-trips, fully populated, through a real Orleans serializer with
/// every member intact; enum values are pinned because they are wire format; and
/// defaults and computed members behave as documented.
/// </summary>
[TestFixture]
public sealed class TenantAccessModelTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void SetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void TearDown() => _services.Dispose();

    private static TenantRuleView TenantRule() => new()
    {
        RuleId = "readers-orders",
        Layer = TenantRuleLayer.Tenant,
        Origin = TenantRuleOrigin.Tenant,
        Editable = true,
        SubjectId = "readers",
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.Prefix,
        TreeName = "orders",
        KeyOrPrefix = "eu/",
        Operations = LatticeOperation.Read | LatticeOperation.RangeRead,
        Effect = LatticeEffect.Allow,
    };

    private static TenantQuotaDimensionUsage Cap(long usage, long limit) =>
        new() { Usage = usage, Limit = limit, BurstLimit = limit };

    private static IEnumerable<TestCaseData> PopulatedSamples()
    {
        object[] samples =
        [
            new TenantAccessPageRequest { PageSize = 25, PageToken = "ops" },
            new TenantGroupDescriptor { Name = "readers", DisplayName = "Readers" },
            new TenantGroupPage
            {
                Entries = [new TenantGroupDescriptor { Name = "admins" }, new TenantGroupDescriptor { Name = "readers" }],
                NextPageToken = "readers",
            },
            new TenantGroupMember { MemberId = "engineering", Kind = TenantSubjectKind.ClusterGroup },
            new TenantGroupRemovalResult
            {
                TenantId = "acme",
                GroupName = "readers",
                Removed = true,
                EdgesRemoved = 3,
                RemovedFromMemberSet = true,
                RemovedFromAdminSet = true,
                RemovedRuleIds = ["r1", "r2"],
            },
            new TenantMemberEntry { SubjectId = "alice@example.com", Kind = TenantSubjectKind.User },
            new TenantMemberPage
            {
                Entries = [new TenantMemberEntry { SubjectId = "readers", Kind = TenantSubjectKind.TenantGroup }],
                NextPageToken = "readers",
            },
            new TenantMembershipChangeResult
            {
                TenantId = "acme", GroupName = "readers", SubjectId = "bob", SubjectKind = TenantSubjectKind.User, Changed = true,
            },
            new TenantSubjectResolution
            {
                TenantId = "acme",
                SubjectId = "bob",
                SubjectKind = TenantSubjectKind.User,
                IsAdmin = true,
                IsMember = true,
                AdminEntries = [new TenantMemberEntry { SubjectId = "admins", Kind = TenantSubjectKind.TenantGroup }],
                MemberEntries = [new TenantMemberEntry { SubjectId = "bob" }],
            },
            new TenantRuleDraft
            {
                RuleId = "readers-orders",
                SubjectId = "readers",
                SubjectKind = TenantSubjectKind.TenantGroup,
                ScopeKind = TenantRuleScopeKind.Key,
                TreeName = "orders",
                KeyOrPrefix = "order-42",
                Operations = LatticeOperation.Read,
                Effect = LatticeEffect.Deny,
            },
            TenantRule(),
            new TenantRulePage { Entries = [TenantRule()], NextPageToken = "orders\u0000readers-orders" },
            new TenantExplanation
            {
                TenantId = "acme",
                SubjectId = "bob",
                SubjectKind = TenantSubjectKind.User,
                TreeName = "orders",
                Key = "eu/1",
                Operation = LatticeOperation.Read,
                Allowed = true,
                Filtered = true,
                Reason = "partial",
                DecidingLayer = TenantRuleLayer.Tenant,
                DecidingRule = TenantRule(),
                DefaultEffect = LatticeEffect.Deny,
                MatchedRules = [TenantRule()],
            },
            new TenantEffectivePermissions
            {
                TenantId = "acme",
                SubjectId = "bob",
                SubjectKind = TenantSubjectKind.ClusterGroup,
                TreeName = "orders",
                Rules = [TenantRule(), new TenantRuleView { RuleId = "app:inventory:reader", Origin = TenantRuleOrigin.AppRole }],
            },
            new TenantAccessPosture
            {
                TenantId = "acme",
                Enabled = true,
                CallerIsTenantAdmin = true,
                CallerIsPlatformOperator = true,
                Groups = Cap(4, 500),
                MembershipEdges = Cap(40, 10000),
                MemberSubjects = Cap(7, 5000),
                TenantRules = Cap(12, 1000),
            },
        ];

        return samples.Select(s => new TestCaseData(s).SetName($"Populated_{s.GetType().Name}_round_trips_with_every_member"));
    }

    [TestCaseSource(nameof(PopulatedSamples))]
    public void Populated_dto_round_trips_with_every_member(object sample)
    {
        var read = _serializer.Deserialize<object>(_serializer.SerializeToArray(sample));

        Assert.Multiple(() =>
        {
            Assert.That(read.GetType(), Is.EqualTo(sample.GetType()));
            Assert.That(JsonSerializer.Serialize(read, read.GetType()), Is.EqualTo(JsonSerializer.Serialize(sample, sample.GetType())));
        });
    }

    [Test]
    public void The_round_trip_samples_cover_every_delegated_access_dto()
    {
        // The alias block epic #4154 added to ApiTenantAdminTypeAliases.
        string[] epicAliases =
        [
            ApiTenantAdminTypeAliases.TenantSubjectKind, ApiTenantAdminTypeAliases.TenantRuleLayer,
            ApiTenantAdminTypeAliases.TenantRuleOrigin, ApiTenantAdminTypeAliases.TenantRuleScopeKind,
            ApiTenantAdminTypeAliases.TenantAccessPageRequest, ApiTenantAdminTypeAliases.TenantGroupDescriptor,
            ApiTenantAdminTypeAliases.TenantGroupPage, ApiTenantAdminTypeAliases.TenantGroupMember,
            ApiTenantAdminTypeAliases.TenantGroupRemovalResult, ApiTenantAdminTypeAliases.TenantMemberEntry,
            ApiTenantAdminTypeAliases.TenantMemberPage, ApiTenantAdminTypeAliases.TenantMembershipChangeResult,
            ApiTenantAdminTypeAliases.TenantSubjectResolution, ApiTenantAdminTypeAliases.TenantRuleDraft,
            ApiTenantAdminTypeAliases.TenantRuleView, ApiTenantAdminTypeAliases.TenantRulePage,
            ApiTenantAdminTypeAliases.TenantExplanation, ApiTenantAdminTypeAliases.TenantEffectivePermissions,
            ApiTenantAdminTypeAliases.TenantAccessPosture,
        ];
        var aliased = typeof(ApiTenantAdminTypeAliases).Assembly.GetTypes()
            .Where(t => t.GetCustomAttributes(typeof(AliasAttribute), false).Cast<AliasAttribute>()
                .Any(a => epicAliases.Contains(a.Alias)))
            .ToList();
        var sampled = PopulatedSamples().Select(c => c.Arguments[0]!.GetType()).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(aliased, Has.Count.EqualTo(epicAliases.Length), "every epic alias is used by exactly one type");
            Assert.That(sampled, Is.EquivalentTo(aliased.Where(t => !t.IsEnum)));
        });
    }

    [TestCase(TenantSubjectKind.User, 0)]
    [TestCase(TenantSubjectKind.TenantGroup, 1)]
    [TestCase(TenantSubjectKind.ClusterGroup, 2)]
    public void TenantSubjectKind_values_are_pinned(TenantSubjectKind kind, int value) => Assert.That((int)kind, Is.EqualTo(value));

    [TestCase(TenantRuleLayer.Platform, 0)]
    [TestCase(TenantRuleLayer.Tenant, 1)]
    public void TenantRuleLayer_values_are_pinned(TenantRuleLayer layer, int value) => Assert.That((int)layer, Is.EqualTo(value));

    [TestCase(TenantRuleOrigin.PlatformTree, 0)]
    [TestCase(TenantRuleOrigin.PlatformWide, 1)]
    [TestCase(TenantRuleOrigin.AppRole, 2)]
    [TestCase(TenantRuleOrigin.Tenant, 3)]
    public void TenantRuleOrigin_values_are_pinned(TenantRuleOrigin origin, int value) => Assert.That((int)origin, Is.EqualTo(value));

    [TestCase(TenantRuleScopeKind.Tree, LatticeScopeKind.Tree)]
    [TestCase(TenantRuleScopeKind.Key, LatticeScopeKind.Key)]
    [TestCase(TenantRuleScopeKind.Prefix, LatticeScopeKind.Prefix)]
    public void TenantRuleScopeKind_mirrors_the_cluster_scope_kind_values(TenantRuleScopeKind kind, LatticeScopeKind cluster) =>
        Assert.That((int)kind, Is.EqualTo((int)cluster));

    [Test]
    public void TenantRuleScopeKind_TenantWide_follows_the_cluster_kinds() =>
        Assert.That((int)TenantRuleScopeKind.TenantWide, Is.EqualTo(3));

    [TestCase(TenantAccessConfinementRule.GroupNesting, 0)]
    [TestCase(TenantAccessConfinementRule.ForeignTenantGroup, 1)]
    [TestCase(TenantAccessConfinementRule.RuleTree, 2)]
    [TestCase(TenantAccessConfinementRule.RuleOperations, 3)]
    [TestCase(TenantAccessConfinementRule.ReservedRuleId, 4)]
    public void TenantAccessConfinementRule_values_are_pinned(TenantAccessConfinementRule rule, int value) =>
        Assert.That((int)rule, Is.EqualTo(value));

    [Test]
    public void The_zero_values_fail_closed()
    {
        var view = new TenantRuleView { RuleId = "r" };

        Assert.Multiple(() =>
        {
            Assert.That(view.Layer, Is.EqualTo(TenantRuleLayer.Platform), "an unset layer must read as read-only Platform");
            Assert.That(view.Origin, Is.Not.EqualTo(TenantRuleOrigin.Tenant), "an unset origin must not read as the tenant's own rule");
            Assert.That(view.Editable, Is.False);
            Assert.That(new TenantAccessPosture { TenantId = "acme" }.Enabled, Is.False);
        });
    }

    [TestCase(0, TenantAccessPageRequest.DefaultPageSize)]
    [TestCase(-5, TenantAccessPageRequest.DefaultPageSize)]
    [TestCase(1, 1)]
    [TestCase(250, 250)]
    [TestCase(TenantAccessPageRequest.MaxPageSize, TenantAccessPageRequest.MaxPageSize)]
    [TestCase(TenantAccessPageRequest.MaxPageSize + 1, TenantAccessPageRequest.MaxPageSize)]
    public void TenantAccessPageRequest_clamps_its_page_size(int requested, int effective) =>
        Assert.That(new TenantAccessPageRequest { PageSize = requested }.EffectivePageSize, Is.EqualTo(effective));

    [Test]
    public void TenantAccessPageRequest_defaults_to_the_first_default_sized_page()
    {
        var page = new TenantAccessPageRequest();

        Assert.Multiple(() =>
        {
            Assert.That(page.PageSize, Is.EqualTo(TenantAccessPageRequest.DefaultPageSize));
            Assert.That(page.PageToken, Is.Null);
        });
    }

    [Test]
    public void The_pages_and_lists_default_to_empty_rather_than_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new TenantGroupPage().Entries, Is.Empty);
            Assert.That(new TenantMemberPage().Entries, Is.Empty);
            Assert.That(new TenantRulePage().Entries, Is.Empty);
            Assert.That(new TenantGroupRemovalResult { TenantId = "acme", GroupName = "g" }.RemovedRuleIds, Is.Empty);
            Assert.That(new TenantSubjectResolution { TenantId = "acme", SubjectId = "s" }.AdminEntries, Is.Empty);
            Assert.That(new TenantSubjectResolution { TenantId = "acme", SubjectId = "s" }.MemberEntries, Is.Empty);
            Assert.That(new TenantEffectivePermissions { TenantId = "acme", SubjectId = "s" }.Rules, Is.Empty);
            Assert.That(new TenantExplanation { TenantId = "acme", SubjectId = "s", TreeName = "t" }.MatchedRules, Is.Empty);
        });
    }

    [TestCase(TenantRuleOrigin.PlatformTree, false)]
    [TestCase(TenantRuleOrigin.PlatformWide, true)]
    [TestCase(TenantRuleOrigin.AppRole, true)]
    [TestCase(TenantRuleOrigin.Tenant, false)]
    public void TenantRuleView_withholds_the_subject_of_platform_wide_and_app_role_rules(TenantRuleOrigin origin, bool withheld) =>
        Assert.That(new TenantRuleView { RuleId = "r", Origin = origin }.SubjectWithheld, Is.EqualTo(withheld));

    [Test]
    public void TenantExplanation_reports_the_deciding_rule_id()
    {
        var decided = new TenantExplanation { TenantId = "acme", SubjectId = "s", TreeName = "t", DecidingRule = TenantRule() };
        var undecided = decided with { DecidingRule = null };

        Assert.Multiple(() =>
        {
            Assert.That(decided.DecidingRuleId, Is.EqualTo("readers-orders"));
            Assert.That(undecided.DecidingRuleId, Is.Null);
            Assert.That(undecided.DecidingLayer, Is.Null);
        });
    }

    [Test]
    public void A_withheld_rule_round_trips_with_its_subject_absent()
    {
        var withheld = new TenantRuleView { RuleId = "*-deny", Origin = TenantRuleOrigin.PlatformWide, Effect = LatticeEffect.Deny };

        var read = _serializer.Deserialize<TenantRuleView>(_serializer.SerializeToArray(withheld));

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(withheld));
            Assert.That(read.SubjectId, Is.Null);
            Assert.That(read.TreeName, Is.Null);
            Assert.That(read.Operations, Is.EqualTo(LatticeOperation.None));
            Assert.That(read.SubjectWithheld, Is.True);
        });
    }

    [Test]
    public void The_scalar_dtos_are_value_records()
    {
        var entry = new TenantMemberEntry { SubjectId = "bob" };
        var member = new TenantGroupMember { MemberId = "bob" };
        var draft = new TenantRuleDraft { RuleId = "r", SubjectId = "bob" };

        Assert.Multiple(() =>
        {
            Assert.That(entry, Is.EqualTo(entry with { }));
            Assert.That(entry, Is.Not.EqualTo(entry with { Kind = TenantSubjectKind.ClusterGroup }));
            Assert.That(member, Is.EqualTo(member with { }));
            Assert.That(draft, Is.EqualTo(draft with { }));
            Assert.That(draft.SubjectKind, Is.EqualTo(TenantSubjectKind.User));
            Assert.That(draft.ScopeKind, Is.EqualTo(TenantRuleScopeKind.Tree));
        });
    }
}
