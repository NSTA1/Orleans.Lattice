using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Proves the cluster authorization facade's contract survives epic #4154 (F3):
/// <see cref="ILatticeAuthAdmin"/> keeps exactly its pre-epic members, and each auth
/// DTO that gained an init-only member (<see cref="AuthPageRequest.IncludeTenantGroups"/>,
/// <see cref="AuthRulePage.TenantRuleTenants"/>, <see cref="AuthExplanation.DecidingLayer"/>
/// and <see cref="AuthExplanation.DecidingRuleId"/>, and
/// <see cref="AuthEffectivePermissions.RuleLayers"/>) still reads a payload written by
/// the pre-epic type, defaults the new member, and re-writes every pre-epic field as a
/// pre-epic peer wrote it, with the new member appended where a pre-epic reader skips it.
/// </summary>
[TestFixture]
public sealed class AuthAdminTenantTierContractTests
{
    // ILatticeAuthAdmin as it stood at 83b748e4a, before any epic #4154 change.
    private static readonly string[] PreEpicAuthAdminMembers =
    [
        "Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)",
        "Task<AuthGroup?> GetGroupAsync(String groupId, CancellationToken cancellationToken = default)",
        "Task RemoveGroupAsync(String groupId, CancellationToken cancellationToken = default)",
        "Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)",
        "Task AddMemberAsync(String groupId, String memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)",
        "Task RemoveMemberAsync(String groupId, String memberId, CancellationToken cancellationToken = default)",
        "Task<IReadOnlyList<String>> ListGroupMembersAsync(String groupId, CancellationToken cancellationToken = default)",
        "Task<IReadOnlyList<String>> ListSubjectGroupsAsync(String memberId, CancellationToken cancellationToken = default)",
        "Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)",
        "Task<LatticeAuthorizationRule?> GetRuleAsync(String treeId, String ruleId, CancellationToken cancellationToken = default)",
        "Task<Boolean> RemoveRuleAsync(String treeId, String ruleId, CancellationToken cancellationToken = default)",
        "Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)",
        "Task<AuthRulePage> ListRulesForTreeAsync(String treeId, AuthPageRequest request, CancellationToken cancellationToken = default)",
        "Task<AuthExplanation> ExplainAsync(String subjectId, LatticeOperation operation, LatticeScope scope, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)",
        "Task<AuthEffectivePermissions> EffectivePermissionsAsync(String subjectId, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)",
        "Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)",
        "Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(String principalId, CancellationToken cancellationToken = default)",
        "Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)",
    ];

    // Payloads written by the pre-epic types (wire-identical to 83b748e4a) for the sample
    // values below. Never regenerate.
    private const string PreEpicPageRequestPayload = "IOgAyUEHdG9rAQPg";
    private const string PreEpicRulePagePayload =
        "IOgwAYjrzVERb2x6LmFyW10AAyHoQAVyMSHoCAMBQQV1MeAh6AgDAUEFdDHBAeAJAwUJAwHBAeDgQQNuQQlhY21l4A==";
    private const string PreEpicExplanationPayload =
        "IOhABXUxKTUAA0EFZzHgCQMFIegIAwFBBXQxwQHgAQMBAUEHd2h5CQMFMQGI681REW9sei5hcltdAAMh6EAFcjEh6AgDAcEF4CHoCAMBwRPBAeAJAwUJAwHBAeDgIegAAwED4OA=";
    private const string PreEpicEffectivePermissionsPayload =
        "IOhABXUxKTUAA0EFZzHgMQGI681REW9sei5hcltdAAMh6EAFcjEh6AgDAcEF4CHoCAMBQQV0McEB4AkDBQkDAcEB4OAh6AADAQHg4A==";

    private static readonly LatticeAuthorizationRule SampleRule =
        new("r1", LatticeSubjectSelector.User("u1"), LatticeScope.Tree("t1"), LatticeOperation.Read, LatticeEffect.Allow);

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

    [Test]
    public void ILatticeAuthAdmin_keeps_exactly_its_pre_epic_members() =>
        Assert.That(InterfaceSignature.Render(typeof(ILatticeAuthAdmin)), Is.EquivalentTo(PreEpicAuthAdminMembers));

    // ----- AuthPageRequest.IncludeTenantGroups -----

    [Test]
    public void Pre_epic_page_request_reads_every_field_and_defaults_to_cluster_groups_only()
    {
        var read = Read<AuthPageRequest>(PreEpicPageRequestPayload);

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(new AuthPageRequest { PageSize = 50, PageToken = "tok", ActiveTenantOnly = true }));
            Assert.That(read.IncludeTenantGroups, Is.False);
        });
    }

    [Test]
    public void Page_request_round_trips_pre_epic_fields_and_appends_the_new_member() =>
        AssertAppended(PreEpicPageRequestPayload, (AuthPageRequest r) => r with { IncludeTenantGroups = true });

    // ----- AuthRulePage.TenantRuleTenants -----

    [Test]
    public void Pre_epic_rule_page_reads_every_field_with_no_tenant_marks()
    {
        var read = Read<AuthRulePage>(PreEpicRulePagePayload);

        Assert.Multiple(() =>
        {
            Assert.That(read.Entries, Is.EqualTo(new[] { SampleRule }));
            Assert.That(read.NextPageToken, Is.EqualTo("n"));
            Assert.That(read.Tenant, Is.EqualTo("acme"));
            Assert.That(read.TenantRuleTenants, Is.Empty);
        });
    }

    [Test]
    public void Rule_page_round_trips_pre_epic_fields_and_appends_the_new_member() =>
        AssertAppended(PreEpicRulePagePayload, (AuthRulePage p) => p with { TenantRuleTenants = new string?[] { "acme" } },
            (expected, actual) => Assert.That(actual.TenantRuleTenants, Is.EqualTo(expected.TenantRuleTenants)));

    // ----- AuthExplanation.DecidingLayer / DecidingRuleId -----

    [Test]
    public void Pre_epic_explanation_reads_every_field_with_no_deciding_layer()
    {
        var read = Read<AuthExplanation>(PreEpicExplanationPayload);

        Assert.Multiple(() =>
        {
            Assert.That(read.SubjectId, Is.EqualTo("u1"));
            Assert.That(read.GroupIds, Is.EqualTo(new[] { "g1" }));
            Assert.That(read.Scope, Is.EqualTo(LatticeScope.Tree("t1")));
            Assert.That(read.Allowed, Is.True);
            Assert.That(read.Reason, Is.EqualTo("why"));
            Assert.That(read.DefaultEffect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(read.MatchedRules, Is.EqualTo(new[] { SampleRule }));
            Assert.That(read.Posture, Is.EqualTo(new AuthPolicyPosture { AllTreesGrantsEnabled = true, AccessAdministrationDelegationEnabled = true }));
            Assert.That(read.DecidingLayer, Is.Null);
            Assert.That(read.DecidingRuleId, Is.Null);
        });
    }

    [Test]
    public void Explanation_round_trips_pre_epic_fields_and_appends_the_new_members() =>
        AssertAppended(
            PreEpicExplanationPayload,
            (AuthExplanation e) => e with { DecidingLayer = TenantRuleLayer.Tenant, DecidingRuleId = "tenant:acme:r" },
            (expected, actual) => Assert.Multiple(() =>
            {
                Assert.That(actual.DecidingLayer, Is.EqualTo(expected.DecidingLayer));
                Assert.That(actual.DecidingRuleId, Is.EqualTo(expected.DecidingRuleId));
            }));

    [Test]
    public void Explanation_round_trips_a_platform_deciding_layer_distinctly_from_none()
    {
        // Platform is the enum's zero value, so the nullable must keep "Platform decided"
        // distinct from "no rule decided".
        var platform = Read<AuthExplanation>(PreEpicExplanationPayload) with { DecidingLayer = TenantRuleLayer.Platform };

        var read = _serializer.Deserialize<AuthExplanation>(_serializer.SerializeToArray(platform));

        Assert.That(read.DecidingLayer, Is.EqualTo(TenantRuleLayer.Platform));
    }

    // ----- AuthEffectivePermissions.RuleLayers -----

    [Test]
    public void Pre_epic_effective_permissions_reads_every_field_with_no_rule_layers()
    {
        var read = Read<AuthEffectivePermissions>(PreEpicEffectivePermissionsPayload);

        Assert.Multiple(() =>
        {
            Assert.That(read.SubjectId, Is.EqualTo("u1"));
            Assert.That(read.GroupIds, Is.EqualTo(new[] { "g1" }));
            Assert.That(read.Rules, Is.EqualTo(new[] { SampleRule }));
            Assert.That(read.Posture, Is.EqualTo(new AuthPolicyPosture { AllTreesGrantsEnabled = true }));
            Assert.That(read.RuleLayers, Is.Empty);
        });
    }

    [Test]
    public void Effective_permissions_round_trips_pre_epic_fields_and_appends_the_new_member() =>
        AssertAppended(
            PreEpicEffectivePermissionsPayload,
            (AuthEffectivePermissions p) => p with { RuleLayers = new[] { TenantRuleLayer.Tenant } },
            (expected, actual) => Assert.That(actual.RuleLayers, Is.EqualTo(expected.RuleLayers)));

    // ----- Defaults -----

    [Test]
    public void New_members_default_to_their_pre_epic_meaning()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new AuthPageRequest().IncludeTenantGroups, Is.False);
            Assert.That(new AuthRulePage().TenantRuleTenants, Is.Empty);
            Assert.That(new AuthEffectivePermissions { SubjectId = "u" }.RuleLayers, Is.Empty);
            var explanation = new AuthExplanation { SubjectId = "u", Scope = LatticeScope.Tree("t") };
            Assert.That(explanation.DecidingLayer, Is.Null);
            Assert.That(explanation.DecidingRuleId, Is.Null);
        });
    }

    private T Read<T>(string payload) => _serializer.Deserialize<T>(Convert.FromBase64String(payload));

    /// <summary>
    /// Asserts that re-writing the pre-epic payload reproduces every pre-epic field
    /// byte for byte, and that setting the new members appends them after every
    /// pre-epic field (before the end-of-object marker) and reads them back.
    /// </summary>
    private void AssertAppended<T>(string payloadBase64, Func<T, T> extend, Action<T, T>? assertExtended = null)
    {
        var payload = Convert.FromBase64String(payloadBase64);
        var rewritten = _serializer.SerializeToArray(_serializer.Deserialize<T>(payload));
        var extended = extend(_serializer.Deserialize<T>(payload));
        var written = _serializer.SerializeToArray(extended);
        var read = _serializer.Deserialize<T>(written);

        Assert.Multiple(() =>
        {
            // Orleans writes a default-valued appended field too, so the rewrite is the
            // pre-epic payload with the new member inserted before the end marker.
            Assert.That(rewritten[..(payload.Length - 1)], Is.EqualTo(payload[..^1]),
                "Every pre-epic field must be re-written exactly as a pre-epic peer wrote it.");
            Assert.That(rewritten[^1], Is.EqualTo(payload[^1]));
            Assert.That(written[..(payload.Length - 1)], Is.EqualTo(payload[..^1]),
                "The new member must follow every pre-epic field, so a pre-epic reader can skip it.");
            Assert.That(written.Length, Is.GreaterThan(payload.Length));
            if (assertExtended is null)
            {
                Assert.That(read, Is.EqualTo(extended));
            }
            else
            {
                assertExtended(extended, read);
            }
        });
    }
}
