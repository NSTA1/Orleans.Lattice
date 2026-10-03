using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Proves the released access-administration contracts are unchanged by epic #4154
/// (delegated tenant access administration): <see cref="ILatticeTenantAccessAdmin"/>
/// and <see cref="ILatticeAuthAdmin"/> keep exactly their pre-epic members, down to
/// parameter names, nullable annotations and default values, and
/// <see cref="AccessModelDescriptor"/>, which gained one init-only member, still
/// reads a payload written by the pre-epic type and re-writes every pre-epic field
/// as a pre-epic peer wrote it, with the new member appended where a pre-epic
/// reader skips it.
/// </summary>
[TestFixture]
public sealed class TenantAccessPreEpicContractTests
{
    // ILatticeTenantAccessAdmin as it stood at 83b748e4a, before any epic #4154 change.
    private static readonly string[] PreEpicTenantAccessAdminMembers =
    [
        "Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(String tenantId, CancellationToken cancellationToken = default)",
        "Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(String tenantId, String subjectId, CancellationToken cancellationToken = default)",
        "Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(String tenantId, String subjectId, CancellationToken cancellationToken = default)",
    ];

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

    // An AccessModelDescriptor with every pre-epic member set, serialized by the type as it
    // stood at 83b748e4a (before DelegatedTenantAccessAdministrationEnabled). Never regenerate.
    private const string PreEpicAccessModelPayload = "IOgIAwkBAwEDQQtlbnRyYUEnQW4gRW50cmEgb2JqZWN0IGlkLgEDAQMBA+A=";

    private static readonly AccessModelDescriptor PreEpicAccessModel = new()
    {
        AuthenticationMode = AccessAuthenticationMode.Claims,
        RulesEnforced = true,
        DirectoryAvailable = true,
        DirectoryProviderId = "entra",
        DirectoryExplanation = "An Entra object id.",
        LocalMembershipEffective = true,
        AllTreesGrantsEnabled = true,
        AccessAdministrationDelegationEnabled = true,
    };

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
    public void ILatticeTenantAccessAdmin_keeps_exactly_its_pre_epic_members() =>
        Assert.That(InterfaceSignature.Render(typeof(ILatticeTenantAccessAdmin)), Is.EquivalentTo(PreEpicTenantAccessAdminMembers));

    [Test]
    public void ILatticeAuthAdmin_keeps_exactly_its_pre_epic_members() =>
        Assert.That(InterfaceSignature.Render(typeof(ILatticeAuthAdmin)), Is.EquivalentTo(PreEpicAuthAdminMembers));

    [Test]
    public void Pre_epic_access_model_payload_reads_every_pre_epic_field_and_defaults_the_new_flag_off()
    {
        var read = _serializer.Deserialize<AccessModelDescriptor>(Convert.FromBase64String(PreEpicAccessModelPayload));

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(PreEpicAccessModel));
            Assert.That(read.DelegatedTenantAccessAdministrationEnabled, Is.False,
                "A server that predates the flag must read as the feature being off.");
        });
    }

    [Test]
    public void Pre_epic_access_model_payload_rewrites_every_pre_epic_field_unchanged()
    {
        var payload = Convert.FromBase64String(PreEpicAccessModelPayload);

        var rewritten = _serializer.SerializeToArray(_serializer.Deserialize<AccessModelDescriptor>(payload));

        // The added member is written as a trailing field before the end-of-object marker,
        // which a pre-epic reader skips; every pre-epic field is byte-identical.
        Assert.Multiple(() =>
        {
            Assert.That(rewritten.Length, Is.GreaterThan(payload.Length));
            Assert.That(rewritten[..(payload.Length - 1)], Is.EqualTo(payload[..^1]),
                "Every pre-epic field must be written exactly as a pre-epic peer wrote it.");
            Assert.That(rewritten[^1], Is.EqualTo(payload[^1]));
        });
    }

    [Test]
    public void Access_model_with_the_new_flag_set_appends_it_after_every_pre_epic_field()
    {
        var payload = Convert.FromBase64String(PreEpicAccessModelPayload);
        var extended = PreEpicAccessModel with { DelegatedTenantAccessAdministrationEnabled = true };

        var written = _serializer.SerializeToArray(extended);
        var read = _serializer.Deserialize<AccessModelDescriptor>(written);

        Assert.Multiple(() =>
        {
            Assert.That(written[..(payload.Length - 1)], Is.EqualTo(payload[..^1]),
                "Every pre-epic field must be written exactly as a pre-epic peer wrote it, so it can skip the new one.");
            Assert.That(written.Length, Is.GreaterThan(payload.Length));
            Assert.That(read, Is.EqualTo(extended));
            Assert.That(JsonSerializer.Serialize(read), Does.Contain("\"DelegatedTenantAccessAdministrationEnabled\":true"));
        });
    }

    [Test]
    public void The_new_access_model_flag_defaults_off() =>
        Assert.That(new AccessModelDescriptor { DirectoryProviderId = "null", DirectoryExplanation = "x" }
            .DelegatedTenantAccessAdministrationEnabled, Is.False);
}
