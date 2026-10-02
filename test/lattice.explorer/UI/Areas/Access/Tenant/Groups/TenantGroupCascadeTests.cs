using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// Issue #4162: what references a tenant group (<see cref="TenantGroupUsage"/>) and the
/// cascade its deletion is confirmed with (<see cref="TenantGroupCascade"/>), read from
/// the delegated contracts before anything is written.
/// </summary>
[TestFixture]
public sealed class TenantGroupCascadeTests
{
    private FakeTenantAccessFacades _facades = null!;
    private TenantAccessCatalog _access = null!;

    [SetUp]
    public async Task SeedAsync()
    {
        _facades = new FakeTenantAccessFacades().AsTenantAdmin().WithGroup("acme", "ops").WithGroup("acme", "eng");
        _access = new TenantAccessCatalog(_facades);
        await _facades.DirectoryFake.AddGroupMemberAsync("acme", "ops", "alice");
        await _facades.DirectoryFake.AddGroupMemberAsync("acme", "ops", "eng", TenantSubjectKind.TenantGroup);
        await _facades.PolicyFake.PutRuleAsync("acme", Rule("readers", "ops"));
        _facades.PolicyFake.SeedPlatformRule("acme", Seeded("app:crm:viewer:1", TenantRuleOrigin.AppRole, "ops"));
        _facades.PolicyFake.SeedPlatformRule("acme", Seeded("platform-deny", TenantRuleOrigin.PlatformTree, "ops"));
    }

    [Test]
    public async Task The_usage_counts_members_rules_and_app_role_bindings()
    {
        var usage = await TenantGroupUsage.ReadAsync(_access, "acme", "ops", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.MemberCount, Is.EqualTo(2));
            Assert.That(usage.RuleCount, Is.EqualTo(2), "the tenant rule and the platform rule");
            Assert.That(usage.AppRoleCount, Is.EqualTo(1));
            Assert.That(usage.TenantRuleCount, Is.EqualTo(1), "only the tenant's own rules are removed with it");
        });
    }

    [Test]
    public async Task A_part_that_cannot_be_read_is_unknown_never_zero()
    {
        _facades.ServesPolicy = false;

        var usage = await TenantGroupUsage.ReadAsync(_access, "acme", "ops", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.MemberCount, Is.EqualTo(2));
            Assert.That(usage.RuleCount, Is.Null);
            Assert.That(usage.AppRoleCount, Is.Null);
            Assert.That(usage.TenantRuleCount, Is.Null);
            Assert.That(TenantGroupUsage.Unknown.MemberCount, Is.Null);
        });
    }

    [Test]
    public async Task A_failing_read_is_unknown_and_a_cancellation_is_not_swallowed()
    {
        _facades.Gate.Denied = true;
        var usage = await TenantGroupUsage.ReadAsync(_access, "acme", "ops", CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(usage.MemberCount, Is.Null);
            Assert.That(usage.RuleCount, Is.Null);
        });

        _facades.Gate.Denied = false;
        _facades.Gate.NextFailure = new OperationCanceledException();
        Assert.That(() => TenantGroupUsage.ReadAsync(_access, "acme", "ops", CancellationToken.None), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task The_cascade_preview_names_the_member_and_administrator_set_entries()
    {
        await _facades.DirectoryFake.AddMemberAsync("acme", "ops", TenantSubjectKind.TenantGroup);
        _facades.DirectoryFake.SeedAdmin("acme", new TenantMemberEntry { SubjectId = "ops", Kind = TenantSubjectKind.TenantGroup });

        var cascade = await TenantGroupCascade.ReadAsync(_access, "acme", "ops", CancellationToken.None);

        Assert.That(
            cascade.Text("ops"),
            Is.EqualTo("Deleting ops removes 2 member entries, 1 rule, 1 app binding, its member-set entry, its administrator entry."));
    }

    [Test]
    public async Task A_group_entered_only_through_another_group_is_not_called_an_entry_of_its_own()
    {
        await _facades.DirectoryFake.UpsertGroupAsync("acme", new TenantGroupDescriptor { Name = "all" });
        await _facades.DirectoryFake.AddGroupMemberAsync("acme", "all", "ops", TenantSubjectKind.TenantGroup);
        await _facades.DirectoryFake.AddMemberAsync("acme", "all", TenantSubjectKind.TenantGroup);

        var cascade = await TenantGroupCascade.ReadAsync(_access, "acme", "ops", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cascade.InMemberSet, Is.False);
            Assert.That(cascade.InAdminSet, Is.False);
        });
    }

    [Test]
    public void A_cascade_that_could_not_be_read_in_full_says_so()
    {
        var cascade = new TenantGroupCascade(TenantGroupUsage.Unknown, null, null);

        Assert.Multiple(() =>
        {
            Assert.That(cascade.Text("ops"), Does.StartWith("What deleting ops removes could not be read in full."));
            Assert.That(() => cascade.Text(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_reads_refuse_missing_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantGroupUsage.ReadAsync(null!, "acme", "ops", CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupUsage.ReadAsync(_access, null!, "ops", CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupUsage.ReadAsync(_access, "acme", null!, CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupUsage.ReadReferencesAsync(null!, "acme", "ops", CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupCascade.ReadAsync(null!, "acme", "ops", CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => TenantGroupCascade.ReadAsync(_access, "acme", null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    private static TenantRuleDraft Rule(string id, string group) => new()
    {
        RuleId = id,
        SubjectId = group,
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.TenantWide,
        Operations = LatticeOperation.Read,
        Effect = LatticeEffect.Allow,
    };

    private static TenantRuleView Seeded(string id, TenantRuleOrigin origin, string group) => new()
    {
        RuleId = id,
        Origin = origin,
        SubjectId = group,
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.Tree,
        TreeName = "orders",
        Operations = LatticeOperation.Read,
        Effect = LatticeEffect.Allow,
    };
}
