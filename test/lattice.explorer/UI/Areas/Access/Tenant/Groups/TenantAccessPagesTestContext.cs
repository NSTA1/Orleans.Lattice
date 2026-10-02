using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The bUnit context the tenant Groups and Members pages are tested under: the
/// Access area's context, at the tenant <c>acme</c>, with the history reader and
/// the tenant access facade (the administrators' reader) faked, so no page dials
/// a transport. Delegated administration is off until a test turns it on.
/// </summary>
public abstract class TenantAccessPagesTestContext : AccessTestContext
{
    /// <summary>Registers the fakes over the Access area's context.</summary>
    protected TenantAccessPagesTestContext()
    {
        History = Substitute.For<IHistoryReader>();
        History.LoadAsync(default!, default!, default, default, default).ReturnsForAnyArgs(new HistoryPage());
        TenantAccessAdmin = Substitute.For<ILatticeTenantAccessAdmin>();
        TenantAccessAdmin.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(call =>
            new TenantAdminSubjectReport { TenantId = call.Arg<string>(), Subjects = ["ops@example.com"] });
        Services.AddSingleton(History);
        Services.AddKeyedSingleton(ShellFacades.Key, TenantAccessAdmin);

        // A seam over the Access context's fakes whose directory a test can swap for a scripted one.
        Facades = new ScriptedTenantAccessFacades(TenantFacades);
        Services.AddSingleton<ITenantAccessFacades>(Facades);
    }

    /// <summary>The tenant facade seam the pages read: the Access context's fakes, or a scripted directory.</summary>
    internal ScriptedTenantAccessFacades Facades { get; }

    /// <summary>Every notice the circuit raised, in order.</summary>
    internal IReadOnlyList<string> ToastMessages =>
        [.. Services.GetRequiredService<LtToastService>().Toasts.Select(toast => toast.Message)];

    /// <summary>The history reader the group page reads its history through.</summary>
    internal IHistoryReader History { get; }

    /// <summary>The tenant access facade, whose administrator entries the Members page lists.</summary>
    internal ILatticeTenantAccessAdmin TenantAccessAdmin { get; }

    /// <summary>How many times an address was declared not found.</summary>
    protected int NotFound { get; private set; }

    /// <summary>Roots the circuit at the tenant <c>acme</c> and counts not-found declarations.</summary>
    [SetUp]
    public void UseAcme()
    {
        UseTenancy("acme");
        Navigation.OnNotFound += (_, _) => NotFound++;
    }

    /// <summary>Scripts the tenant directory the pages read, in place of the fake; the fake policy still answers the posture probe.</summary>
    /// <param name="directory">The scripted tenant directory.</param>
    internal void UseDirectory(ILatticeTenantDirectoryAdmin directory) => Facades.DirectoryOverride = directory;

    /// <summary>A tenant rule of <c>acme</c> naming the tenant group <paramref name="group"/>.</summary>
    /// <param name="id">The rule's local id.</param>
    /// <param name="group">The group's local name.</param>
    internal Task NameInTenantRuleAsync(string id, string group) =>
        TenantFacades.PolicyFake.PutRuleAsync("acme", new TenantRuleDraft
        {
            RuleId = id,
            SubjectId = group,
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.TenantWide,
            Operations = Orleans.Lattice.LatticeOperation.Read,
            Effect = Orleans.Lattice.Auth.LatticeEffect.Allow,
        });

    /// <summary>An app role binding of <c>acme</c> naming the tenant group <paramref name="group"/>.</summary>
    /// <param name="id">The binding's rule id.</param>
    /// <param name="group">The group's local name.</param>
    internal void NameInAppRole(string id, string group) =>
        TenantFacades.PolicyFake.SeedPlatformRule("acme", new TenantRuleView
        {
            RuleId = id,
            Origin = TenantRuleOrigin.AppRole,
            SubjectId = group,
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.Tree,
            TreeName = "a/crm/orders",
            Operations = Orleans.Lattice.LatticeOperation.Read,
            Effect = Orleans.Lattice.Auth.LatticeEffect.Allow,
        });

    /// <summary>A platform rule on <c>acme</c>'s tree <c>orders</c> naming the tenant group <paramref name="group"/>.</summary>
    /// <param name="id">The rule id.</param>
    /// <param name="group">The group's local name.</param>
    internal void NameInPlatformRule(string id, string group) =>
        TenantFacades.PolicyFake.SeedPlatformRule("acme", new TenantRuleView
        {
            RuleId = id,
            Origin = TenantRuleOrigin.PlatformTree,
            SubjectId = group,
            SubjectKind = TenantSubjectKind.TenantGroup,
            ScopeKind = TenantRuleScopeKind.Tree,
            TreeName = "orders",
            Operations = Orleans.Lattice.LatticeOperation.Read,
            Effect = Orleans.Lattice.Auth.LatticeEffect.Deny,
        });

    /// <summary>Adds <paramref name="member"/> to the group <paramref name="group"/> of <c>acme</c>.</summary>
    /// <param name="group">The group's local name.</param>
    /// <param name="member">The member's id.</param>
    /// <param name="kind">The member's kind.</param>
    internal async Task AddGroupMemberAsync(string group, string member, TenantSubjectKind kind = TenantSubjectKind.User)
    {
        var enabled = TenantFacades.Gate.Enabled;
        TenantFacades.Gate.Enabled = true;
        await TenantFacades.DirectoryFake.AddGroupMemberAsync("acme", group, member, kind);
        TenantFacades.Gate.Enabled = enabled;
        TenantFacades.Gate.Calls.Clear();
    }
}
