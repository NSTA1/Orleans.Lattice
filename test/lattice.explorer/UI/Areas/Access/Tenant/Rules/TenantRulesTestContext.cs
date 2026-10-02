using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Data;
using Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The bUnit context the tenant Rules and Explain pages are tested under: the
/// Access context with tenancy on for <c>acme</c>, delegated administration
/// granted to the caller as acme's administrator, and, when a test asks for it,
/// the Data catalogue the tree field completes against.
/// </summary>
public abstract class TenantRulesTestContext : AccessTestContext
{
    /// <summary>The tenant every test administers.</summary>
    internal const string Acme = "acme";

    /// <summary>Turns tenancy on for acme and makes the caller its administrator.</summary>
    protected TenantRulesTestContext()
    {
        UseTenancy(Acme);
        TenantFacades.AsTenantAdmin();
    }

    /// <summary>
    /// Serves the Data catalogue with the trees <paramref name="stateIds"/> name,
    /// scoped to acme by the same ownership rule the real tenant view applies.
    /// </summary>
    /// <param name="stateIds">The physical tree ids the catalogue lists.</param>
    /// <returns>The in-memory state API, for seeding history.</returns>
    internal FakeStateClient UseTrees(params string[] stateIds)
    {
        var client = new FakeStateClient();
        foreach (var id in stateIds)
        {
            client.WithTree(id);
        }

        Services.AddSingleton<ILatticeStateClient>(client);
        Services.AddSingleton<IExplorerTenantView>(new FakeTenantView(Acme));
        Services.AddKeyedSingleton<ILatticeTenantGrantAdmin>(ShellFacades.Key, new FakeTenancyCluster());
        return client;
    }

    /// <summary>
    /// Puts a scripted policy in front of the fakes, so a named policy call can be
    /// made to fail every time; call it before the first render.
    /// </summary>
    /// <returns>The scripted facades, whose <see cref="ScriptedTenantAccessFacades.Faults"/> a test fills.</returns>
    internal ScriptedTenantAccessFacades Script()
    {
        var scripted = new ScriptedTenantAccessFacades(TenantFacades);
        Services.AddSingleton<ITenantAccessFacades>(scripted);
        return scripted;
    }

    /// <summary>Seeds a platform rule on one of acme's trees, listed read-only.</summary>
    internal TenantRuleView SeedPlatformRule(
        string id,
        string? tree,
        string subject,
        TenantSubjectKind kind = TenantSubjectKind.TenantGroup,
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Deny,
        TenantRuleScopeKind scope = TenantRuleScopeKind.Tree,
        string? keyOrPrefix = null,
        TenantRuleOrigin origin = TenantRuleOrigin.PlatformTree)
    {
        var rule = new TenantRuleView
        {
            RuleId = id,
            Layer = TenantRuleLayer.Platform,
            Origin = origin,
            SubjectId = subject,
            SubjectKind = kind,
            ScopeKind = scope,
            TreeName = tree,
            KeyOrPrefix = keyOrPrefix,
            Operations = operations,
            Effect = effect,
        };
        TenantFacades.PolicyFake.SeedPlatformRule(Acme, rule);
        return rule;
    }

    /// <summary>Stores one of acme's own rules directly in the fake, leaving no call in its log.</summary>
    internal TenantRuleView SeedTenantRule(
        string id,
        string? tree,
        string subject,
        TenantSubjectKind kind = TenantSubjectKind.TenantGroup,
        LatticeOperation operations = LatticeOperation.Read,
        LatticeEffect effect = LatticeEffect.Allow,
        TenantRuleScopeKind scope = TenantRuleScopeKind.Tree,
        string? keyOrPrefix = null)
    {
        var view = TenantFacades.PolicyFake.PutRuleAsync(Acme, new TenantRuleDraft
        {
            RuleId = id,
            SubjectId = subject,
            SubjectKind = kind,
            ScopeKind = scope,
            TreeName = tree,
            KeyOrPrefix = keyOrPrefix,
            Operations = operations,
            Effect = effect,
        }).GetAwaiter().GetResult();
        TenantFacades.Gate.Calls.Clear();
        return view;
    }
}
