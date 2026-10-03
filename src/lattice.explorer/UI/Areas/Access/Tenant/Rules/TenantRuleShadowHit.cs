using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>One platform rule that decides a tenant rule's scope first.</summary>
/// <param name="Rule">The platform rule.</param>
/// <param name="Operations">The operations of the tenant rule it decides.</param>
/// <param name="Partial">Whether it decides only some of the tenant rule's operations.</param>
internal sealed record TenantRuleShadowHit(TenantRuleView Rule, LatticeOperation Operations, bool Partial);
