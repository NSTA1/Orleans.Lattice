using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The outcome of compiling a manifest's roles into authorization rules. Either the complete
/// owned rule set (<see cref="Succeeded"/>) or an activation failure that lists every ceiling
/// excess, unknown-role binding and tenant-mismatched binding; a failure never carries a partially
/// granted rule set.
/// </summary>
public sealed class AppRuleCompilation
{
    internal AppRuleCompilation(
        IReadOnlyList<LatticeAuthorizationRule> rules,
        IReadOnlyList<AppCeilingExcess> excesses,
        IReadOnlyList<AppRoleBinding> unknownRoleBindings,
        IReadOnlyList<string> unboundRoles,
        IReadOnlyList<AppRoleBinding> tenantMismatchBindings)
    {
        Rules = rules;
        Excesses = excesses;
        UnknownRoleBindings = unknownRoleBindings;
        UnboundRoles = unboundRoles;
        TenantMismatchBindings = tenantMismatchBindings;
    }

    /// <summary>
    /// <c>true</c> when there are no ceiling excesses, no unknown-role bindings and no
    /// tenant-mismatched bindings.
    /// </summary>
    public bool Succeeded => Excesses.Count == 0 && UnknownRoleBindings.Count == 0 && TenantMismatchBindings.Count == 0;

    /// <summary>
    /// On success, the whole owned rule set ordered by ordinal rule id, each an unconditional
    /// <see cref="LatticeEffect.Allow"/> rule for a group subject. Empty on failure.
    /// </summary>
    public IReadOnlyList<LatticeAuthorizationRule> Rules { get; }

    /// <summary>Every way the manifest's roles exceed the ceiling, in manifest order; empty when within it.</summary>
    public IReadOnlyList<AppCeilingExcess> Excesses { get; }

    /// <summary>Bindings naming a role the manifest does not declare, in binding order. Any entry fails compilation.</summary>
    public IReadOnlyList<AppRoleBinding> UnknownRoleBindings { get; }

    /// <summary>
    /// Bindings naming a group in the reserved tenant-group namespace (<c>t/...</c>) that is not a
    /// group of the installing tenant, in binding order: another tenant's group, or an id in that
    /// namespace that is not a well-formed tenant group id. Any entry fails compilation, so a binding
    /// can never confer an app role on another tenant's members.
    /// </summary>
    public IReadOnlyList<AppRoleBinding> TenantMismatchBindings { get; }

    /// <summary>
    /// Diagnostic only: declared roles with no binding, in manifest order. They emit no rules and do
    /// not fail compilation, but are still checked against the ceiling.
    /// </summary>
    public IReadOnlyList<string> UnboundRoles { get; }
}
