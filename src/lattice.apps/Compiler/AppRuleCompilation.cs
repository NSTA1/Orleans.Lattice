using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The outcome of compiling a manifest's roles into authorization rules. Either the complete
/// owned rule set (<see cref="Succeeded"/>) or an activation failure that lists every ceiling
/// excess and unknown-role binding; a failure never carries a partially granted rule set.
/// </summary>
public sealed class AppRuleCompilation
{
    internal AppRuleCompilation(
        IReadOnlyList<LatticeAuthorizationRule> rules,
        IReadOnlyList<AppCeilingExcess> excesses,
        IReadOnlyList<AppRoleBinding> unknownRoleBindings,
        IReadOnlyList<string> unboundRoles)
    {
        Rules = rules;
        Excesses = excesses;
        UnknownRoleBindings = unknownRoleBindings;
        UnboundRoles = unboundRoles;
    }

    /// <summary><c>true</c> when there are no ceiling excesses and no unknown-role bindings.</summary>
    public bool Succeeded => Excesses.Count == 0 && UnknownRoleBindings.Count == 0;

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
    /// Diagnostic only: declared roles with no binding, in manifest order. They emit no rules and do
    /// not fail compilation, but are still checked against the ceiling.
    /// </summary>
    public IReadOnlyList<string> UnboundRoles { get; }
}
