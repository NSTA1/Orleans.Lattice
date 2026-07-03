namespace Orleans.Lattice.Auth;

/// <summary>
/// The decision an authorization rule contributes when it matches a request:
/// grant access or forbid it. A matching <see cref="Deny"/> rule takes
/// precedence over a matching <see cref="Allow"/> rule under the deny-overrides
/// combination the decision engine applies (implemented by a later feature).
/// </summary>
public enum LatticeEffect
{
    /// <summary>The rule grants the covered operations to the selected subject.</summary>
    Allow = 0,

    /// <summary>The rule forbids the covered operations for the selected subject.</summary>
    Deny = 1,
}
