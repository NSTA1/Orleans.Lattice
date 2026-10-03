namespace Orleans.Lattice.Auth;

/// <summary>
/// Thrown by <see cref="ILatticeAuthorizationPolicyStore"/> when a direct write
/// (<see cref="ILatticeAuthorizationPolicyStore.PutRuleAsync"/>) or delete
/// (<see cref="ILatticeAuthorizationPolicyStore.RemoveRuleAsync"/>) targets a rule id
/// in the tenant-tier namespace (<see cref="LatticeTenantRuleIds.Prefix"/>) outside a
/// system-origin scope. Tenant-tier rules are authored only through the tenant
/// policy administration surface, which validates their subjects, scopes and
/// operations against the owning tenant before writing under system origin; a
/// direct write would bypass that confinement, so the store rejects it instead.
/// The rejection is fail-closed: nothing is read or written before this exception
/// is raised.
/// </summary>
/// <remarks>
/// <para>
/// To constrain what a tenant's subjects may do, author an operator rule whose id
/// is outside the <see cref="LatticeTenantRuleIds.Prefix"/> namespace: operator
/// rules are evaluated before the tenant layer and their verdict is final.
/// </para>
/// <para>
/// Derives from <see cref="ArgumentException"/> because the rejection is caused by a
/// caller-supplied rule id; the transport bindings map it to a client-facing
/// invalid-argument status rather than an internal error. It is raised and handled
/// in-process on the policy-store write path and is deliberately not an Orleans
/// serializable type.
/// </para>
/// </remarks>
public sealed class LatticeTenantOwnedRuleException : ArgumentException
{
    /// <summary>
    /// The tenant-tier rule id the rejected write or delete targeted. Empty on the
    /// parameterless / message-only constructors.
    /// </summary>
    public string RuleId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="RuleId"/>. Provided to satisfy the framework's
    /// exception-construction contract; the policy store uses the context-carrying
    /// factory method.
    /// </summary>
    public LatticeTenantOwnedRuleException()
    {
        RuleId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and an empty <see cref="RuleId"/>.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    public LatticeTenantOwnedRuleException(string message) : base(message)
    {
        RuleId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeTenantOwnedRuleException(string message, Exception innerException)
        : base(message, innerException)
    {
        RuleId = string.Empty;
    }

    private LatticeTenantOwnedRuleException(string message, string paramName, string ruleId)
        : base(message, paramName)
    {
        RuleId = ruleId;
    }

    /// <summary>
    /// Creates the exception the policy store raises for a rejected direct write or
    /// delete of the tenant-tier rule <paramref name="ruleId"/>.
    /// </summary>
    /// <param name="ruleId">The tenant-tier rule id that was targeted. Must not be <c>null</c>.</param>
    /// <param name="paramName">The name of the offending store parameter. Must not be <c>null</c>.</param>
    /// <returns>A configured <see cref="LatticeTenantOwnedRuleException"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="ruleId"/> or <paramref name="paramName"/> is <c>null</c>.</exception>
    public static LatticeTenantOwnedRuleException Rejected(string ruleId, string paramName)
    {
        ArgumentNullException.ThrowIfNull(ruleId);
        ArgumentNullException.ThrowIfNull(paramName);
        var message = $"The rule id '{ruleId}' is a tenant-tier rule (it starts with '{LatticeTenantRuleIds.Prefix}') "
            + "and cannot be written or deleted directly: tenant-tier rules are authored only through the tenant "
            + "policy administration surface, which confines them to the owning tenant. Author an operator rule with "
            + $"an id outside the '{LatticeTenantRuleIds.Prefix}' prefix instead; operator rules are evaluated first "
            + "and their verdict is final.";
        return new LatticeTenantOwnedRuleException(message, paramName, ruleId);
    }
}
