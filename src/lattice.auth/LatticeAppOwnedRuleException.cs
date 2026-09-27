namespace Orleans.Lattice.Auth;

/// <summary>
/// Thrown by <see cref="ILatticeAuthorizationPolicyStore"/> when a direct write
/// (<see cref="ILatticeAuthorizationPolicyStore.PutRuleAsync"/>) or delete
/// (<see cref="ILatticeAuthorizationPolicyStore.RemoveRuleAsync"/>) targets a rule id
/// in the app-owned namespace (<see cref="LatticeAppRuleIds.Prefix"/>) outside a
/// system-origin scope. App-owned rules are the compiled projection of an installed
/// app's manifest and are replaced wholesale by the app compiler on every
/// reconciliation, so an operator edit would be silently discarded; the store
/// rejects it instead. The rejection is fail-closed: nothing is read or written
/// before this exception is raised.
/// </summary>
/// <remarks>
/// <para>
/// To change what an installed app's principals may do, author a separate rule
/// whose id is outside the <see cref="LatticeAppRuleIds.Prefix"/> namespace (for
/// example a more specific deny, which the decision engine's deny-over-allow
/// precedence honours), or change the app's manifest or role bindings and let the
/// compiler reconcile.
/// </para>
/// <para>
/// Derives from <see cref="ArgumentException"/> because the rejection is caused by a
/// caller-supplied rule id; the transport bindings map it to a client-facing
/// invalid-argument status rather than an internal error. It is raised and handled
/// in-process on the policy-store write path and is deliberately not an Orleans
/// serializable type.
/// </para>
/// </remarks>
public sealed class LatticeAppOwnedRuleException : ArgumentException
{
    /// <summary>
    /// The app-owned rule id the rejected write or delete targeted. Empty on the
    /// parameterless / message-only constructors.
    /// </summary>
    public string RuleId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="RuleId"/>. Provided to satisfy the framework's
    /// exception-construction contract; the policy store uses the context-carrying
    /// factory method.
    /// </summary>
    public LatticeAppOwnedRuleException()
    {
        RuleId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and an empty <see cref="RuleId"/>.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    public LatticeAppOwnedRuleException(string message) : base(message)
    {
        RuleId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeAppOwnedRuleException(string message, Exception innerException)
        : base(message, innerException)
    {
        RuleId = string.Empty;
    }

    private LatticeAppOwnedRuleException(string message, string paramName, string ruleId)
        : base(message, paramName)
    {
        RuleId = ruleId;
    }

    /// <summary>
    /// Creates the exception the policy store raises for a rejected direct write or
    /// delete of the app-owned rule <paramref name="ruleId"/>.
    /// </summary>
    /// <param name="ruleId">The app-owned rule id that was targeted. Must not be <c>null</c>.</param>
    /// <param name="paramName">The name of the offending store parameter. Must not be <c>null</c>.</param>
    /// <returns>A configured <see cref="LatticeAppOwnedRuleException"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="ruleId"/> or <paramref name="paramName"/> is <c>null</c>.</exception>
    public static LatticeAppOwnedRuleException Rejected(string ruleId, string paramName)
    {
        ArgumentNullException.ThrowIfNull(ruleId);
        ArgumentNullException.ThrowIfNull(paramName);
        var message = $"The rule id '{ruleId}' is owned by an installed app (it starts with '{LatticeAppRuleIds.Prefix}') "
            + "and cannot be written or deleted directly: app-owned rules are replaced by the app compiler on every "
            + "reconciliation, so a direct edit would be discarded. Author a separate rule with an id outside the "
            + $"'{LatticeAppRuleIds.Prefix}' prefix instead, or change the app's manifest or role bindings.";
        return new LatticeAppOwnedRuleException(message, paramName, ruleId);
    }
}
