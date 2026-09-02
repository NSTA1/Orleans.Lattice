namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// What a gate <em>observed</em>, as opposed to what the shell should render.
/// <para>
/// This is the input half of the four-state contract. A gate answers three
/// separable facts - does the cluster serve the capability, did the caller
/// present a credential, and does the caller hold the grant - and
/// <see cref="ExplorerPluginAccessContract"/> turns them into the one state the
/// shell renders. Splitting the two is what stops each plugin re-deriving the
/// ordering for itself and arriving at a different reading of the same contract.
/// </para>
/// <para>
/// <c>default</c> is "the capability is present, the credential is unknown, and
/// the caller does not hold the grant", which resolves to
/// <see cref="ExplorerPluginAccessState.Denied"/> for a signed-in caller and
/// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> for an
/// anonymous one. Both fail closed, and neither hides a surface that exists.
/// </para>
/// </summary>
public readonly record struct ExplorerPluginAccessFacts
{
    /// <summary>
    /// The caller holds the grant. The only facts that can resolve to
    /// <see cref="ExplorerPluginAccessState.Allowed"/>.
    /// </summary>
    public static ExplorerPluginAccessFacts Granted { get; } =
        new(ExplorerPluginCapabilityPresence.Present, ExplorerPluginCallerAuthentication.Unknown, true, null);

    /// <summary>
    /// The capability exists but the caller does not hold the grant. Identical
    /// to <c>default</c>.
    /// </summary>
    public static ExplorerPluginAccessFacts Withheld { get; }

    private ExplorerPluginAccessFacts(
        ExplorerPluginCapabilityPresence capability,
        ExplorerPluginCallerAuthentication authentication,
        bool isGranted,
        string? explanation)
    {
        Capability = capability;
        Authentication = authentication;
        IsGranted = isGranted;
        Explanation = explanation;
    }

    /// <summary>Whether the cluster serves the capability the plugin surfaces.</summary>
    public ExplorerPluginCapabilityPresence Capability { get; }

    /// <summary>What the probe learned about the caller's credential.</summary>
    public ExplorerPluginCallerAuthentication Authentication { get; }

    /// <summary>
    /// Whether the caller demonstrably holds the grant the plugin's operations
    /// need. <see langword="false"/> by default, so an unproven grant is never
    /// an admission.
    /// </summary>
    public bool IsGranted { get; }

    /// <summary>
    /// The probe's own advisory explanation - typically a cached constant, or a
    /// message the server supplied - or <see langword="null"/>. Display text
    /// only; never parsed.
    /// </summary>
    public string? Explanation { get; }

    /// <summary>
    /// The caller holds the grant, optionally with an explanation.
    /// </summary>
    /// <param name="explanation">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccessFacts Grant(string? explanation = null) =>
        explanation is null
            ? Granted
            : new ExplorerPluginAccessFacts(
                ExplorerPluginCapabilityPresence.Present,
                ExplorerPluginCallerAuthentication.Unknown,
                true,
                explanation);

    /// <summary>
    /// The capability exists but the caller was not shown to hold the grant.
    /// Whether that renders as a denial or as a sign-in prompt is the contract's
    /// decision, not the gate's.
    /// </summary>
    /// <param name="explanation">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccessFacts Withhold(string? explanation = null) =>
        explanation is null
            ? Withheld
            : new ExplorerPluginAccessFacts(
                ExplorerPluginCapabilityPresence.Present,
                ExplorerPluginCallerAuthentication.Unknown,
                false,
                explanation);

    /// <summary>
    /// The cluster does not serve the capability at all, so no credential and no
    /// grant could change the answer.
    /// </summary>
    /// <param name="explanation">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccessFacts CapabilityAbsent(string? explanation = null) =>
        new(
            ExplorerPluginCapabilityPresence.Absent,
            ExplorerPluginCallerAuthentication.Unknown,
            false,
            explanation);

    /// <summary>
    /// The probe established that no accepted credential was presented, so the
    /// refusal is recoverable by signing in.
    /// </summary>
    /// <param name="explanation">The advisory explanation, or <see langword="null"/>.</param>
    public static ExplorerPluginAccessFacts CredentialMissing(string? explanation = null) =>
        new(
            ExplorerPluginCapabilityPresence.Present,
            ExplorerPluginCallerAuthentication.Anonymous,
            false,
            explanation);

    /// <summary>
    /// Re-reads an already-resolved decision as facts, so a gate that composes a
    /// shared availability probe with its own authorization check can narrow
    /// that answer without re-deriving the ordering. Never widens: an
    /// <see cref="ExplorerPluginAccessState.Allowed"/> input becomes granted
    /// facts, and every other state keeps its meaning.
    /// </summary>
    /// <param name="access">The decision to re-read.</param>
    /// <returns>The equivalent facts.</returns>
    public static ExplorerPluginAccessFacts From(ExplorerPluginAccess access) => access.State switch
    {
        ExplorerPluginAccessState.Allowed => Grant(access.Reason),
        ExplorerPluginAccessState.Unavailable => CapabilityAbsent(access.Reason),
        ExplorerPluginAccessState.AuthenticationRequired => CredentialMissing(access.Reason),
        _ => Withhold(access.Reason),
    };

    /// <summary>
    /// Returns the same facts with <paramref name="authentication"/> recorded,
    /// for a gate that learns the credential state separately from the grant.
    /// </summary>
    /// <param name="authentication">What the probe learned about the credential.</param>
    public ExplorerPluginAccessFacts WithAuthentication(ExplorerPluginCallerAuthentication authentication) =>
        authentication == Authentication
            ? this
            : new ExplorerPluginAccessFacts(Capability, authentication, IsGranted, Explanation);
}
