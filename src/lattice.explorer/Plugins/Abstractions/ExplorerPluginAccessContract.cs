namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The one place the Explorer's four-state access contract is decided.
/// <para>
/// Before this existed each plugin gate mapped its own probe onto
/// <see cref="ExplorerPluginAccessState"/> by hand, and the gates disagreed:
/// two told an <em>anonymous</em> visitor a surface "is not available for your
/// account" where the honest answer was "sign in", and one reported
/// <see cref="ExplorerPluginAccessState.Allowed"/> to a caller holding no grant
/// at all, inviting them into a surface that would refuse them from the server.
/// A shell cannot render a policy over a state that was computed wrongly, so the
/// ordering is stated once, here, and every gate resolves through it.
/// </para>
/// <para>
/// <b>The contract, in precedence order.</b> Each rule is checked only when the
/// ones above it did not apply:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>The cluster does not serve the capability</b>
///     (<see cref="ExplorerPluginCapabilityPresence.Absent"/>) yields
///     <see cref="ExplorerPluginAccessState.Unavailable"/>. It is first because
///     no credential and no grant can change it: offering a sign-in, or naming a
///     permission to request, would both be false advice.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>The caller demonstrably holds the grant</b>
///     (<see cref="ExplorerPluginAccessFacts.IsGranted"/>) yields
///     <see cref="ExplorerPluginAccessState.Allowed"/>. The converse is the
///     load-bearing half: a gate that cannot show the grant never reaches this
///     rule, so "the call did not fail" is not by itself an admission.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>The caller presented no accepted credential</b> yields
///     <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> - never
///     <see cref="ExplorerPluginAccessState.Denied"/>. An anonymous visitor has
///     no account to be refused for, so a denial would state something untrue
///     about them and hide the one action that would help. This holds whether
///     the probe observed the anonymity itself
///     (<see cref="ExplorerPluginCallerAuthentication.Anonymous"/>) or the shell
///     supplies it through <c>isCallerAuthenticated</c>.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Otherwise</b> the caller is signed in and lacks the grant, which is
///     <see cref="ExplorerPluginAccessState.Denied"/> - carrying the gate's
///     remedy, so the entry can say which permission is missing and who issues
///     it rather than only that it is unavailable.
///     </description>
///   </item>
/// </list>
/// <para>
/// <b>This is advisory.</b> The server remains the sole enforcement point, so a
/// resolved state is a usability affordance and never a security control:
/// every plugin action must still handle a runtime refusal, and no caller gains
/// anything by reaching a surface this marked reachable.
/// </para>
/// </summary>
public static class ExplorerPluginAccessContract
{
    /// <summary>
    /// Resolves <paramref name="facts"/> into the state the shell renders.
    /// <para>
    /// Allocation-free: every returned value is either a cached static or a
    /// two-field struct built from references the caller already holds, so a
    /// gate resolving once per nav entry per render allocates nothing.
    /// </para>
    /// </summary>
    /// <param name="facts">What the gate's probe observed.</param>
    /// <param name="remedy">
    /// The gate's own remedy, attached to a denial so the entry can state which
    /// grant is missing and who issues it. Ignored for every other state.
    /// </param>
    /// <param name="isCallerAuthenticated">
    /// The shell's own sign-in state, consulted only when the probe reported
    /// <see cref="ExplorerPluginCallerAuthentication.Unknown"/> - which is the
    /// common case, because a server answers the same status to an anonymous
    /// caller and to an authenticated one it refuses.
    /// </param>
    /// <returns>The resolved decision.</returns>
    public static ExplorerPluginAccess Resolve(
        in ExplorerPluginAccessFacts facts,
        in ExplorerAccessRemedy remedy,
        bool isCallerAuthenticated)
    {
        if (facts.Capability == ExplorerPluginCapabilityPresence.Absent)
        {
            return ExplorerPluginAccess.ReportUnavailable(facts.Explanation);
        }

        if (facts.IsGranted)
        {
            return ExplorerPluginAccess.Allow(facts.Explanation);
        }

        return IsAnonymous(facts, isCallerAuthenticated)
            ? ExplorerPluginAccess.RequireAuthentication(facts.Explanation)
            : ExplorerPluginAccess.Deny(facts.Explanation, remedy);
    }

    /// <summary>
    /// Whether the refused caller presented no accepted credential. What the
    /// probe observed wins over the shell's view, because a probe that saw an
    /// <c>Unauthenticated</c> status knows the credential never reached the
    /// server even if the shell believes one is applied.
    /// </summary>
    /// <param name="facts">What the gate's probe observed.</param>
    /// <param name="isCallerAuthenticated">The shell's own sign-in state.</param>
    private static bool IsAnonymous(in ExplorerPluginAccessFacts facts, bool isCallerAuthenticated) =>
        facts.Authentication switch
        {
            ExplorerPluginCallerAuthentication.Anonymous => true,
            ExplorerPluginCallerAuthentication.Authenticated => false,
            _ => !isCallerAuthenticated,
        };
}
