namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The base every plugin access gate derives from: it owns the four-state
/// <em>ordering</em> so a plugin only has to answer what it actually observed.
/// <para>
/// Each gate used to map its own probe onto
/// <see cref="ExplorerPluginAccessState"/> by hand, and the three that did it
/// most carefully still disagreed - two answered
/// <see cref="ExplorerPluginAccessState.Denied"/> to an anonymous visitor where
/// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> was correct,
/// and one answered <see cref="ExplorerPluginAccessState.Allowed"/> to a caller
/// holding no grant. Deriving from this class removes the opportunity: the
/// ordering lives once in <see cref="ExplorerPluginAccessContract"/>, and a
/// plugin supplies only <see cref="EvaluateAsync"/>,
/// <see cref="IsCallerAuthenticated"/>, and <see cref="Remedy"/>.
/// </para>
/// <para>
/// <see cref="ProbeAsync"/> is deliberately not <see langword="virtual"/>: a
/// gate that could override it could re-litigate the ordering, which is the
/// whole defect. It also avoids an <see langword="async"/> state machine when
/// <see cref="EvaluateAsync"/> answers synchronously, so a gate with a cached or
/// fixed answer costs nothing on the render path.
/// </para>
/// <para>
/// Gating is advisory. The server remains the sole enforcement point, so a
/// decision resolved here is a usability affordance and every plugin action must
/// still handle a runtime refusal.
/// </para>
/// </summary>
public abstract class ExplorerPluginAccessGate : IExplorerPluginAccessGate
{
    /// <summary>
    /// What a refused caller should do about it: the grant they are missing and
    /// who issues it. Attached to every denial this gate resolves, so a demoted
    /// entry can state a remedy instead of only that it is unavailable.
    /// <para>
    /// Declare it as a cached <see langword="static"/> and return that, so
    /// carrying a remedy costs no allocation per probe.
    /// </para>
    /// </summary>
    public abstract ExplorerAccessRemedy Remedy { get; }

    /// <summary>
    /// Whether the shell currently holds an accepted credential.
    /// <para>
    /// Consulted only when <see cref="EvaluateAsync"/> could not tell - which is
    /// the usual case, because a server answers the same refusal to an anonymous
    /// caller and to an authenticated one it denies. Read from the plugin's own
    /// sign-in seam; a gate that has none must not simply return
    /// <see langword="true"/>, because that turns every anonymous refusal back
    /// into the denial this contract exists to prevent.
    /// </para>
    /// </summary>
    protected abstract bool IsCallerAuthenticated { get; }

    /// <inheritdoc />
    /// <remarks>
    /// Sealed in effect: the state mapping is
    /// <see cref="ExplorerPluginAccessContract.Resolve"/> and nothing else.
    /// </remarks>
    public ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var pending = EvaluateAsync(context, cancellationToken);

        // A synchronously-answered probe - a fixed decision, or one short-
        // circuited on an ambient fact - never builds a state machine. The gate
        // resolves once per rendered nav entry per render, so that is the case
        // worth keeping free.
        return pending.IsCompletedSuccessfully
            ? new ValueTask<ExplorerPluginAccess>(Resolve(pending.Result))
            : ResolveAsync(pending);
    }

    /// <summary>
    /// Reports what this gate's probe observed: whether the cluster serves the
    /// capability, whether the caller holds the grant, and - when it can tell -
    /// whether a credential was presented.
    /// <para>
    /// Report facts, not states. In particular do not decide here that a refusal
    /// is a denial rather than a sign-in prompt: return
    /// <see cref="ExplorerPluginAccessFacts.Withhold"/> and let the contract
    /// decide, because only it knows the caller's credential state.
    /// </para>
    /// <para>
    /// Must not throw: the host contains a faulting gate at
    /// <see cref="ExplorerPluginAccess.Denied"/>, which loses the distinction
    /// this contract exists to preserve. Fold a transport fault into facts
    /// instead.
    /// </para>
    /// </summary>
    /// <param name="context">
    /// The probing plugin's own host context, bound to its plugin id. Never
    /// <see langword="null"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>What the probe observed.</returns>
    protected abstract ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken);

    private async ValueTask<ExplorerPluginAccess> ResolveAsync(ValueTask<ExplorerPluginAccessFacts> pending) =>
        Resolve(await pending.ConfigureAwait(false));

    private ExplorerPluginAccess Resolve(in ExplorerPluginAccessFacts facts) =>
        ExplorerPluginAccessContract.Resolve(facts, Remedy, IsCallerAuthenticated);
}
