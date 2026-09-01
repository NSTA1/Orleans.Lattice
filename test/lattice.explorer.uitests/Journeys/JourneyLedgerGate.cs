using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// An area whose access gate answers the shipped four-state contract from the
/// caller's real credential, so the composed shell's rendering of
/// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> versus
/// <see cref="ExplorerPluginAccessState.Denied"/> can be observed in a browser.
/// <para>
/// <b>Why the shipped areas cannot show this.</b> Every area the product ships probes
/// a cluster. The journey head has none, so each of those probes throws a transport
/// fault, and <c>ExplorerPluginAccessRefresher</c> deliberately contains a faulting
/// gate at <c>Deny(ex.Message)</c> - documented, intended, fail-closed behaviour. The
/// consequence is that in <i>any</i> cluster-free head every shipped area renders
/// Denied whether or not anyone is signed in, which makes the epic's first-run promise
/// ("tell me this needs a sign-in, not that my account lacks a grant") unobservable
/// there. That is a property of the harness, not a regression of the gate fix.
/// </para>
/// <para>
/// <b>What this restores.</b> This gate derives from the real
/// <see cref="ExplorerPluginAccessGate"/> and reports <i>facts</i> exactly as the
/// contract asks - it never decides a state - so the ordering, the credential rule and
/// the remedy all come from shipped code. Only the fact source is a double, and it is
/// a fact source that cannot throw. The result is three genuinely different renderings
/// of one area:
/// </para>
/// <list type="bullet">
///   <item>anonymous - the contract resolves AuthenticationRequired, and the rail must
///   keep the area prominent and clickable so it opens the sign-in;</item>
///   <item><see cref="JourneyWorld.DataReader"/> - Denied, and the rail must demote it
///   below the divider with the gate's own remedy;</item>
///   <item><see cref="JourneyWorld.PlatformAdmin"/> - Allowed, and the area opens.</item>
/// </list>
/// </summary>
/// <param name="session">The circuit's authentication session, the credential fact's real source.</param>
internal sealed class JourneyLedgerGate(IExplorerAuthSession session) : ExplorerPluginAccessGate
{
    /// <summary>The permission this area's denial names.</summary>
    internal const string Permission = "Ledger";

    /// <summary>The audience this area's denial tells the caller to ask.</summary>
    internal const string Audience = "an operator";

    private static readonly ExplorerAccessRemedy Required =
        ExplorerAccessRemedy.Requiring(Permission, Audience);

    private readonly IExplorerAuthSession _session =
        session ?? throw new ArgumentNullException(nameof(session));

    /// <inheritdoc />
    public override ExplorerAccessRemedy Remedy => Required;

    /// <inheritdoc />
    protected override bool IsCallerAuthenticated => _session.IsAuthenticated;

    /// <inheritdoc />
    /// <remarks>
    /// Reports only what was observed: the grant is held by the operator identity and
    /// withheld from everyone else. Whether a withheld grant means "sign in" or "you
    /// are denied" is the contract's decision, not this gate's - which is the whole
    /// point of the base class.
    /// </remarks>
    protected override ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken) =>
        ValueTask.FromResult(
            _session.IsAuthenticated && JourneyWorld.IsOperator(_session.Username)
                ? ExplorerPluginAccessFacts.Granted
                : ExplorerPluginAccessFacts.Withheld);
}
