using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The default <see cref="ITenancyAvailability"/>, and the shared
/// <see cref="ExplorerPluginAccessGate"/> a tenancy plugin composes with its own
/// authorization check.
/// <para>
/// It answers "does this cluster serve tenancy, and can this caller reach it?"
/// from two independent signals, in cost order:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>The Explorer's own tenant-view seam.</b> When no
///     <see cref="IExplorerTenantSwitcher"/> is registered, or the registered one
///     reports <see cref="IExplorerTenantSwitcher.IsActive"/>
///     <see langword="false"/>, tenant scoping is not enabled for this head at
///     all - the same posture <c>NullExplorerTenantView</c> takes - so the
///     capability is absent and no call is made.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>The cluster's own answer.</b> The binding answers
///     <see cref="StatusCode.Unimplemented"/> for an optional facade the cluster
///     did not register, and a host serving no tenant-administration binding at
///     all answers it for every method. The client surfaces that as
///     <see cref="TenancyUnavailableException"/>, which is the signal that the
///     add-on is absent.
///     </description>
///   </item>
/// </list>
/// <para>
/// Every other fault folds into withheld facts rather than an error, so a probe
/// never throws into the shell. It reports <em>facts</em> and leaves the state
/// to <see cref="ExplorerPluginAccessContract"/>: a refusal it cannot classify
/// is "the grant was not shown", which the contract renders as a sign-in prompt
/// for an anonymous caller and as a denial for a signed-in one. Deciding that
/// here is how this probe used to tell a signed-out visitor that tenancy was
/// "not available for your account".
/// </para>
/// <para>
/// The probe is not cached: it is a single light read, and the host re-probes
/// only on mount, sign-in change, and reconnect - the same posture the Backups
/// capability probe takes, and it keeps the answer from going stale across a
/// reconnect.
/// </para>
/// </summary>
/// <param name="client">The transport seam used for the light probe read.</param>
/// <param name="switcher">
/// The Explorer's optional tenant-view switcher. <see langword="null"/> when the
/// head never called <c>AddExplorerTenantView()</c>, which is itself the
/// "no tenancy here" signal.
/// </param>
/// <param name="session">
/// The Explorer's sign-in seam, read only to tell an anonymous refusal from an
/// authenticated one. <see langword="null"/> on a head that registered no auth,
/// which reads as anonymous - the recoverable answer.
/// </param>
public sealed class TenancyAvailability(
    ITenantAdminClient client,
    IExplorerTenantSwitcher? switcher = null,
    IExplorerAuthSession? session = null)
    : ExplorerPluginAccessGate, ITenancyAvailability
{
    private const string InactiveReason =
        "This deployment does not have the tenancy add-on enabled.";

    private const string DeniedReason =
        "The tenancy control surface could not be reached for this caller.";

    /// <summary>
    /// The grant a refused caller is missing. Cached, so attaching it to a
    /// denial costs nothing per probe.
    /// </summary>
    private static readonly ExplorerAccessRemedy MissingGrant =
        ExplorerAccessRemedy.Requiring("Tenant read", "a platform administrator");

    private readonly ITenantAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    private readonly IExplorerTenantSwitcher? _switcher = switcher;

    private readonly IExplorerAuthSession? _session = session;

    /// <inheritdoc />
    public override ExplorerAccessRemedy Remedy => MissingGrant;

    /// <inheritdoc />
    protected override bool IsCallerAuthenticated => _session?.IsAuthenticated ?? false;

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAsync(CancellationToken cancellationToken = default)
    {
        var pending = EvaluateAvailabilityAsync(cancellationToken);
        return pending.IsCompletedSuccessfully
            ? new ValueTask<ExplorerPluginAccess>(Resolve(pending.Result))
            : ResolveAvailabilityAsync(pending);
    }

    /// <inheritdoc />
    protected override ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken) =>
        // The host's own tenant scope already says whether the deployment has the
        // tenancy add-on. Trusting it here saves a call and keeps a plugin's gate
        // consistent with the rest of the shell.
        context.Tenant.IsActive
            ? EvaluateAvailabilityAsync(cancellationToken)
            : new ValueTask<ExplorerPluginAccessFacts>(
                ExplorerPluginAccessFacts.CapabilityAbsent(InactiveReason));

    private async ValueTask<ExplorerPluginAccessFacts> EvaluateAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (_switcher is not { IsActive: true })
        {
            // Tenant scoping is not enabled for this head, so there is no tenancy
            // surface to render and nothing to ask the cluster about.
            return ExplorerPluginAccessFacts.CapabilityAbsent(InactiveReason);
        }

        try
        {
            // The cheapest read on the surface, and the one every caller is
            // entitled to: reaching it shows the surface exists and that this
            // caller can read it. Per-operation authorization stays the server's
            // job, and the plugins composing this probe add their own check.
            var current = await _client.GetCurrentTenantAsync(cancellationToken).ConfigureAwait(false);

            // A seam that answered nothing proved nothing. Fail closed rather
            // than read an absent response as an admission.
            return current is null
                ? ExplorerPluginAccessFacts.Withhold(DeniedReason)
                : ExplorerPluginAccessFacts.Granted;
        }
        catch (TenancyUnavailableException ex)
        {
            return ExplorerPluginAccessFacts.CapabilityAbsent(ex.Message);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            // The server refuses an anonymous caller and an authenticated but
            // unauthorized one with the same status, so this cannot be
            // classified here. Report the withheld grant and let the contract
            // pick the state from the caller's credential.
            return ExplorerPluginAccessFacts.Withhold(ex.Message);
        }
        catch (RpcException ex)
        {
            return Classify(ex);
        }
        catch (InvalidOperationException ex)
        {
            // The Explorer holds no endpoint yet. Fail closed; a later
            // connection-status change re-probes.
            return ExplorerPluginAccessFacts.Withhold(ex.Message);
        }
    }

    private async ValueTask<ExplorerPluginAccess> ResolveAvailabilityAsync(
        ValueTask<ExplorerPluginAccessFacts> pending) =>
        Resolve(await pending.ConfigureAwait(false));

    private ExplorerPluginAccess Resolve(in ExplorerPluginAccessFacts facts) =>
        ExplorerPluginAccessContract.Resolve(facts, MissingGrant, IsCallerAuthenticated);

    /// <summary>
    /// Classifies a residual transport fault the client did not translate.
    /// </summary>
    /// <remarks>
    /// <see cref="StatusCode.Unavailable"/> is deliberately <em>not</em>
    /// <see cref="ExplorerPluginCapabilityPresence.Absent"/>: on the wire it
    /// means the server could not be reached, which is transient, whereas an
    /// absent capability means it does not exist and never will here. Hiding the
    /// surface on a dropped connection would make a reconnect look like an
    /// uninstall.
    /// </remarks>
    private static ExplorerPluginAccessFacts Classify(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => ExplorerPluginAccessFacts.CapabilityAbsent(
            string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? "This cluster does not serve tenant administration."
                : exception.Status.Detail),
        StatusCode.Unauthenticated => ExplorerPluginAccessFacts.CredentialMissing(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? null : exception.Status.Detail),
        _ => ExplorerPluginAccessFacts.Withhold(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? DeniedReason : exception.Status.Detail),
    };
}
