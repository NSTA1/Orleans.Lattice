using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The default <see cref="ITenancyAvailability"/>, and a ready-made
/// <see cref="IExplorerPluginAccessGate"/> a tenancy plugin composes with its
/// own authorization check.
/// <para>
/// It answers "does this cluster serve tenancy?" from two independent signals,
/// in cost order:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>The Explorer's own tenant-view seam.</b> When no
///     <see cref="IExplorerTenantSwitcher"/> is registered, or the registered one
///     reports <see cref="IExplorerTenantSwitcher.IsActive"/>
///     <see langword="false"/>, tenant scoping is not enabled for this head at
///     all - the same posture <c>NullExplorerTenantView</c> takes - so the
///     surface is unavailable and no call is made.
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
/// Every other fault fails closed to a denial rather than an error, so a probe
/// never throws into the shell. The probe is not cached: it is a single light
/// read, and the host re-probes only on mount, sign-in change, and reconnect -
/// the same posture the Backups capability probe takes, and it keeps the answer
/// from going stale across a reconnect.
/// </para>
/// </summary>
/// <param name="client">The transport seam used for the light probe read.</param>
/// <param name="switcher">
/// The Explorer's optional tenant-view switcher. <see langword="null"/> when the
/// head never called <c>AddExplorerTenantView()</c>, which is itself the
/// "no tenancy here" signal.
/// </param>
public sealed class TenancyAvailability(
    ITenantAdminClient client,
    IExplorerTenantSwitcher? switcher = null) : ITenancyAvailability, IExplorerPluginAccessGate
{
    private const string InactiveReason =
        "This deployment does not have the tenancy add-on enabled.";

    private const string DeniedReason =
        "The tenancy control surface could not be reached for this caller.";

    private readonly ITenantAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    private readonly IExplorerTenantSwitcher? _switcher = switcher;

    /// <inheritdoc />
    public async ValueTask<ExplorerPluginAccess> ProbeAsync(CancellationToken cancellationToken = default)
    {
        if (_switcher is not { IsActive: true })
        {
            // Tenant scoping is not enabled for this head, so there is no tenancy
            // surface to render and nothing to ask the cluster about.
            return ExplorerPluginAccess.ReportUnavailable(InactiveReason);
        }

        try
        {
            // The cheapest read on the surface, and the one every caller is
            // entitled to: it requires no special authorization, so reaching it
            // proves the surface exists rather than proving the caller is
            // privileged. Per-operation authorization stays the server's job.
            await _client.GetCurrentTenantAsync(cancellationToken).ConfigureAwait(false);
            return ExplorerPluginAccess.Allowed;
        }
        catch (TenancyUnavailableException ex)
        {
            return ExplorerPluginAccess.ReportUnavailable(ex.Message);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return ExplorerPluginAccess.Deny(ex.Message);
        }
        catch (RpcException ex)
        {
            return Classify(ex);
        }
        catch (InvalidOperationException ex)
        {
            // The Explorer holds no endpoint yet. Fail closed; a later
            // connection-status change re-probes.
            return ExplorerPluginAccess.Deny(ex.Message);
        }
    }

    /// <summary>
    /// Probes on behalf of a plugin, short-circuiting to unavailable when the
    /// host has already resolved the deployment as non-tenant.
    /// </summary>
    /// <param name="context">The probing plugin's own host context. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The resolved access decision.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> is <see langword="null"/>.</exception>
    public ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        // The host's own tenant scope already says whether the deployment has the
        // tenancy add-on. Trusting it here saves a call and keeps a plugin's gate
        // consistent with the rest of the shell.
        return context.Tenant.IsActive
            ? ProbeAsync(cancellationToken)
            : new ValueTask<ExplorerPluginAccess>(ExplorerPluginAccess.ReportUnavailable(InactiveReason));
    }

    /// <summary>
    /// Classifies a residual transport fault the client did not translate.
    /// </summary>
    /// <remarks>
    /// <see cref="StatusCode.Unavailable"/> is deliberately a denial, not
    /// <see cref="ExplorerPluginAccessState.Unavailable"/>: on the wire it means
    /// the server could not be reached, which is transient, whereas the plugin
    /// state means the capability does not exist and never will here. Hiding the
    /// surface on a dropped connection would make a reconnect look like an
    /// uninstall.
    /// </remarks>
    private static ExplorerPluginAccess Classify(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => ExplorerPluginAccess.ReportUnavailable(
            string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? "This cluster does not serve tenant administration."
                : exception.Status.Detail),
        StatusCode.Unauthenticated => ExplorerPluginAccess.RequireAuthentication(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? null : exception.Status.Detail),
        _ => ExplorerPluginAccess.Deny(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? DeniedReason : exception.Status.Detail),
    };
}
