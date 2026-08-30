using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// The Tenants plugin's own four-state access gate: the surface exists only on a
/// cluster that serves tenancy, and only a validated platform operator may use
/// it.
/// <para>
/// It resolves in two steps, in cost order, and each step maps onto exactly one
/// of the shell's four states:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>Does the cluster serve tenancy at all?</b> The tenancy seam's
///     availability probe answers
///     <see cref="ExplorerPluginAccessState.Unavailable"/> for a deployment
///     without the tenancy add-on - either because this head never enabled
///     tenant scoping, or because the cluster answered the light probe read with
///     an <c>Unimplemented</c> status. The shell then renders no Tenants entry at
///     all, rather than an entry no operator could ever be granted (epic
///     decision D9).
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Is this caller a platform operator?</b> Only once the surface is known
///     to exist. The check runs through the Explorer's existing operator gate -
///     the cluster's <c>Admin</c>-on-the-reserved-policy-tree root of trust -
///     reached through the controlled domain model, so this plugin introduces no
///     second operator signal of its own.
///     </description>
///   </item>
/// </list>
/// <para>
/// The order matters: probing the operator first would let a non-tenant cluster
/// render a greyed-out Tenants entry to a non-operator, which is the one thing
/// the unavailable state exists to prevent.
/// </para>
/// </summary>
/// <remarks>
/// Gating is advisory (epic decision D6). The cluster re-enforces every tenancy
/// operation regardless of what this gate said, so an allowed decision never
/// removes the workspace's duty to render a runtime refusal, and a denial here
/// is a display affordance layered over the cluster's own fail-closed
/// enforcement rather than a substitute for it.
/// </remarks>
/// <param name="domain">
/// The controlled tenancy domain model - the whole of what this plugin reaches
/// (epic decision D3).
/// </param>
public sealed class TenantsAccessGate(ITenancyDomain domain) : IExplorerPluginAccessGate
{
    /// <summary>
    /// The reason a non-operator is refused. Phrased as a statement about the
    /// caller's authority rather than about the cluster, so it is not mistaken
    /// for the surface being absent.
    /// </summary>
    internal const string NotOperatorReason =
        "Tenant administration is reserved for platform operators. Your account is not "
        + "authorized as an administrator on this cluster.";

    private readonly ITenancyDomain _domain = domain ?? throw new ArgumentNullException(nameof(domain));

    /// <inheritdoc />
    public async ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Never throws: the seam folds an unreachable endpoint, a refusal, and an
        // absent add-on into a decision, so a probe cannot break the shell.
        var availability = await _domain.ProbeAvailabilityAsync(cancellationToken).ConfigureAwait(false);
        if (!availability.IsAllowed)
        {
            // Unavailable, denied, and authentication-required all pass through
            // unchanged, so the shell can tell "no such capability here" from
            // "not yours" from "sign in first".
            return availability;
        }

        var isOperator = await _domain.IsPlatformOperatorAsync(cancellationToken).ConfigureAwait(false);
        return isOperator ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Deny(NotOperatorReason);
    }
}
