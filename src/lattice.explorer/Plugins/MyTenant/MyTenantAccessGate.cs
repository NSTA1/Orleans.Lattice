using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The default <see cref="IMyTenantAccessGate"/>: the caller reaches the My
/// Tenant area when they hold admin authority over their <em>own active</em>
/// tenant.
/// <para>
/// The probe reports <em>facts</em> in three steps, cheapest first, and every
/// one of them fails closed:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>Is there tenancy here at all?</b> A deployment without the tenancy
///     add-on reports an absent capability without a call, so the area renders
///     nothing rather than an error (epic decision D9).
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Does the cluster serve tenant administration?</b> Delegated to the
///     shared availability probe, whose answer already separates "the surface
///     does not exist here" from "no credential was presented" from "the grant
///     was withheld".
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Does this caller administer the active tenant?</b> Reading the
///     tenant's own admin-subject set is the narrowest question that answers it:
///     the cluster admits a live admin subject of that tenant, and it admits a
///     platform operator - which is exactly the "an operator viewing a tenant
///     sees the same surface" case - and refuses everyone else.
///     </description>
///   </item>
/// </list>
/// <para>
/// A withheld grant is reported as withheld, not as a denial:
/// <see cref="ExplorerPluginAccessContract"/> renders it as a sign-in prompt for
/// an anonymous caller and as a denial for a signed-in one. Deciding it here is
/// how this gate came to tell a signed-out visitor that they did not hold admin
/// authority over an active tenant they never had.
/// </para>
/// <para>
/// Client gating is advisory (epic decision D6): the cluster re-enforces every
/// read and every mutation, so a caller who slipped past this gate still
/// achieves nothing.
/// </para>
/// </summary>
/// <remarks>
/// The probe also files the plugin's registration-order diagnostic under
/// <see cref="MyTenantPluginKeys.OperatorGateScope"/>, so a head running on the
/// navigation core's fail-closed placeholder operator gate is told rather than
/// silently losing every tenant switch. That decision gates nothing: it is a
/// diagnostic the surface renders.
/// </remarks>
/// <param name="domain">The tenancy domain model the plugin operates against.</param>
/// <param name="store">The keyed plugin access store the diagnostic is filed into.</param>
/// <param name="operatorGate">
/// The Explorer's platform-operator gate, or <see langword="null"/> on a head
/// that never opted into tenant scoping. Read only to classify which gate is
/// installed; the plugin never asks it for an authorization decision, which
/// stays with <paramref name="domain"/>.
/// </param>
/// <param name="session">
/// The Explorer's sign-in seam, read only to tell an anonymous refusal from an
/// authenticated one.
/// </param>
internal sealed class MyTenantAccessGate(
    IMyTenantDomain domain,
    IExplorerPluginAccessStore store,
    IExplorerTenantOperatorGate? operatorGate = null,
    IExplorerAuthSession? session = null) : ExplorerPluginAccessGate, IMyTenantAccessGate
{
    private const string NoTenancyReason =
        "This deployment does not have the tenancy add-on enabled, so there is no tenant to administer.";

    private const string NoActiveTenantReason =
        "No active tenant is established for your sign-in, so there is no tenant to administer.";

    private const string NotTenantAdminReason =
        "You do not hold admin authority over your active tenant.";

    private const string UnprovenReason =
        "Your admin authority over this tenant could not be confirmed.";

    /// <summary>
    /// The grant a refused caller is missing, and who issues it. Cached, so
    /// attaching it to a denial costs nothing per probe.
    /// </summary>
    private static readonly ExplorerAccessRemedy MissingGrant =
        ExplorerAccessRemedy.Requiring("Tenant admin", "your tenant's administrator");

    private readonly IMyTenantDomain _domain = domain ?? throw new ArgumentNullException(nameof(domain));

    private readonly IExplorerPluginAccessStore _store = store ?? throw new ArgumentNullException(nameof(store));

    private readonly IExplorerTenantOperatorGate? _operatorGate = operatorGate;

    private readonly IExplorerAuthSession? _session = session;

    /// <inheritdoc />
    public override ExplorerAccessRemedy Remedy => MissingGrant;

    /// <inheritdoc />
    protected override bool IsCallerAuthenticated => _session?.IsAuthenticated ?? false;

    /// <inheritdoc />
    protected override async ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken)
    {
        PublishOperatorGateDiagnostic();

        // D9: no tenancy add-on, so the surface does not exist here. Both the
        // host's own resolved scope and the domain must agree it is on, so a head
        // that publishes one without the other still degrades to nothing rather
        // than half a surface.
        if (!context.Tenant.IsActive || !_domain.IsTenancyEnabled)
        {
            return ExplorerPluginAccessFacts.CapabilityAbsent(NoTenancyReason);
        }

        // Does the cluster serve tenant administration for this caller at all?
        // An absent capability, an unauthenticated connection, and a withheld
        // grant all pass straight through: this gate can only narrow that
        // answer, never widen it.
        var availability = await _domain.ProbeAvailabilityAsync(cancellationToken).ConfigureAwait(false);
        if (availability.State != ExplorerPluginAccessState.Allowed)
        {
            return ExplorerPluginAccessFacts.From(availability);
        }

        var tenantId = ResolveActiveTenantId(context);
        if (string.IsNullOrEmpty(tenantId))
        {
            return ExplorerPluginAccessFacts.Withhold(NoActiveTenantReason);
        }

        // The narrowest read that proves tenant-admin standing: the cluster
        // admits a live admin subject of this tenant, and a platform operator
        // acting on it, and refuses everyone else. A not-found answer is the
        // cluster deliberately refusing to confirm the tenant exists, so the
        // grant is withheld rather than the surface being absent.
        var admins = await _domain.Tenants
            .ListAdminSubjectsAsync(tenantId, cancellationToken)
            .ConfigureAwait(false);

        // A seam that answered nothing proved nothing.
        if (admins is null)
        {
            return ExplorerPluginAccessFacts.Withhold(UnprovenReason);
        }

        return admins.Status switch
        {
            TenantOperationStatus.Succeeded => ExplorerPluginAccessFacts.Granted,
            TenantOperationStatus.AuthenticationRequired =>
                ExplorerPluginAccessFacts.CredentialMissing(admins.Message),
            TenantOperationStatus.Unavailable => ExplorerPluginAccessFacts.CapabilityAbsent(admins.Message),
            TenantOperationStatus.Denied or TenantOperationStatus.NotFound =>
                ExplorerPluginAccessFacts.Withhold(NotTenantAdminReason),
            _ => ExplorerPluginAccessFacts.Withhold(admins.Message),
        };
    }

    /// <summary>
    /// The tenant the surface scopes itself to: the domain's active tenant, and
    /// the host's own resolved scope when the domain has not established one.
    /// Both name the same tenant on a correctly wired head; reading either keeps
    /// the gate working while a switch is settling.
    /// </summary>
    private string? ResolveActiveTenantId(IExplorerPluginHostContext context) =>
        _domain.ActiveTenant?.Value ?? context.Tenant.ActiveTenantId;

    private void PublishOperatorGateDiagnostic()
    {
        var diagnostic = MyTenantOperatorGateDiagnostic.Describe(_operatorGate);

        // Filed either way, so a head that fixes its ordering clears the notice
        // on the next probe instead of keeping a stale one.
        _store.Set(
            MyTenantPluginKeys.PluginId,
            MyTenantPluginKeys.OperatorGateScope,
            diagnostic is null ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Deny(diagnostic));
    }
}
