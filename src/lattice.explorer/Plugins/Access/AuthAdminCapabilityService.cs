using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The default <see cref="IAuthAdminCapabilityService"/>. Drives the
/// <see cref="IAuthAdminClient"/> probe surface. A genuine authorization denial
/// resolves to <see cref="ExplorerPluginAccess.Denied"/> (the area greys out);
/// a probe that fails because the connection is <em>unauthenticated</em>
/// resolves to <see cref="ExplorerPluginAccess.AuthenticationRequired"/> so the
/// shell can prompt a (re-)sign-in rather than collapse both into the same
/// silent deny.
/// </summary>
public sealed class AuthAdminCapabilityService(
    IAuthAdminClient client,
    IExplorerPluginAccessStore store,
    IExplorerAuthSession session) : ExplorerPluginAccessGate, IAuthAdminCapabilityService
{
    private const string NoGrantReason =
        "Your account is not authorized to administer access on this cluster.";

    /// <summary>
    /// The grant a refused caller is missing, and who issues it. Cached, so
    /// attaching it to a denial costs nothing per probe.
    /// </summary>
    private static readonly ExplorerAccessRemedy MissingGrant =
        ExplorerAccessRemedy.Requiring("Admin", "a platform administrator");

    private readonly IAuthAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    private readonly IExplorerPluginAccessStore _store =
        store ?? throw new ArgumentNullException(nameof(store));

    private readonly IExplorerAuthSession _session = session ?? throw new ArgumentNullException(nameof(session));

    // Advisory display state, not an access decision, so it is the plugin's own
    // field rather than a store entry. An int-sized enum field is written and
    // read atomically, so a probe completing on the thread pool never publishes
    // a torn value to a reader on the render path.
    private ExplorerAccessAuthenticationMode _authenticationMode = ExplorerAccessAuthenticationMode.Unknown;

    /// <summary>
    /// The classification of the coarse administrator probe: the connection is
    /// accepted as an administrator, denied (authenticated but unauthorized, or
    /// the endpoint is unreachable), or rejected because the connection is
    /// unauthenticated and a sign-in is required.
    /// </summary>
    private enum AdminProbeOutcome
    {
        /// <summary>The control plane accepts the caller as an administrator.</summary>
        Allowed,

        /// <summary>A genuine deny (authenticated-but-unauthorized) or an unreachable endpoint: advisory grey-out.</summary>
        Denied,

        /// <summary>The connection is unauthenticated: a (re-)sign-in is required.</summary>
        AuthenticationRequired,
    }

    /// <inheritdoc />
    public ExplorerAccessAuthenticationMode AuthenticationMode => _authenticationMode;

    /// <inheritdoc />
    public override ExplorerAccessRemedy Remedy => MissingGrant;

    /// <inheritdoc />
    protected override bool IsCallerAuthenticated => _session.IsAuthenticated;

    /// <inheritdoc />
    protected override async ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken)
    {
        var snapshot = await ProbeInternalAsync(cancellationToken).ConfigureAwait(false);

        _authenticationMode = snapshot.AuthenticationMode;
        _store.Set(
            AccessPluginKeys.PluginId,
            AccessPluginKeys.DirectoryScope,
            snapshot.DirectoryAvailable ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied);

        if (snapshot.Allowed)
        {
            return ExplorerPluginAccessFacts.Granted;
        }

        // The probe already distinguished an unauthenticated connection from a
        // genuine refusal, so it reports the sharper fact and the contract
        // agrees with it rather than re-deriving the state from the session.
        return snapshot.AuthenticationRequired
            ? ExplorerPluginAccessFacts.CredentialMissing()
            : ExplorerPluginAccessFacts.Withhold(NoGrantReason);
    }

    private async ValueTask<AccessCapabilitySnapshot> ProbeInternalAsync(CancellationToken cancellationToken)
    {
        var outcome = await ProbeAdminAsync(cancellationToken).ConfigureAwait(false);
        if (outcome != AdminProbeOutcome.Allowed)
        {
            // Not an administrator (or the endpoint is unreachable): the access
            // model is admin-gated too, so there is nothing more to learn. Publish
            // the safe deny snapshot without a second round trip, flagging the
            // unauthenticated case so the shell can prompt a sign-in.
            return outcome == AdminProbeOutcome.AuthenticationRequired
                ? AccessCapabilitySnapshot.Unauthenticated
                : AccessCapabilitySnapshot.Denied;
        }

        return await ProbeAccessModelAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<AdminProbeOutcome> ProbeAdminAsync(CancellationToken cancellationToken)
    {
        try
        {
            // A light, read-only list is the coarse gate: reaching it (even with an
            // empty page) means the control plane accepts the caller as an
            // administrator. It has no side effects, so it is safe to run on mount.
            var page = await _client
                .ListGroupsAsync(new AuthPageRequest { PageSize = 1 }, cancellationToken)
                .ConfigureAwait(false);

            // A seam that answered nothing proved nothing: an absent response is
            // not an admission, so it fails closed rather than granting.
            return page is null ? AdminProbeOutcome.Denied : AdminProbeOutcome.Allowed;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            // A translated server denial. The client cannot see from the status
            // alone whether the subject was anonymous or an authenticated
            // non-administrator, so it uses the session: a denial while no sign-in
            // is applied means the connection is unauthenticated (the token never
            // attached), which is a recoverable sign-in-required state rather than
            // a genuine authorization deny.
            return _session.IsAuthenticated
                ? AdminProbeOutcome.Denied
                : AdminProbeOutcome.AuthenticationRequired;
        }
        catch (RpcException ex)
        {
            // Unauthenticated is unambiguous: the server rejected the call as having
            // no accepted credential. A PermissionDenied while signed out is the
            // anonymous-subject denial; while signed in it is a genuine authz deny.
            if (ex.StatusCode == StatusCode.Unauthenticated)
            {
                return AdminProbeOutcome.AuthenticationRequired;
            }

            if (ex.StatusCode == StatusCode.PermissionDenied && !_session.IsAuthenticated)
            {
                return AdminProbeOutcome.AuthenticationRequired;
            }

            return AdminProbeOutcome.Denied;
        }
        catch (InvalidOperationException)
        {
            // The explorer is not configured with an endpoint yet (no connection
            // client). Treat as deny; a later connection-status change re-probes.
            return AdminProbeOutcome.Denied;
        }
    }

    private async ValueTask<AccessCapabilitySnapshot> ProbeAccessModelAsync(CancellationToken cancellationToken)
    {
        try
        {
            var model = await _client.GetAccessModelAsync(cancellationToken).ConfigureAwait(false);
            if (model is null)
            {
                return AccessCapabilitySnapshot.AdminWithoutModel;
            }

            return new AccessCapabilitySnapshot(
                Allowed: true,
                AuthenticationRequired: false,
                model.DirectoryAvailable,
                MapAuthenticationMode(model.AuthenticationMode));
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return AccessCapabilitySnapshot.AdminWithoutModel;
        }
        catch (RpcException)
        {
            return AccessCapabilitySnapshot.AdminWithoutModel;
        }
        catch (InvalidOperationException)
        {
            return AccessCapabilitySnapshot.AdminWithoutModel;
        }
    }

    private static ExplorerAccessAuthenticationMode MapAuthenticationMode(AccessAuthenticationMode mode) => mode switch
    {
        AccessAuthenticationMode.Anonymous => ExplorerAccessAuthenticationMode.Anonymous,
        AccessAuthenticationMode.Claims => ExplorerAccessAuthenticationMode.Claims,
        AccessAuthenticationMode.Basic => ExplorerAccessAuthenticationMode.Basic,
        _ => ExplorerAccessAuthenticationMode.Unknown,
    };

    /// <summary>
    /// The merged outcome of the Access-area capability probe: whether the caller
    /// is an administrator, whether the failure was an unauthenticated connection,
    /// whether a searchable identity directory is available, and the active
    /// authentication mode.
    /// </summary>
    private readonly record struct AccessCapabilitySnapshot(
        bool Allowed,
        bool AuthenticationRequired,
        bool DirectoryAvailable,
        ExplorerAccessAuthenticationMode AuthenticationMode)
    {
        /// <summary>The safe snapshot published when the caller is an authenticated non-administrator or the endpoint is unreachable.</summary>
        public static AccessCapabilitySnapshot Denied { get; } =
            new(false, false, false, ExplorerAccessAuthenticationMode.Unknown);

        /// <summary>The snapshot published when the probe failed because the connection is unauthenticated.</summary>
        public static AccessCapabilitySnapshot Unauthenticated { get; } =
            new(false, true, false, ExplorerAccessAuthenticationMode.Unknown);

        /// <summary>The snapshot published when the caller is an administrator but the access model could not be read.</summary>
        public static AccessCapabilitySnapshot AdminWithoutModel { get; } =
            new(true, false, false, ExplorerAccessAuthenticationMode.Unknown);
    }
}
