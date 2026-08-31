using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The default <see cref="IBackupCapabilityService"/>. Drives the
/// <see cref="IBackupControlClient"/> probe surface: its plugin-level probe is
/// the coarse capability probe, and <see cref="ProbeScopeAsync"/> files the
/// per-tree decisions into the keyed <see cref="IExplorerPluginAccessStore"/>.
/// All probes swallow a denial / transport failure and fall back to a withheld
/// grant, so a probe never breaks the shell.
/// </summary>
/// <remarks>
/// <para>
/// <b>The coarse gate reads a capability flag, not a status code.</b> It used to
/// treat "the catalog list call did not throw" as proof of backup access, which
/// is not the same thing: a cluster that lets a read-only identity page an empty
/// catalog made the Backups area render <em>enabled</em> for
/// <c>data-reader</c> - an identity holding only cluster <c>Read</c> and
/// <c>RangeRead</c> and no backup grant whatsoever. That user was invited in and
/// met a server-side denial once inside, which is strictly worse than an honest
/// disabled entry. The probe now asks the control plane the question it actually
/// wants answered, through the same fail-closed gate the real operations use,
/// and reads <see cref="BackupScopeCapabilities.CanList"/>.
/// </para>
/// <para>
/// The plugin-level decision is that coarse grant <em>or</em> any per-tree scope
/// that currently grants list access, so a caller who can read backups for at
/// least one tree can reach the area even when the cluster-wide grant is absent.
/// That second half is <em>re-derived from the keyed store on every probe</em>
/// rather than remembered: a scope grant keeps the area reachable for exactly as
/// long as the store still says the scope grants list, so revoking every scope -
/// or resetting the store on sign-out - closes the area again on the next probe.
/// </para>
/// </remarks>
/// <param name="client">The backup control-API seam the probes run against.</param>
/// <param name="store">The keyed plugin access store scope decisions are filed into.</param>
/// <param name="session">
/// The Explorer's sign-in seam, read only to tell an anonymous refusal from an
/// authenticated one.
/// </param>
public sealed class BackupCapabilityService(
    IBackupControlClient client,
    IExplorerPluginAccessStore store,
    IExplorerAuthSession? session = null) : ExplorerPluginAccessGate, IBackupCapabilityService
{
    /// <summary>
    /// The reserved scope the coarse grant is probed over. Probing has no side
    /// effects and reads no tree data, so it is safe on mount regardless of which
    /// trees exist, and a cluster-wide backup grant covers it. This mirrors the
    /// Schema area's reserved capability-probe tree.
    /// </summary>
    internal const string CapabilityProbeTreeId = "__backup_capability_probe__";

    private const string NoGrantReason =
        "Your account does not hold the backup authority for this cluster.";

    private const string DisconnectedReason =
        "The backup control surface could not be reached.";

    // Bound once. The plugin-level probe asks the store this on every refresh
    // occasion, and a lambda written at the call site would allocate a delegate
    // on each of them.
    private static readonly Func<string, bool> ListScopes = BackupsPluginKeys.IsListScope;

    /// <summary>The scope the coarse probe asks about. Built once, not per probe.</summary>
    private static readonly BackupScopeSelector CapabilityProbeScope =
        BackupScopeSelector.WholeTree(CapabilityProbeTreeId);

    /// <summary>
    /// The grant a refused caller is missing, and who issues it. Cached, so
    /// attaching it to a denial costs nothing per probe.
    /// </summary>
    private static readonly ExplorerAccessRemedy MissingGrant =
        ExplorerAccessRemedy.Requiring("Backup", "a platform administrator");

    private readonly IBackupControlClient _client = client ?? throw new ArgumentNullException(nameof(client));

    private readonly IExplorerPluginAccessStore _store =
        store ?? throw new ArgumentNullException(nameof(store));

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
        var coarse = await ProbeCoarseAsync(cancellationToken).ConfigureAwait(false);

        if (coarse.IsGranted
            || coarse.Capability == ExplorerPluginCapabilityPresence.Absent
            || coarse.Authentication == ExplorerPluginCallerAuthentication.Anonymous)
        {
            // A credential the server never accepted cannot be rescued by a scope
            // grant the store still remembers: that would re-admit an anonymous
            // caller on stale evidence.
            return coarse;
        }

        // Derived, never latched: the store is the only thing that remembers a
        // scope grant, and it is overwritten by the next scope probe and dropped
        // on a reset, so this answer heals when the grant behind it goes away.
        return _store.AnyScopeAllowed(BackupsPluginKeys.PluginId, ListScopes)
            ? ExplorerPluginAccessFacts.Granted
            : coarse;
    }

    /// <inheritdoc />
    public async Task<BackupScopeCapabilitySnapshot> ProbeScopeAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        BackupScopeCapabilitySnapshot snapshot;
        try
        {
            var capabilities = await _client
                .ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(treeId), cancellationToken)
                .ConfigureAwait(false);
            snapshot = capabilities is null ? BackupScopeCapabilitySnapshot.None : Map(capabilities);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            snapshot = BackupScopeCapabilitySnapshot.None;
        }
        catch (RpcException)
        {
            snapshot = BackupScopeCapabilitySnapshot.None;
        }

        Publish(treeId, snapshot);
        return snapshot;
    }

    private void Publish(string treeId, BackupScopeCapabilitySnapshot snapshot)
    {
        _store.Set(BackupsPluginKeys.PluginId, BackupsPluginKeys.ListScope(treeId), Decide(snapshot.CanList));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.CaptureScope(treeId),
            Decide(snapshot.CanCapture));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.CaptureIncrementalScope(treeId),
            Decide(snapshot.CanCaptureIncremental));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.RestoreScope(treeId),
            Decide(snapshot.CanRestore));
        _store.Set(
            BackupsPluginKeys.PluginId,
            BackupsPluginKeys.DeleteScope(treeId),
            Decide(snapshot.CanDelete));

        if (!snapshot.CanList)
        {
            return;
        }

        // A scope that grants list access also implies the plugin-level gate.
        // The scoped entry filed above is what keeps that true across a later
        // coarse denial; this write only saves the area waiting for the next
        // gate refresh to notice.
        _store.Set(BackupsPluginKeys.PluginId, ExplorerPluginAccess.Allowed);
    }

    /// <summary>
    /// The cluster-wide backup grant, read from the control plane's own
    /// capability probe. The probe evaluates the same fail-closed gate the real
    /// operations use and reports each capability as a flag rather than throwing,
    /// so a <see langword="false"/> flag is a genuine refusal and not a transport
    /// accident.
    /// </summary>
    private async ValueTask<ExplorerPluginAccessFacts> ProbeCoarseAsync(CancellationToken cancellationToken)
    {
        try
        {
            var capabilities = await _client
                .ProbeCapabilitiesAsync(CapabilityProbeScope, cancellationToken)
                .ConfigureAwait(false);

            // A seam that answered nothing proved nothing: withhold rather than
            // read an absent response as an admission.
            return capabilities is { CanList: true }
                ? ExplorerPluginAccessFacts.Granted
                : ExplorerPluginAccessFacts.Withhold(NoGrantReason);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            // The server refuses an anonymous caller and an authenticated but
            // unauthorized one identically, so this is reported as a withheld
            // grant and the contract picks the state from the credential.
            return ExplorerPluginAccessFacts.Withhold(ex.Message);
        }
        catch (RpcException ex)
        {
            return Classify(ex);
        }
        catch (InvalidOperationException)
        {
            // The explorer is not configured with an endpoint yet (no connection
            // client). Withhold; a later connection-status change re-probes.
            return ExplorerPluginAccessFacts.Withhold(DisconnectedReason);
        }
    }

    /// <summary>
    /// Classifies a residual transport fault. An unimplemented facade is the
    /// backup add-on being absent from the cluster, which is the one case where
    /// the area should render nothing at all rather than a demoted entry.
    /// </summary>
    private static ExplorerPluginAccessFacts Classify(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => ExplorerPluginAccessFacts.CapabilityAbsent(
            string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? "This cluster does not serve backup control."
                : exception.Status.Detail),
        StatusCode.Unauthenticated => ExplorerPluginAccessFacts.CredentialMissing(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? null : exception.Status.Detail),
        _ => ExplorerPluginAccessFacts.Withhold(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? DisconnectedReason : exception.Status.Detail),
    };

    private static ExplorerPluginAccess Decide(bool granted) =>
        granted ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied;

    private static BackupScopeCapabilitySnapshot Map(BackupScopeCapabilities capabilities) => new()
    {
        CanList = capabilities.CanList,
        CanCapture = capabilities.CanCapture,
        CanCaptureIncremental = capabilities.CanCaptureIncremental,
        CanRestore = capabilities.CanRestore,
        CanDelete = capabilities.CanDelete,
    };
}
