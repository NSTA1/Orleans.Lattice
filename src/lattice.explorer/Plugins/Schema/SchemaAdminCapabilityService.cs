using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The default <see cref="ISchemaAdminCapabilityService"/>. Drives the
/// <see cref="ISchemaAdminClient"/> probe surface. All probes swallow a denial /
/// transport failure and fall back to a withheld grant, so a probe never breaks
/// the shell.
/// </summary>
/// <remarks>
/// <para>
/// The backend capability probe is fail-closed but does not itself throw on an
/// authorization denial: it returns an all-false capability set. The plugin-level
/// gate therefore reads that set rather than merely observing that the RPC
/// completed - "the schema control endpoint is reachable" is not a grant, and
/// reporting <see cref="ExplorerPluginAccessState.Allowed"/> for it invited a
/// caller with no schema authority into a surface every action of which the
/// server would refuse.
/// </para>
/// <para>
/// The per-action grey-out inside the panel is still driven by the per-tree
/// <see cref="SchemaCapabilitySnapshot"/> requested through
/// <see cref="ProbeTreeAsync"/>.
/// </para>
/// </remarks>
/// <param name="client">The schema control-API seam the probes run against.</param>
/// <param name="session">
/// The Explorer's sign-in seam, read only to tell an anonymous refusal from an
/// authenticated one.
/// </param>
public sealed class SchemaAdminCapabilityService(
    ISchemaAdminClient client,
    IExplorerAuthSession? session = null) : ExplorerPluginAccessGate, ISchemaAdminCapabilityService
{
    /// <summary>
    /// The reserved tree id used for the coarse capability probe. Probing it has no
    /// side effects and never reads or writes real tree data, so it is safe to run on
    /// mount regardless of which trees exist.
    /// </summary>
    internal const string CapabilityProbeTreeId = "__schema_capability_probe__";

    private const string NoGrantReason =
        "Your account holds no schema-management authority on this cluster.";

    private const string DisconnectedReason =
        "The schema control surface could not be reached.";

    /// <summary>
    /// The grant a refused caller is missing, and who issues it. Cached, so
    /// attaching it to a denial costs nothing per probe.
    /// </summary>
    private static readonly ExplorerAccessRemedy MissingGrant =
        ExplorerAccessRemedy.Requiring("Admin", ExplorerVocabulary.GrantAudience);

    private readonly ISchemaAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

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
        try
        {
            // The capability probe has no side effects and does not throw on an
            // authorization denial (it returns an all-false set), so the flags it
            // reports - not the fact that it completed - are the grant.
            var capabilities = await _client
                .ProbeCapabilitiesAsync(CapabilityProbeTreeId, cancellationToken)
                .ConfigureAwait(false);

            // A seam that answered nothing proved nothing.
            return capabilities is not null && SchemaCapabilitySnapshot.From(capabilities).HasAny
                ? ExplorerPluginAccessFacts.Granted
                : ExplorerPluginAccessFacts.Withhold(NoGrantReason);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
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

    /// <inheritdoc />
    public async Task<SchemaCapabilitySnapshot> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            var capabilities = await _client.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false);
            return capabilities is null ? SchemaCapabilitySnapshot.None : SchemaCapabilitySnapshot.From(capabilities);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return SchemaCapabilitySnapshot.None;
        }
        catch (RpcException)
        {
            return SchemaCapabilitySnapshot.None;
        }
        catch (InvalidOperationException)
        {
            // The explorer is not configured with an endpoint yet. Fail closed.
            return SchemaCapabilitySnapshot.None;
        }
    }

    /// <summary>
    /// Classifies a residual transport fault. An unimplemented facade is the
    /// schema add-on being absent from the cluster, which is the one case where
    /// the area should render nothing at all rather than a demoted entry.
    /// </summary>
    private static ExplorerPluginAccessFacts Classify(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => ExplorerPluginAccessFacts.CapabilityAbsent(
            string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? "This cluster does not serve schema administration."
                : exception.Status.Detail),
        StatusCode.Unauthenticated => ExplorerPluginAccessFacts.CredentialMissing(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? null : exception.Status.Detail),
        _ => ExplorerPluginAccessFacts.Withhold(
            string.IsNullOrWhiteSpace(exception.Status.Detail) ? DisconnectedReason : exception.Status.Detail),
    };
}
