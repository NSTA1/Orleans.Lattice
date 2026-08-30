using Grpc.Core;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The default <see cref="ISchemaAdminCapabilityService"/>. Drives the
/// <see cref="ISchemaAdminClient"/> probe surface. All probes swallow a denial /
/// transport failure and fall back to deny, so a probe never breaks the shell.
/// </summary>
/// <remarks>
/// The backend capability probe is fail-closed but does not itself throw on an
/// authorization denial: it returns an all-false capability set. So the plugin-level
/// Schema gate is "the schema control endpoint is reachable" - the probe RPC completed
/// without a transport fault - while the per-action grey-out is driven by the
/// per-tree <see cref="SchemaCapabilitySnapshot"/> the panel requests through
/// <see cref="ProbeTreeAsync"/>.
/// </remarks>
public sealed class SchemaAdminCapabilityService(ISchemaAdminClient client) : ISchemaAdminCapabilityService
{
    /// <summary>
    /// The reserved tree id used for the coarse reachability probe. Probing it has no
    /// side effects and never reads or writes real tree data, so it is safe to run on
    /// mount regardless of which trees exist.
    /// </summary>
    internal const string CapabilityProbeTreeId = "__schema_capability_probe__";

    private readonly ISchemaAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var reachable = await ProbeReachableAsync(cancellationToken).ConfigureAwait(false);
        return reachable ? ExplorerPluginAccess.Allowed : ExplorerPluginAccess.Denied;
    }

    /// <inheritdoc />
    public async Task<SchemaCapabilitySnapshot> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            var capabilities = await _client.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false);
            return SchemaCapabilitySnapshot.From(capabilities);
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

    private async Task<bool> ProbeReachableAsync(CancellationToken cancellationToken)
    {
        try
        {
            // The capability probe has no side effects and does not throw on an
            // authorization denial (it returns an all-false set), so reaching it means
            // the schema control endpoint is present and accepting calls.
            await _client.ProbeCapabilitiesAsync(CapabilityProbeTreeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
        catch (RpcException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            // The explorer is not configured with an endpoint yet (no connection
            // client). Treat as deny; a later connection-status change re-probes.
            return false;
        }
    }
}
