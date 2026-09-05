using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The permission-aware discovery core: the per-session hook the streamable-HTTP
/// transport invokes (through
/// <c>HttpServerTransportOptions.ConfigureSessionOptions</c>) once a session
/// initialises. It resolves the caller's identity fail-closed via the credential
/// bridge, scopes the advertised tool set to the caller's effective permissions,
/// installs the <c>lattice_capabilities</c> meta-tool, and populates the server
/// <c>instructions</c> - all from the same permission-scoped view - so two
/// callers with different grants observe different tool sets and an anonymous
/// session is offered nothing.
/// </summary>
/// <remarks>
/// <para>
/// The SDK's per-session tool collection
/// (<see cref="McpServerOptions.ToolCollection"/>) plus the tool
/// <c>list_changed</c> capability are the mechanism that makes a conventionally
/// static tool list vary per authenticated caller; this type owns that wiring.
/// A group whose caller holds no matching grant contributes <b>none</b> of its
/// tools, so the caller can neither see nor invoke them (not listed-then-denied).
/// </para>
/// </remarks>
internal sealed class LatticeApiMcpSessionConfigurator
{
    /// <summary>The advertised name of the region-discovery meta-tool.</summary>
    private const string RegionDiscoveryToolName = "lattice_list_regions";

    private readonly ILatticeApiMcpCredentialBridge _credentialBridge;
    private readonly ILatticeApiMcpPermissionResolver _permissionResolver;
    private readonly IReadOnlyList<ILatticeApiMcpToolGroup> _toolGroups;
    private readonly LatticeApiMcpAccessSet _registeredGroups;
    private readonly IServiceProvider _services;
    private readonly ILatticeApiMcpGroupEndpointSource? _endpointSource;
    private readonly ILatticeApiMcpUnsupportedToolSource? _unsupportedToolSource;
    private readonly ILogger<LatticeApiMcpSessionConfigurator> _logger;

    /// <summary>Initialises the session configurator from the registered discovery collaborators.</summary>
    public LatticeApiMcpSessionConfigurator(
        ILatticeApiMcpCredentialBridge credentialBridge,
        ILatticeApiMcpPermissionResolver permissionResolver,
        IEnumerable<ILatticeApiMcpToolGroup> toolGroups,
        IServiceProvider services,
        ILogger<LatticeApiMcpSessionConfigurator> logger,
        ILatticeApiMcpGroupEndpointSource? endpointSource = null,
        ILatticeApiMcpUnsupportedToolSource? unsupportedToolSource = null)
    {
        _credentialBridge = credentialBridge ?? throw new ArgumentNullException(nameof(credentialBridge));
        _permissionResolver = permissionResolver ?? throw new ArgumentNullException(nameof(permissionResolver));
        ArgumentNullException.ThrowIfNull(toolGroups);
        _toolGroups = toolGroups as IReadOnlyList<ILatticeApiMcpToolGroup> ?? toolGroups.ToArray();
        var registered = LatticeApiMcpAccessSet.None;
        for (var i = 0; i < _toolGroups.Count; i++)
        {
            registered = registered.With(_toolGroups[i].Group);
        }

        _registeredGroups = registered;
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _endpointSource = endpointSource;
        _unsupportedToolSource = unsupportedToolSource;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Configures <paramref name="mcpOptions"/> for the session initiated by
    /// <paramref name="httpContext"/>: filters the tool collection, installs the
    /// capabilities meta-tool, and sets the permission-scoped instructions.
    /// </summary>
    /// <param name="httpContext">The request context that initiated the session.</param>
    /// <param name="mcpOptions">The per-session server options to populate.</param>
    /// <param name="cancellationToken">Cancels the configuration.</param>
    public async Task ConfigureAsync(
        HttpContext httpContext,
        McpServerOptions mcpOptions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(mcpOptions);

        var plan = await BuildSessionPlanAsync(httpContext, cancellationToken).ConfigureAwait(false);

        mcpOptions.ToolCollection = plan.Tools;
        mcpOptions.Capabilities ??= new ServerCapabilities();
        mcpOptions.Capabilities.Tools ??= new ToolsCapability();
        // Advertise per-session tool-list mutability so a client re-fetches the
        // scoped list rather than assuming a static server-wide set.
        mcpOptions.Capabilities.Tools.ListChanged = true;
        mcpOptions.ServerInstructions = plan.Instructions;
    }

    /// <summary>
    /// Resolves the caller's permission-scoped session plan - the capability
    /// snapshot, the filtered tool collection, and the instructions string -
    /// without mutating any <see cref="McpServerOptions"/>. Exposed for
    /// deterministic unit testing of the discovery core in isolation from the MCP
    /// transport.
    /// </summary>
    /// <param name="httpContext">The request context that initiated the session.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved session plan.</returns>
    internal async Task<LatticeApiMcpSessionPlan> BuildSessionPlanAsync(
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var credential = _credentialBridge.Resolve(httpContext);
        var access = LatticeApiMcpAccessSet.None;
        if (credential is not null)
        {
            access = await _permissionResolver.ResolveAsync(credential.Value, cancellationToken)
                .ConfigureAwait(false);
        }

        var clusterInfo = await ResolveClusterInfoAsync(httpContext, cancellationToken).ConfigureAwait(false);
        var capabilities = BuildCapabilities(credential, access, clusterInfo);
        var tools = new McpServerPrimitiveCollection<McpServerTool>();
        var regionToolAdvertised = false;
        if (credential is not null)
        {
            // Only an authenticated caller is offered anything at all. The
            // meta-tool is always available to an authenticated caller so an
            // agent can learn what it may (or may not) do; group tools are added
            // only for the groups the caller is granted.
            tools.Add(CreateCapabilitiesTool(capabilities));
            regionToolAdvertised = await AddRegionDiscoveryToolAsync(
                tools, access, httpContext, cancellationToken).ConfigureAwait(false);
            await AddPermittedGroupToolsAsync(tools, access, httpContext, cancellationToken)
                .ConfigureAwait(false);
        }

        return new LatticeApiMcpSessionPlan(
            capabilities, tools, BuildInstructions(capabilities, regionToolAdvertised));
    }

    /// <summary>
    /// Adds the <c>lattice_list_regions</c> discovery tool when - and only when -
    /// the caller may reach it, returning whether it was advertised.
    /// </summary>
    /// <remarks>
    /// Unlike <c>lattice_capabilities</c>, whose entire payload is derived from
    /// the caller's own credential and grants, this tool reports cluster-internal
    /// topology: the id and cluster id of every reachable <b>peer</b> region and
    /// the per-group gRPC endpoint each is served from. That is only ever useful
    /// to a caller who can actually route a tool call somewhere, so it is gated
    /// exactly like a group tool rather than riding along with the meta-tool - a
    /// caller holding no facade grant at all is told nothing, and the registered
    /// authorizer (default-deny) must additionally permit it by name. The
    /// advertised tool is wrapped in <see cref="AuthorizedMetaTool"/> so the same
    /// gate runs again per invocation, keeping the two enforcement points in
    /// lock-step.
    /// </remarks>
    private async Task<bool> AddRegionDiscoveryToolAsync(
        McpServerPrimitiveCollection<McpServerTool> tools,
        LatticeApiMcpAccessSet access,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        if (access.IsEmpty)
        {
            return false;
        }

        var authorized = await McpToolAuthorizationGate
            .IsAuthorizedAsync(_services, httpContext, RegionDiscoveryToolName, cancellationToken)
            .ConfigureAwait(false);
        if (!authorized)
        {
            return false;
        }

        tools.Add(new AuthorizedMetaTool(CreateListRegionsTool()));
        return true;
    }

    /// <summary>
    /// Resolves the cluster identity stamped into the capabilities report.
    /// </summary>
    /// <remarks>
    /// A <b>transient</b> backend fault is re-raised: the session plan is being built
    /// while the cluster is unreachable, so answering with a well-formed advertisement
    /// would present a permission-scoped tool list assembled from answers that never
    /// arrived. Any other fault leaves the cluster identity unresolved and is
    /// best-effort, because that field is decorative and its absence cannot be
    /// mistaken for a narrower permission set.
    /// </remarks>
    private async Task<ClusterInfo?> ResolveClusterInfoAsync(
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        var stateQuery = httpContext.RequestServices.GetService<ILatticeStateQuery>()
            ?? _services.GetService<ILatticeStateQuery>();
        if (stateQuery is null)
        {
            return null;
        }

        try
        {
            return await stateQuery.GetClusterInfoAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            && LatticeApiMcpDiscoveryFaultClassifier.IsTransientBackendFault(ex))
        {
            _logger.LogWarning(
                ex,
                "Resolving cluster info for the MCP capabilities report hit a transient backend fault; "
                + "surfacing a retryable discovery error rather than a partially resolved session.");
            throw new LatticeApiMcpDiscoveryUnavailableException(
                "MCP tool discovery could not reach the cluster while building the session plan. "
                + "Retry the session.",
                ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Resolving cluster info for the MCP capabilities report failed.");
            return null;
        }
    }

    private async Task AddPermittedGroupToolsAsync(
        McpServerPrimitiveCollection<McpServerTool> tools,
        LatticeApiMcpAccessSet access,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < _toolGroups.Count; i++)
        {
            var group = _toolGroups[i];
            if (!access.Contains(group.Group))
            {
                continue;
            }

            var groupTools = group.Tools;
            for (var j = 0; j < groupTools.Count; j++)
            {
                var tool = groupTools[j];
                var toolName = tool.ProtocolTool.Name;

                // Per-tool minimum inside an already-permitted group. The coarse
                // group mask admits on ANY intersecting operation, so for a
                // data-plane group a bare Read grant reaches the mutating tools
                // too. Withhold those unless the caller actually holds a matching
                // operation. Withheld here means unreachable at tools/call as
                // well, because the session's tool collection serves both. A
                // resolver that supplies no operation detail keeps the historical
                // group-level-only filtering.
                if (access.CarriesOperationDetail
                    && (access.GrantedOperations & group.RequiredOperationsFor(toolName)) == LatticeOperation.None)
                {
                    continue;
                }

                // Defer (omit) any tool the topology reports unsupported so a
                // listed tool is never one that hard-errors on invoke. Under the
                // in-silo topology no source is registered, so nothing is deferred.
                if (_unsupportedToolSource?.IsUnsupported(toolName) == true)
                {
                    continue;
                }

                // Coarse transport gate: only advertise a tool the registered
                // authorizer (default-deny) permits, so a listed tool is never
                // one the caller would be rejected from invoking. The same gate
                // runs again per invocation in CredentialStampingTool.
                var authorized = await McpToolAuthorizationGate
                    .IsAuthorizedAsync(_services, httpContext, toolName, cancellationToken)
                    .ConfigureAwait(false);
                if (!authorized)
                {
                    continue;
                }

                if (!tools.TryAdd(new CredentialStampingTool(tool, group.Group)))
                {
                    _logger.LogWarning(
                        "MCP tool '{ToolName}' from group '{Group}' collides with an existing tool and was skipped.",
                        toolName,
                        group.Group);
                }
            }
        }
    }

    private LatticeApiMcpCapabilities BuildCapabilities(
        LatticeCredential? credential,
        LatticeApiMcpAccessSet access,
        ClusterInfo? clusterInfo)
    {
        var groups = new LatticeApiMcpGroupCapability[LatticeApiMcpGroupCapabilityMap.AllGroups.Count];
        for (var i = 0; i < LatticeApiMcpGroupCapabilityMap.AllGroups.Count; i++)
        {
            var group = LatticeApiMcpGroupCapabilityMap.AllGroups[i];
            groups[i] = new LatticeApiMcpGroupCapability
            {
                Group = group,
                // Usable now: the caller holds a matching grant AND a tool module
                // for the group is registered on this server.
                Available = access.Contains(group) && _registeredGroups.Contains(group),
                // In-silo topology: every group is co-hosted, so the endpoint
                // source is absent and no endpoint is advertised. The remote-host
                // binding registers an endpoint source that populates this slot
                // with each group's served gRPC endpoint.
                Endpoint = _endpointSource?.EndpointFor(group),
            };
        }

        return new LatticeApiMcpCapabilities
        {
            Authenticated = credential is not null,
            SubjectId = credential is null ? null : SubjectIdOf(credential.Value),
            ClusterId = clusterInfo?.ClusterId ?? string.Empty,
            ServiceId = clusterInfo?.ServiceId ?? string.Empty,
            Groups = groups,
        };
    }

    private static string? SubjectIdOf(LatticeCredential credential)
        => !string.IsNullOrEmpty(credential.PrincipalId) ? credential.PrincipalId
            : !string.IsNullOrEmpty(credential.Token) ? credential.Token
            : null;

    private static McpServerTool CreateCapabilitiesTool(LatticeApiMcpCapabilities capabilities)
        => McpServerTool.Create(
            () => capabilities,
            new McpServerToolCreateOptions
            {
                Name = "lattice_capabilities",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Lattice capabilities",
                Description =
                    "Reports which of the Lattice API facade groups (such as state, data, backup, auth, "
                    + "replication, telemetry, and treeadmin) "
                    + "the current authenticated caller may use, the connected cluster's identity, and "
                    + "the endpoint each group is served from. A group served by a routable facade reports "
                    + "that facade's endpoint; a group co-hosted with this server reports none. Read-only "
                    + "and scoped to the caller's effective permissions.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateListRegionsTool()
        => McpServerTool.Create(
            RegionDiscoveryToolHandlers.ListRegionsAsync,
            new McpServerToolCreateOptions
            {
                Name = RegionDiscoveryToolName,
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Lattice regions",
                Description =
                    "Lists the regions this server can route a tool call to: the current cluster plus "
                    + "any reachable, credentialed peer region, each with per-group endpoint availability. "
                    + "A region with no route or credentials is omitted (fail-closed), and a group a region "
                    + "does not serve - telemetry included - is reported unavailable for it and rejected "
                    + "fail-closed when targeted there. Pass a listed "
                    + "region id as the optional 'region' argument of any tool to target that region; "
                    + "omit it to target the current region. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static string BuildInstructions(
        LatticeApiMcpCapabilities capabilities,
        bool regionToolAdvertised)
    {
        var builder = new StringBuilder();
        if (!capabilities.Authenticated)
        {
            builder.Append(
                "This Orleans.Lattice MCP endpoint is fail-closed. The session is not authenticated, "
                + "so no facade groups are available and no tools are advertised. Present a recognised "
                + "credential to enumerate and use the state, data, backup, and auth tools.");
            return builder.ToString();
        }

        builder.Append("This Orleans.Lattice MCP endpoint exposes the cluster's API facades as tools, ");
        builder.Append("scoped to your effective permissions. ");

        if (!string.IsNullOrEmpty(capabilities.ClusterId) || !string.IsNullOrEmpty(capabilities.ServiceId))
        {
            builder.Append("Connected cluster: clusterId='");
            builder.Append(capabilities.ClusterId);
            builder.Append("', serviceId='");
            builder.Append(capabilities.ServiceId);
            builder.Append("'. ");
        }

        var available = new List<string>(capabilities.Groups.Count);
        var unavailable = new List<string>(capabilities.Groups.Count);
        for (var i = 0; i < capabilities.Groups.Count; i++)
        {
            var group = capabilities.Groups[i];
            var name = LatticeApiMcpGroupCapabilityMap.DisplayName(group.Group);
            (group.Available ? available : unavailable).Add(name);
        }

        if (available.Count > 0)
        {
            builder.Append("Available groups: ");
            builder.Append(string.Join(", ", available));
            builder.Append(". ");
        }
        else
        {
            builder.Append("No facade groups are available to you. ");
        }

        if (unavailable.Count > 0)
        {
            builder.Append("Unavailable groups (no grant): ");
            builder.Append(string.Join(", ", unavailable));
            builder.Append(". ");
        }

        builder.Append("Call lattice_capabilities for the machine-readable capability report. ");
        if (regionToolAdvertised)
        {
            builder.Append(
                "Call lattice_list_regions to discover which regions you can target; pass a region id as "
                + "the optional 'region' argument of any tool to route it there (omit it for the current region).");
        }

        return builder.ToString();
    }
}
