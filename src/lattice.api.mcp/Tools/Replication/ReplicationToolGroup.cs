using System.ComponentModel;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The replication tool module: an <see cref="ILatticeApiMcpToolGroup"/> for
/// <see cref="LatticeApiMcpGroup.Replication"/> whose tools are thin adapters
/// over the <see cref="ILatticeReplicationControl"/> facade. The read-only
/// inspect tool (<c>lattice_replication_get_config</c>) is always contributed;
/// the mutating control tools (<c>lattice_replication_enable</c>,
/// <c>lattice_replication_disable</c>) are contributed only when replication
/// control is opted in via
/// <see cref="LatticeApiMcpOptions.EnableReplicationControlTools"/> or
/// <c>AddReplicationTools(enableControl: true)</c>. Every control tool is
/// annotated destructive and non-read-only.
/// </summary>
/// <remarks>
/// <para>
/// The tools are built <b>once</b> in the constructor and are stateless: each
/// resolves the facade from the tool invocation's request service provider and
/// stamps the caller credential - bridged from the request's authenticated
/// principal - onto the ambient <see cref="LatticeCredentialContext"/> for the
/// duration of the facade call, so the facade's own fail-closed replication
/// access gate resolves the caller's subject and authorizes every read and
/// mutation. The module adds no authorization path of its own.
/// </para>
/// <para>
/// Enabling replication egresses data to a peer cluster, so both control tools
/// carry <c>destructiveHint</c>; the config-inspect tool is <c>readOnlyHint</c>.
/// The group itself is advertised only to a caller whose effective permissions
/// grant <see cref="LatticeOperation.Replication"/> - an agent without the grant
/// is offered no replication tools at all.
/// </para>
/// </remarks>
internal sealed class ReplicationToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the replication tool set once from the resolved MCP options,
    /// including the mutating control tools only when replication control is
    /// opted in.
    /// </summary>
    /// <param name="options">The resolved MCP binding options. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    public ReplicationToolGroup(IOptions<LatticeApiMcpOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Tools = BuildTools(options.Value.EnableReplicationControlTools);
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.Replication;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> BuildTools(bool enableControl)
    {
        var tools = new List<McpServerTool>(enableControl ? 3 : 1)
        {
            CreateGetConfigTool(),
        };

        if (enableControl)
        {
            tools.Add(CreateEnableTool());
            tools.Add(CreateDisableTool());
        }

        return tools;
    }

    private static McpServerTool CreateGetConfigTool()
        => McpServerTool.Create(
            (RequestContext<CallToolRequestParams> context, CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeReplicationControl>();
                return ReplicationToolInvocations.GetReplicationConfigAsync(control, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_replication_get_config",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Get replication config",
                Description =
                    "Reports the runtime replicated-tree set the caller may manage: each tree's enabled state, "
                    + "fixed merge mode, and whether its mode is ambiguous (shipping paused fail-closed). "
                    + "Permission-scoped: a tree the caller cannot manage is omitted. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateEnableTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The target tree id to enable replication for.")] string treeId,
                [Description("The wire merge mode to fix for the tree: LwwRegister, OrSet, PnCounter, VersionVector, MvRegister, OrMap, Sequence, OrFlag, RwFlag, or RwSet.")] string mode,
                CancellationToken cancellationToken,
                [Description("Optional cluster id to pull an initial snapshot from when the tree already holds data; null skips the bootstrap.")] string? bootstrapSourceClusterId = null) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeReplicationControl>();
                return ReplicationToolInvocations.EnableReplicationAsync(
                    control, treeId, mode, bootstrapSourceClusterId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_replication_enable",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Enable replication",
                Description =
                    "Enables cross-cluster replication for a tree under a fixed merge mode, authoring config that "
                    + "converges across the enrolled peer set. Egresses data to a peer: subject to the fail-closed "
                    + "replication access gate. Changing the mode of an already-enabled tree is rejected (disable "
                    + "then re-enable). Requires replication control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateDisableTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The target tree id to disable replication for.")] string treeId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeReplicationControl>();
                return ReplicationToolInvocations.DisableReplicationAsync(control, treeId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_replication_disable",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Disable replication",
                Description =
                    "Disables cross-cluster replication for a tree, pausing shipping of new mutations. Never purges "
                    + "already-replicated peer data and keeps the tree's fixed merge mode so a later re-enable is a "
                    + "fresh bootstrap. Idempotent. Subject to the fail-closed replication access gate. Requires "
                    + "replication control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static IDisposable StampCredential(IServiceProvider services)
    {
        var httpContext = services.GetService<IHttpContextAccessor>()?.HttpContext;
        if (httpContext is null)
        {
            return NullScope.Instance;
        }

        var credential = services.GetService<ILatticeApiMcpCredentialBridge>()?.Resolve(httpContext);
        // A null credential leaves the ambient context cleared (fail-closed): the
        // facade's access gate then denies the caller as anonymous.
        return LatticeCredentialContext.With(credential);
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
