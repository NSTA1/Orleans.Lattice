using System.ComponentModel;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The tenant self-awareness tool module: an <see cref="ILatticeApiMcpToolGroup"/>
/// whose tools are thin adapters over the read-only
/// <see cref="ILatticeTenantSelfService"/> facade. It contributes three read-only
/// tools that let an agent discover the tenant context it is operating in -
/// <c>lattice_tenant_current</c> (the tenant the caller's credential resolves to),
/// <c>lattice_tenant_list</c> (the tenants the caller may access), and
/// <c>lattice_tenant_get</c> (the read-only lifecycle and per-region residency of
/// one accessible tenant). Every tool is annotated read-only and non-destructive.
/// </summary>
/// <remarks>
/// <para>
/// <b>Activation is keyed purely on whether tenancy is enabled</b>, never on a new
/// opt-in flag of its own and never on the mutating group's
/// <see cref="LatticeApiMcpOptions.EnableTenantAdminControlTools"/> switch. The
/// read-only <see cref="ILatticeTenantSelfService"/> facade is registered only when
/// the tenant-admin API is wired, which in turn requires the tenancy add-on, so the
/// facade's presence is exactly the "tenancy is enabled" signal. When the facade is
/// absent (tenancy not enabled) the group contributes <b>no</b> tools, so a
/// non-tenancy deployment's MCP surface is byte-for-byte unchanged.
/// </para>
/// <para>
/// <b>It advertises under <see cref="LatticeApiMcpGroup.State"/></b>, the read-only
/// introspection group, rather than introducing a new discovery group. This is
/// deliberate: the permission-aware capability report and instructions iterate the
/// fixed <see cref="LatticeApiMcpGroup"/> set, so adding a new group member would
/// change that report for every deployment - including non-tenancy ones - which
/// would violate the byte-for-byte-unchanged invariant. Reusing the existing
/// read-only <c>State</c> bucket keeps the capability surface identical when
/// tenancy is off and reaches exactly the read-capable callers for whom read-only
/// tenant introspection is appropriate. The per-tenant, leak-free authorization is
/// enforced at the single narrowest seam - the facade - not at the transport
/// advertisement, so the choice of advertising group grants no visibility the
/// facade would not itself permit.
/// </para>
/// <para>
/// The tools are built <b>once</b> in the constructor and are stateless: each
/// resolves the facade from the tool invocation's request service provider and
/// stamps the caller credential - bridged from the request's authenticated
/// principal - onto the ambient <see cref="LatticeCredentialContext"/> for the
/// duration of the facade call, so the facade resolves the caller's subject and
/// scopes enumeration and inspection fail-closed. The module adds no authorization
/// path of its own; an anonymous or unauthorized caller sees only its own (default)
/// context, an empty accessible list, and a fail-closed not-found on inspect.
/// </para>
/// </remarks>
internal sealed class TenantSelfAwarenessToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the tenant self-awareness tool set once, contributing the three
    /// read-only tools only when the tenancy-gated
    /// <see cref="ILatticeTenantSelfService"/> facade is registered.
    /// </summary>
    /// <param name="facades">
    /// The registered tenant self-awareness facades (zero or one). Injected as an
    /// enumerable so the group can detect tenancy being enabled without failing to
    /// construct when the facade is absent. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="facades"/> is <c>null</c>.</exception>
    public TenantSelfAwarenessToolGroup(IEnumerable<ILatticeTenantSelfService> facades)
    {
        ArgumentNullException.ThrowIfNull(facades);
        var tenancyEnabled = facades.Any();
        Tools = tenancyEnabled ? BuildTools() : [];
    }

    /// <inheritdoc />
    /// <remarks>
    /// The read-only tenant self-awareness tools advertise under the read-only
    /// <see cref="LatticeApiMcpGroup.State"/> introspection group so the fixed
    /// capability surface is unchanged when tenancy is off; see the type remarks.
    /// </remarks>
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.State;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> BuildTools()
        =>
        [
            CreateCurrentTool(),
            CreateListTool(),
            CreateGetTool(),
        ];

    private static McpServerTool CreateCurrentTool()
        => McpServerTool.Create(
            (RequestContext<CallToolRequestParams> context, CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var service = context.Services!.GetRequiredService<ILatticeTenantSelfService>();
                return TenantSelfAwarenessToolInvocations.GetCurrentTenantAsync(service, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_current",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Current tenant",
                Description =
                    "Reports the tenant the calling credential is operating as - the tenant resolved from the "
                    + "caller's ambient context - together with its lifecycle status and whether it is the reserved "
                    + "default tenant. Read-only. A caller with no tenant in context resolves to the default tenant.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateListTool()
        => McpServerTool.Create(
            (RequestContext<CallToolRequestParams> context, CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var service = context.Services!.GetRequiredService<ILatticeTenantSelfService>();
                return TenantSelfAwarenessToolInvocations.ListAccessibleTenantsAsync(service, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_list",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "List accessible tenants",
                Description =
                    "Lists the tenants the calling credential is authorized to access, in ascending tenant-id "
                    + "order. Scoped fail-closed to the caller: it never includes a tenant the caller cannot see, "
                    + "so an anonymous or non-privileged caller under the default tenant gets an empty list. "
                    + "Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateGetTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id to inspect. Must be a valid, non-empty tenant id the caller is authorized to see.")] string tenantId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var service = context.Services!.GetRequiredService<ILatticeTenantSelfService>();
                return TenantSelfAwarenessToolInvocations.GetTenantAsync(service, tenantId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Get tenant status",
                Description =
                    "Reads the lifecycle status and per-region residency of one tenant the caller is authorized "
                    + "to see. Read-only. Fails closed with a not-found error when the tenant does not exist or the "
                    + "caller is not authorized to see it - the two cases are deliberately indistinguishable so no "
                    + "caller can probe for a tenant outside its authority.",
                ReadOnly = true,
                Destructive = false,
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
        // facade then resolves the caller as anonymous, which administers no tenant.
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
