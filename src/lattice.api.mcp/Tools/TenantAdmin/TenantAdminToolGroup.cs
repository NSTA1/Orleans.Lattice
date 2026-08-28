using System.ComponentModel;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The tenant-admin tool module: an <see cref="ILatticeApiMcpToolGroup"/> for
/// <see cref="LatticeApiMcpGroup.TenantAdmin"/> whose tools are thin adapters over
/// the <see cref="ILatticeTenantAdmin"/> tenant lifecycle facade and the
/// <see cref="ILatticeTenantRegionAdmin"/> region-residency facade. The tenant
/// lifecycle is all-mutating - there is no read-only inspect operation - so the
/// group contributes its control tools (<c>lattice_tenant_create</c>,
/// <c>lattice_tenant_suspend</c>, <c>lattice_tenant_resume</c>,
/// <c>lattice_tenant_delete</c>, <c>lattice_tenant_set_quotas</c>,
/// <c>lattice_tenant_authorize_regions</c>, <c>lattice_tenant_set_residency</c>,
/// <c>lattice_tenant_region_status</c>) only when tenant-admin control is opted in
/// via <see cref="LatticeApiMcpOptions.EnableTenantAdminControlTools"/> or
/// <c>AddTenantAdminTools(enableControl: true)</c>. Every tool is annotated
/// destructive and non-read-only except the read-only
/// <c>lattice_tenant_region_status</c>.
/// </summary>
/// <remarks>
/// <para>
/// The tools are built <b>once</b> in the constructor and are stateless: each
/// resolves the facade from the tool invocation's request service provider and
/// stamps the caller credential - bridged from the request's authenticated
/// principal - onto the ambient <see cref="LatticeCredentialContext"/> for the
/// duration of the facade call, so the facade's own fail-closed tenant-admin
/// access gate resolves the caller's subject and authorizes every mutation. The
/// module adds no authorization path of its own.
/// </para>
/// <para>
/// Every tenant lifecycle operation mutates cluster state (delete cascades the
/// tenant's trees; set-quotas rewrites the tenant's capacity allocation), so all
/// tools carry <c>destructiveHint</c>. The group
/// itself is advertised only to a caller whose effective permissions grant
/// <see cref="LatticeOperation.Admin"/> - an agent without the grant is offered no
/// tenant-admin tools at all - and only when the host has opted the group in, so a
/// cluster that never calls <c>AddTenantAdminTools</c> exposes nothing.
/// </para>
/// <para>
/// The three region-residency tools keep the facade's <b>two-tier</b>
/// authorization intact and do not widen either tier:
/// <c>lattice_tenant_authorize_regions</c> is operator-only, while
/// <c>lattice_tenant_set_residency</c> and <c>lattice_tenant_region_status</c> are
/// operator-or-tenant-admin. The group advertises them together so an agent can
/// discover the whole workflow, but the server decides each call on its own tier.
/// </para>
/// </remarks>
internal sealed class TenantAdminToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the tenant-admin tool set once from the resolved MCP options,
    /// contributing the mutating control tools only when tenant-admin control is
    /// opted in.
    /// </summary>
    /// <param name="options">The resolved MCP binding options. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    public TenantAdminToolGroup(IOptions<LatticeApiMcpOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Tools = BuildTools(options.Value.EnableTenantAdminControlTools);
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.TenantAdmin;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> BuildTools(bool enableControl)
    {
        if (!enableControl)
        {
            return [];
        }

        return
        [
            CreateCreateTool(),
            CreateSuspendTool(),
            CreateResumeTool(),
            CreateDeleteTool(),
            CreateSetQuotasTool(),
            CreateAuthorizeRegionsTool(),
            CreateSetResidencyTool(),
            CreateRegionStatusTool(),
        ];
    }

    private static McpServerTool CreateCreateTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id to create. Must be a valid, non-empty tenant id that is not already registered.")] string tenantId,
                CancellationToken cancellationToken,
                [Description("The tenant-admin subject ids to seed onto the new tenant, deciding who can subsequently see it. Omit or leave empty to seed the calling subject.")] string[]? adminSubjects = null) =>
            {
                using var scope = StampCredential(context.Services!);
                var admin = context.Services!.GetRequiredService<ILatticeTenantAdmin>();
                return TenantAdminToolInvocations.CreateTenantAsync(admin, tenantId, adminSubjects, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_create",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Create tenant",
                Description =
                    "Registers a new tenant in the active status. Fails closed if a tenant with the same id "
                    + "already exists (create is not an idempotent upsert, so it never resets or reuses another "
                    + "tenant). Tenant visibility on the read-only surface resolves from tenant-admin subject "
                    + "membership, so adminSubjects decides who can subsequently see the tenant through "
                    + "lattice_tenant_list / lattice_tenant_get: omit it (or pass an empty list) to seed the "
                    + "calling subject, so a create followed by a read-back works; pass an explicit list to hand "
                    + "the tenant to other identities instead - the caller is then not added. A tenant left with "
                    + "no admin subjects at all is mutable by a platform operator but invisible to list and get "
                    + "for every caller. The result reports the subjects that were seeded. Subject to the "
                    + "fail-closed tenant-admin access gate. Requires tenant-admin control to be enabled on the "
                    + "server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateSuspendTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id to suspend. Must be a valid, non-empty tenant id.")] string tenantId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var admin = context.Services!.GetRequiredService<ILatticeTenantAdmin>();
                return TenantAdminToolInvocations.SuspendTenantAsync(admin, tenantId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_suspend",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Suspend tenant",
                Description =
                    "Suspends a tenant, transitioning it to the suspended status. Idempotent - suspending an "
                    + "already-suspended tenant reports changed=false and makes no change. The reserved default "
                    + "tenant can never be suspended. Fails closed if the tenant is not registered. Subject to the "
                    + "fail-closed tenant-admin access gate. Requires tenant-admin control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateResumeTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id to resume. Must be a valid, non-empty tenant id.")] string tenantId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var admin = context.Services!.GetRequiredService<ILatticeTenantAdmin>();
                return TenantAdminToolInvocations.ResumeTenantAsync(admin, tenantId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_resume",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Resume tenant",
                Description =
                    "Resumes a tenant, transitioning it back to the active status. Idempotent - resuming an "
                    + "already-active tenant reports changed=false and makes no change. Fails closed if the tenant "
                    + "is not registered. Subject to the fail-closed tenant-admin access gate. Requires tenant-admin "
                    + "control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateDeleteTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id to delete. Must be a valid, non-empty tenant id.")] string tenantId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var admin = context.Services!.GetRequiredService<ILatticeTenantAdmin>();
                return TenantAdminToolInvocations.DeleteTenantAsync(admin, tenantId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_delete",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Delete tenant",
                Description =
                    "Deletes a tenant, cascading the delete to every tree the tenant owns (each of the tenant's "
                    + "trees is soft-deleted) before the tenant's registry record is removed. The result reports the "
                    + "number of trees cascaded. The reserved default tenant can never be deleted. Fails closed if "
                    + "the tenant is not registered. Subject to the fail-closed tenant-admin access gate. Requires "
                    + "tenant-admin control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateSetQuotasTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id whose quotas to author. Must be a valid, non-empty tenant id that is registered and is not the reserved default tenant.")] string tenantId,
                CancellationToken cancellationToken,
                [Description("The maximum total stored value bytes, or null for unbounded on this dimension.")] long? maxBytes = null,
                [Description("The maximum total live key count, or null for unbounded on this dimension.")] long? maxKeys = null,
                [Description("The maximum resident memory in bytes, or null for unbounded on this dimension.")] long? maxMemoryBytes = null,
                [Description("The maximum number of trees the tenant may own, or null for unbounded on this dimension.")] long? maxTreeCount = null,
                [Description("The maximum sustained operations per second, or null for unbounded on this dimension.")] long? maxOpsPerSecond = null,
                [Description("The transient burst headroom above the bounded ceilings, as a percentage (0 for none). Must be non-negative.")] int burstPercent = 0) =>
            {
                using var scope = StampCredential(context.Services!);
                var admin = context.Services!.GetRequiredService<ILatticeTenantAdmin>();
                var quotas = new TenantQuotasDescriptor
                {
                    MaxBytes = maxBytes,
                    MaxKeys = maxKeys,
                    MaxMemoryBytes = maxMemoryBytes,
                    MaxTreeCount = maxTreeCount,
                    MaxOpsPerSecond = maxOpsPerSecond,
                    BurstPercent = burstPercent,
                };
                return TenantAdminToolInvocations.SetTenantQuotasAsync(admin, tenantId, quotas, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_set_quotas",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Set tenant quotas",
                Description =
                    "Authors a tenant's resource quotas and burst allowance, replacing whatever quotas the tenant "
                    + "currently carries. Each resource ceiling (maxBytes, maxKeys, maxMemoryBytes, maxTreeCount, "
                    + "maxOpsPerSecond) is null for unbounded on that dimension; pass every dimension null to lift a "
                    + "tenant's caps again. burstPercent is the transient headroom above the bounded ceilings and must "
                    + "be non-negative. The reserved default tenant can never be given quotas and fails closed. Fails "
                    + "closed if the tenant is not registered. Subject to the fail-closed tenant-admin access gate. "
                    + "Requires tenant-admin control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateAuthorizeRegionsTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id whose allowed region set to author. Must be a valid, non-empty tenant id that is registered.")] string tenantId,
                [Description("The complete desired allowed region set. This is a replacement, not a delta: a currently-allowed region absent from this list is revoked.")] string[] allowedRegions,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var regionAdmin = context.Services!.GetRequiredService<ILatticeTenantRegionAdmin>();
                return TenantAdminToolInvocations.AuthorizeAllowedRegionsAsync(
                    regionAdmin, tenantId, allowedRegions ?? [], cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_authorize_regions",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Authorize tenant regions",
                Description =
                    "Authors the set of regions a tenant is allowed to place residency in. This is an OPERATOR "
                    + "action: the server authorizes it as cluster-wide admin on the reserved auth policy tree and "
                    + "denies every non-operator caller, including a tenant admin, regardless of the data-plane "
                    + "default effect. allowedRegions is the complete desired set, not a delta - regions absent from "
                    + "it are revoked. Revoking a region the tenant is still resident in is refused fail-closed "
                    + "(residency must always stay a subset of the allowed set), so drain the region with "
                    + "lattice_tenant_set_residency first. Fails closed if the tenant is not registered. Requires "
                    + "tenant-admin control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateSetResidencyTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id whose residency to author. Must be a valid, non-empty tenant id that is registered.")] string tenantId,
                [Description("The complete desired residency set. This is a replacement, not a delta: a currently-resident region absent from this list begins draining. Must not be empty.")] string[] residencyRegions,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var regionAdmin = context.Services!.GetRequiredService<ILatticeTenantRegionAdmin>();
                return TenantAdminToolInvocations.SetResidencyAsync(
                    regionAdmin, tenantId, residencyRegions ?? [], cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_set_residency",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Set tenant residency",
                Description =
                    "Moves a tenant into and out of regions within its operator-authorized allowed set. This is a "
                    + "TENANT-ADMIN action: the server authorizes the caller as the platform operator OR a live admin "
                    + "subject on the tenant record, independent of the data-plane default effect. residencyRegions is "
                    + "the complete desired set, not a delta - newly listed regions begin provisioning and backfilling, "
                    + "and currently-resident regions absent from it begin draining. Transitions are asynchronous, so a "
                    + "newly added region reports Provisioning here, not Online; poll lattice_tenant_region_status until "
                    + "it reaches Online before routing traffic there. Every region in the set must already be allowed "
                    + "(use lattice_tenant_authorize_regions first), and the change may never remove the last resident "
                    + "region - both are refused fail-closed. Fails closed if the tenant is not registered. Requires "
                    + "tenant-admin control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateRegionStatusTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tenant id to report on. Must be a valid, non-empty tenant id that is registered.")] string tenantId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var regionAdmin = context.Services!.GetRequiredService<ILatticeTenantRegionAdmin>();
                return TenantAdminToolInvocations.GetTenantRegionStatusAsync(regionAdmin, tenantId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_tenant_region_status",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read tenant region status",
                Description =
                    "Reads a tenant's per-region residency lifecycle: one row per region that is either in the "
                    + "tenant's operator-authorized allowed set or carries a non-None status, ordered by region id. "
                    + "Read-only, and a TENANT-ADMIN action: the server authorizes the caller as the platform operator "
                    + "OR a live admin subject on the tenant record. Use it to watch a residency change reach Online "
                    + "after lattice_tenant_set_residency, and to see which regions an operator has authorized but the "
                    + "tenant has not yet moved into. Fails closed if the tenant is not registered. Requires "
                    + "tenant-admin control to be enabled on the server.",
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
