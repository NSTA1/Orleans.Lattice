using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A <see cref="DelegatingMcpServerTool"/> decorator that enforces the coarse
/// <see cref="ILatticeApiMcpAuthorizer"/> for the wrapped tool, resolves the
/// optional per-call <c>region</c> selector, stamps the calling session's bridged
/// credential onto the ambient <see cref="LatticeCredentialContext"/> for the
/// duration of the tool's invocation, then delegates to the inner tool -
/// translating any escaping fault through <see cref="McpToolFaultTranslator"/> so
/// it is never masked.
/// </summary>
/// <remarks>
/// <para>
/// The discovery core wraps every facade-backed group tool in this decorator as
/// it assembles a session's tool collection, so every tool call is first checked
/// against the transport-level authorizer (fail-closed by default) and then runs
/// the adapted facade under the caller's own credential, whose per-tree /
/// per-key access gate authorizes the real caller. The base
/// <see cref="DelegatingMcpServerTool"/> forwards the advertised name, schema,
/// annotations, and metadata verbatim; this decorator overrides only the schema to
/// advertise the optional <c>region</c> property, so the wrapped tool is otherwise
/// indistinguishable from the inner tool to a client.
/// </para>
/// <para>
/// This decorator is also the single narrowest seam every facade-backed tool
/// invocation funnels through, so it is where both the shared fault translation
/// (issue #1352) and region targeting (issue #1364) live. Region handling is
/// strictly opt-in: when no <c>region</c> is supplied the invocation takes the
/// default path unchanged - no region resolution, no ambient scope, no result
/// annotation - so an existing call is byte-for-byte identical and allocation-free
/// versus before. When a <c>region</c> is supplied it is validated against the
/// <see cref="ILatticeApiMcpRegionRouter"/> for this tool's group (an unknown or
/// unreachable region yields a clean typed fault), the ambient
/// <see cref="LatticeApiMcpRegionScope"/> routes the outbound gRPC call to the
/// target region under the same forwarded caller credential (so the target
/// authorizes independently, fail-closed), and the result is annotated with the
/// region it was served from.
/// </para>
/// <para>
/// Catching around the inner invocation (rather than inside the adapter methods)
/// is essential: a method whose exception-handling table names an unloadable
/// satellite type throws <see cref="System.IO.FileNotFoundException"/> while it is
/// being JIT-compiled, before its own <c>try</c> block can run, so only a caller
/// that does not name that type can catch it. A
/// <see cref="System.OperationCanceledException"/> is rethrown unchanged to
/// preserve cancellation semantics.
/// </para>
/// </remarks>
internal sealed class CredentialStampingTool : DelegatingMcpServerTool
{
    private readonly LatticeApiMcpGroup _group;
    private readonly Tool _protocolTool;

    /// <summary>Wraps <paramref name="inner"/> with per-invocation credential stamping and region routing.</summary>
    /// <param name="inner">The facade-backed tool to run under the caller's credential.</param>
    /// <param name="group">The facade group the wrapped tool belongs to, for region validation.</param>
    public CredentialStampingTool(McpServerTool inner, LatticeApiMcpGroup group)
        : base(inner)
    {
        _group = group;
        _protocolTool = LatticeApiMcpRegionToolSchema.WithRegionProperty(base.ProtocolTool);
    }

    /// <inheritdoc />
    public override Tool ProtocolTool => _protocolTool;

    /// <inheritdoc />
    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var services = request.Services!;
        var toolName = ProtocolTool.Name;

        // Coarse transport gate: reject the call before touching a facade when
        // the registered authorizer (default-deny) does not permit this tool.
        var authorized = await McpToolAuthorizationGate
            .IsAuthorizedAsync(services, toolName, cancellationToken)
            .ConfigureAwait(false);
        if (!authorized)
        {
            throw new McpException(
                $"Caller is not authorized to invoke the '{toolName}' tool.");
        }

        var requestedRegion = ReadRequestedRegion(request);

        // Default path: no region supplied. Byte-for-byte unchanged and
        // allocation-free versus a non-region-aware binding.
        if (requestedRegion is null)
        {
            using var scope = McpToolCredentialScope.Stamp(services);
            return await InvokeInnerAsync(request, cancellationToken).ConfigureAwait(false);
        }

        // Explicit region: resolve fail-closed against this tool's group.
        var route = ResolveRegion(services, requestedRegion);
        if (!route.IsRouted)
        {
            throw new McpException(route.Fault!);
        }

        using var credentialScope = McpToolCredentialScope.Stamp(services);

        // A peer region is verified before it serves the call (when verification is
        // configured): its identity is proven to reach the expected cluster, so a
        // region mis-pointed at a shared/anycast endpoint is rejected fail-closed
        // rather than silently answering from the wrong cluster. The current region
        // is local and authoritative, so it is never probed.
        if (!route.IsDefault)
        {
            await EnsureRegionVerifiedAsync(services, route.ServedRegionId, cancellationToken)
                .ConfigureAwait(false);
        }

        var regionScope = route.IsDefault ? null : LatticeApiMcpRegionScope.Enter(route.ServedRegionId);
        try
        {
            var result = await InvokeInnerAsync(request, cancellationToken).ConfigureAwait(false);
            AnnotateServedRegion(result, route.ServedRegionId);
            return result;
        }
        finally
        {
            regionScope?.Dispose();
        }
    }

    private static async ValueTask EnsureRegionVerifiedAsync(
        IServiceProvider services, string regionId, CancellationToken cancellationToken)
    {
        var verifier = services.GetService<ILatticeApiMcpRegionIdentityVerifier>();
        if (verifier is null)
        {
            // Verification not configured: routing proceeds exactly as before.
            return;
        }

        var verdict = await verifier.VerifyAsync(regionId, cancellationToken).ConfigureAwait(false);
        if (verdict is RegionIdentityVerdict.Mismatch or RegionIdentityVerdict.Unreachable)
        {
            throw new McpException(
                $"Region '{regionId}' failed identity verification and cannot be targeted: its configured "
                + "endpoint does not reach the region's own cluster. This usually means the region is pointed "
                + "at a shared or anycast endpoint (for example an Azure Front Door endpoint that latency-routes "
                + "to the nearest region) rather than the region's direct endpoint. Call lattice_list_regions "
                + "for the regions this server can reach.");
        }
    }

    private LatticeApiMcpRegionRoute ResolveRegion(IServiceProvider services, string requestedRegion)
    {
        var router = services.GetService<ILatticeApiMcpRegionRouter>();
        if (router is null)
        {
            return LatticeApiMcpRegionRoute.Rejected(
                $"This server cannot target region '{requestedRegion}'; region targeting is not configured.");
        }

        return router.Resolve(requestedRegion, _group);
    }

    private async ValueTask<CallToolResult> InvokeInnerAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken)
    {
        try
        {
            return await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Preserve cancellation semantics - a cancelled call is not a fault to
            // translate into a tool error message.
            throw;
        }
        catch (Exception ex)
        {
            // The single narrowest seam every facade tool funnels through: no
            // unclassified fault reaches the SDK's generic mask. An already
            // actionable McpException is surfaced unchanged; everything else -
            // including a FileNotFoundException raised while JIT-compiling an
            // adapter that names a missing satellite assembly - is translated.
            throw McpToolFaultTranslator.Translate(ex);
        }
    }

    private static string? ReadRequestedRegion(RequestContext<CallToolRequestParams> request)
    {
        var arguments = request.Params?.Arguments;
        if (arguments is null
            || !arguments.TryGetValue(LatticeApiMcpRegionToolSchema.RegionPropertyName, out var value)
            || value.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        var region = value.GetString();
        return string.IsNullOrWhiteSpace(region) ? null : region;
    }

    private static void AnnotateServedRegion(CallToolResult result, string regionId)
    {
        if (result is null)
        {
            return;
        }

        result.Meta ??= new JsonObject();
        result.Meta[LatticeApiMcpRegionToolSchema.RegionPropertyName] = regionId;
    }
}
