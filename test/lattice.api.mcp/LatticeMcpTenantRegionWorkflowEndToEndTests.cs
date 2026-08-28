using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using NSubstitute;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// The full tenant region-residency workflow, driven end to end through a real
/// <see cref="McpClient"/> against an in-process Kestrel host wired with two
/// regions and the tenant-admin control tools. It walks the exact sequence issue
/// #1714 identifies as uncovered: an operator authorizes the tenant's allowed
/// regions, the tenant admin reads the per-region report and sees the
/// allowed-but-not-resident peer, sets residency, the region transitions to
/// <c>Online</c>, <c>lattice_list_regions</c> then advertises it annotated as
/// resident, and a tool call routed there succeeds where it was previously
/// refused.
/// </summary>
/// <remarks>
/// <para>
/// Marked <c>Integration</c>: it binds a loopback TCP port and drives the full MCP
/// streamable-HTTP handshake.
/// </para>
/// <para>
/// The residency <b>lifecycle driver</b> is explicitly out of scope for #1714, so
/// the fixture's <see cref="WorkflowRegionState"/> stands in for it with an
/// explicit <c>CompleteTransitions</c> step rather than depending on the real
/// driver's timing - nothing here is time- or ordering-sensitive. Likewise
/// <see cref="ResidencyAwareDataApi"/> stands in for the silo-side
/// <c>TenantGateEnforcer.EnforceResidency</c> gate, which lives behind the facade
/// boundary this head calls across; it refuses on exactly the same condition (the
/// tenant is not online in the serving region).
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeMcpTenantRegionWorkflowEndToEndTests
{
    private const string Tenant = "acme";
    private const string CurrentRegion = "us";
    private const string PeerRegion = "eu";

    [Test]
    public async Task Operator_authorizes_then_tenant_sets_residency_and_the_region_becomes_discoverable_and_routable()
    {
        var state = new WorkflowRegionState();
        await using var host = await StartHostAsync(state);
        await using var client = await ConnectAsync(host);

        // 1. Before anything is authorized the tenant sees only the region it is
        //    talking to - the peer topology is not disclosed.
        var initial = await ListRegionsAsync(client);
        Assert.That(initial, Is.EqualTo(new[] { CurrentRegion }),
            "An unauthorized tenant must not learn the peer topology.");

        // 2. ... and a call routed at the peer is refused, because the tenant is not
        //    online there.
        var refused = await GetAsync(client, PeerRegion);
        Assert.That(refused.IsError, Is.True,
            "Routing at a region the tenant is not online in must be refused.");

        // 3. The operator authorizes the allowed set.
        var authorized = await CallAsync(
            client,
            "lattice_tenant_authorize_regions",
            new Dictionary<string, object?>
            {
                ["tenantId"] = Tenant,
                ["allowedRegions"] = new[] { CurrentRegion, PeerRegion },
            });
        Assert.That(authorized.IsError, Is.Not.True, Text(authorized));
        Assert.That(
            authorized.StructuredContent!.Value.GetProperty("allowedRegions").EnumerateArray()
                .Select(e => e.GetString()),
            Is.EquivalentTo(new[] { CurrentRegion, PeerRegion }));

        // 4. The tenant admin reads the report and sees the peer as allowed but not
        //    yet resident.
        var report = await CallAsync(
            client, "lattice_tenant_region_status", new Dictionary<string, object?> { ["tenantId"] = Tenant });
        Assert.That(report.IsError, Is.Not.True, Text(report));
        var peerRow = report.StructuredContent!.Value.GetProperty("regions").EnumerateArray()
            .Single(r => r.GetProperty("regionId").GetString() == PeerRegion);
        Assert.Multiple(() =>
        {
            Assert.That(peerRow.GetProperty("isAllowed").GetBoolean(), Is.True);
            Assert.That(peerRow.GetProperty("status").GetString(), Is.EqualTo(nameof(TenantRegionLifecycleStatus.None)));
        });

        // 5. The peer is already discoverable at this point - it is actionable, even
        //    though the tenant is not resident there yet.
        var afterAuthorize = await ListRegionsAsync(client);
        Assert.That(afterAuthorize, Is.EqualTo(new[] { CurrentRegion, PeerRegion }),
            "An allowed region is actionable, so it must be advertised.");

        // 6. The tenant admin sets residency; the peer begins provisioning.
        var change = await CallAsync(
            client,
            "lattice_tenant_set_residency",
            new Dictionary<string, object?>
            {
                ["tenantId"] = Tenant,
                ["residencyRegions"] = new[] { CurrentRegion, PeerRegion },
            });
        Assert.That(change.IsError, Is.Not.True, Text(change));
        Assert.That(
            change.StructuredContent!.Value.GetProperty("addedRegions").EnumerateArray().Select(e => e.GetString()),
            Does.Contain(PeerRegion));

        // 7. The lifecycle advances to Online (driver out of scope - stepped here).
        state.CompleteTransitions();

        // 8. lattice_list_regions now advertises the peer annotated as resident.
        var listed = await CallAsync(client, "lattice_list_regions", null);
        var peerDescriptor = listed.StructuredContent!.Value.GetProperty("regions").EnumerateArray()
            .Single(r => r.GetProperty("regionId").GetString() == PeerRegion);
        var scope = peerDescriptor.GetProperty("tenantScope");
        Assert.Multiple(() =>
        {
            Assert.That(scope.GetProperty("tenantId").GetString(), Is.EqualTo(Tenant));
            Assert.That(scope.GetProperty("isAllowed").GetBoolean(), Is.True);
            Assert.That(scope.GetProperty("isResident").GetBoolean(), Is.True);
            Assert.That(scope.GetProperty("status").GetString(), Is.EqualTo(nameof(TenantRegionLifecycleStatus.Online)));
        });

        // 9. And the call that was refused in step 2 now succeeds.
        var served = await GetAsync(client, PeerRegion);
        Assert.Multiple(() =>
        {
            Assert.That(served.IsError, Is.Not.True, Text(served));
            Assert.That(served.Meta!["region"]!.GetValue<string>(), Is.EqualTo(PeerRegion));
        });
    }

    [Test]
    public async Task Residency_outside_the_allowed_set_is_refused_and_the_region_stays_undiscoverable()
    {
        var state = new WorkflowRegionState();
        await using var host = await StartHostAsync(state);
        await using var client = await ConnectAsync(host);

        var result = await CallAsync(
            client,
            "lattice_tenant_set_residency",
            new Dictionary<string, object?>
            {
                ["tenantId"] = Tenant,
                ["residencyRegions"] = new[] { PeerRegion },
            });

        Assert.That(result.IsError, Is.True, "Residency must stay a subset of the operator-authored allowed set.");
        Assert.That(await ListRegionsAsync(client), Is.EqualTo(new[] { CurrentRegion }));
    }

    [Test]
    public async Task An_unresolvable_tenant_standing_degrades_to_the_current_region_not_the_full_topology()
    {
        var state = new WorkflowRegionState { FailResolution = true };
        await using var host = await StartHostAsync(state);
        await using var client = await ConnectAsync(host);

        Assert.That(await ListRegionsAsync(client), Is.EqualTo(new[] { CurrentRegion }),
            "An unresolvable standing must fail closed, never widen back to the full topology.");
    }

    private static string Text(CallToolResult result)
        => result.Content.OfType<TextContentBlock>().FirstOrDefault()?.Text ?? string.Empty;

    private static async Task<string[]> ListRegionsAsync(McpClient client)
    {
        var result = await CallAsync(client, "lattice_list_regions", null);
        Assert.That(result.IsError, Is.Not.True, Text(result));
        return [.. result.StructuredContent!.Value.GetProperty("regions").EnumerateArray()
            .Select(r => r.GetProperty("regionId").GetString()!)];
    }

    private static ValueTask<CallToolResult> GetAsync(McpClient client, string region)
        => CallAsync(
            client,
            "lattice_data_get",
            new Dictionary<string, object?> { ["treeId"] = "t", ["key"] = "k", ["region"] = region });

    private static ValueTask<CallToolResult> CallAsync(
        McpClient client, string tool, IReadOnlyDictionary<string, object?>? arguments)
        => client.CallToolAsync(tool, arguments, cancellationToken: TestContext.CurrentContext.CancellationToken);

    private static async Task<McpClient> ConnectAsync(WebApplication host)
    {
        var transport = new HttpClientTransport(
            new HttpClientTransportOptions
            {
                Endpoint = new Uri(host.Urls.First(), UriKind.Absolute),
                TransportMode = HttpTransportMode.StreamableHttp,
            });
        return await McpClient.CreateAsync(
            transport, cancellationToken: TestContext.CurrentContext.CancellationToken);
    }

    private static async Task<WebApplication> StartHostAsync(WorkflowRegionState state)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseKestrel();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<ILatticeApiMcpCredentialBridge>(
            new WorkflowCredentialBridge());
        builder.Services.AddSingleton<ILatticeApiMcpPermissionResolver>(
            new WorkflowPermissionResolver(LatticeApiMcpAccessSet.None
                .With(LatticeApiMcpGroup.Data)
                .With(LatticeApiMcpGroup.TenantAdmin)));
        builder.Services.AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer());
        builder.Services.AddSingleton<ILatticeApiMcpActiveTenantBridge>(new WorkflowTenantBridge());
        builder.Services.AddSingleton<ILatticeDataApi>(ResidencyAwareDataApi(state));
        builder.Services.AddSingleton<ILatticeTenantRegionAdmin>(new WorkflowRegionAdmin(state));
        builder.Services.AddSingleton<ITenantRegionVisibilityResolver>(new WorkflowVisibilityResolver(state));

        builder.Services.AddSingleton<ILatticeApiMcpRegionRouter>(
            new LatticeApiMcpRegionRouter(CurrentRegion, new[]
            {
                new LatticeApiMcpRegionDefinition
                {
                    RegionId = CurrentRegion,
                    ClusterId = "cluster-us",
                    IsCurrent = true,
                    Groups = new Dictionary<LatticeApiMcpGroup, string?> { [LatticeApiMcpGroup.Data] = null },
                },
                new LatticeApiMcpRegionDefinition
                {
                    RegionId = PeerRegion,
                    ClusterId = "cluster-eu",
                    IsCurrent = false,
                    Groups = new Dictionary<LatticeApiMcpGroup, string?>
                    {
                        [LatticeApiMcpGroup.Data] = "https://eu-data:5001",
                    },
                },
            }));

        builder.Services.AddLatticeMcp(o =>
        {
            o.RequireAuthorization = false;
            o.EnableDataTools = true;
        });
        builder.Services.AddDataTools();
        builder.Services.AddTenantAdminTools(enableControl: true);

        var app = builder.Build();
        app.MapLatticeMcp();
        await app.StartAsync();
        return app;
    }

    /// <summary>
    /// The mutable per-tenant region state the whole fixture shares: the operator's
    /// allowed set and the tenant's per-region lifecycle status, plus a switch that
    /// makes standing unresolvable so the fail-closed path can be driven.
    /// </summary>
    private sealed class WorkflowRegionState
    {
        private readonly HashSet<string> _allowed = new(StringComparer.Ordinal);
        private readonly Dictionary<string, TenantRegionLifecycleStatus> _status = new(StringComparer.Ordinal);

        public bool FailResolution { get; init; }

        public IReadOnlyList<string> Allowed => [.. _allowed.Order(StringComparer.Ordinal)];

        public bool IsOnline(string regionId)
            => _status.TryGetValue(regionId, out var status) && status == TenantRegionLifecycleStatus.Online;

        public IReadOnlyList<TenantRegionStatusDescriptor> Rows()
            => [.. _allowed.Union(_status.Keys, StringComparer.Ordinal).Order(StringComparer.Ordinal)
                .Select(id => new TenantRegionStatusDescriptor
                {
                    RegionId = id,
                    Status = _status.TryGetValue(id, out var s) ? s : TenantRegionLifecycleStatus.None,
                    IsAllowed = _allowed.Contains(id),
                })];

        public IReadOnlyList<string> Authorize(IReadOnlyCollection<string> allowedRegions)
        {
            _allowed.Clear();
            foreach (var region in allowedRegions)
            {
                _allowed.Add(region);
            }

            return Allowed;
        }

        public (IReadOnlyList<string> Added, IReadOnlyList<string> Removed) SetResidency(
            IReadOnlyCollection<string> residencyRegions)
        {
            foreach (var region in residencyRegions)
            {
                if (!_allowed.Contains(region))
                {
                    throw new TenantRegionNotAllowedException(Tenant, region);
                }
            }

            if (residencyRegions.Count == 0)
            {
                throw new TenantLastRegionException(Tenant);
            }

            var desired = new HashSet<string>(residencyRegions, StringComparer.Ordinal);
            List<string> added = [];
            List<string> removed = [];

            foreach (var region in desired)
            {
                if (!IsResident(region))
                {
                    _status[region] = TenantRegionLifecycleStatus.Provisioning;
                    added.Add(region);
                }
            }

            foreach (var region in _status.Keys.ToArray())
            {
                if (!desired.Contains(region) && IsResident(region))
                {
                    _status[region] = TenantRegionLifecycleStatus.Draining;
                    removed.Add(region);
                }
            }

            return (added, removed);
        }

        /// <summary>
        /// Stands in for the residency lifecycle driver, which is out of scope for
        /// this change: promotes every provisioning region to online and completes
        /// every drain.
        /// </summary>
        public void CompleteTransitions()
        {
            foreach (var region in _status.Keys.ToArray())
            {
                _status[region] = _status[region] switch
                {
                    TenantRegionLifecycleStatus.Provisioning or TenantRegionLifecycleStatus.Backfilling
                        => TenantRegionLifecycleStatus.Online,
                    TenantRegionLifecycleStatus.Draining => TenantRegionLifecycleStatus.Removed,
                    var other => other,
                };
            }
        }

        private bool IsResident(string regionId)
            => _status.TryGetValue(regionId, out var status)
                && status is TenantRegionLifecycleStatus.Provisioning
                    or TenantRegionLifecycleStatus.Backfilling
                    or TenantRegionLifecycleStatus.Online;
    }

    private sealed class WorkflowRegionAdmin(WorkflowRegionState state) : ILatticeTenantRegionAdmin
    {
        public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
            string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)
            => Task.FromResult(new TenantRegionAuthorizationResult
            {
                TenantId = tenantId,
                AllowedRegions = state.Authorize(allowedRegions),
            });

        public Task<TenantResidencyChangeResult> SetResidencyAsync(
            string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)
        {
            var (added, removed) = state.SetResidency(residencyRegions);
            return Task.FromResult(new TenantResidencyChangeResult
            {
                TenantId = tenantId,
                AddedRegions = added,
                RemovedRegions = removed,
                Regions = state.Rows(),
            });
        }

        public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
            string tenantId, CancellationToken cancellationToken = default)
            => Task.FromResult(new TenantRegionStatusReport { TenantId = tenantId, Regions = state.Rows() });
    }

    private sealed class WorkflowVisibilityResolver(WorkflowRegionState state) : ITenantRegionVisibilityResolver
    {
        public bool IsActive => true;

        public ValueTask<TenantRegionVisibilityMap> ResolveAsync(
            TenantId tenant, CancellationToken cancellationToken = default)
        {
            if (state.FailResolution)
            {
                return new ValueTask<TenantRegionVisibilityMap>(TenantRegionVisibilityMap.Unresolved);
            }

            return new ValueTask<TenantRegionVisibilityMap>(TenantRegionVisibilityMap.Create(
                state.Rows().Select(row => new KeyValuePair<string, TenantRegionVisibility>(
                    row.RegionId,
                    new TenantRegionVisibility(row.IsAllowed, Map(row.Status))))));
        }

        private static TenantRegionResidencyStatus Map(TenantRegionLifecycleStatus status) => status switch
        {
            TenantRegionLifecycleStatus.Provisioning => TenantRegionResidencyStatus.Provisioning,
            TenantRegionLifecycleStatus.Backfilling => TenantRegionResidencyStatus.Backfilling,
            TenantRegionLifecycleStatus.Online => TenantRegionResidencyStatus.Online,
            TenantRegionLifecycleStatus.Draining => TenantRegionResidencyStatus.Draining,
            TenantRegionLifecycleStatus.Offline => TenantRegionResidencyStatus.Offline,
            TenantRegionLifecycleStatus.Removed => TenantRegionResidencyStatus.Removed,
            _ => TenantRegionResidencyStatus.None,
        };
    }

    /// <summary>
    /// Stands in for the silo-side <c>TenantGateEnforcer.EnforceResidency</c> gate,
    /// which lives behind the facade boundary this head calls across: it refuses a
    /// read served from a region the asserted tenant is not online in.
    /// </summary>
    private static ILatticeDataApi ResidencyAwareDataApi(WorkflowRegionState state)
    {
        var api = Substitute.For<ILatticeDataApi>();
        api.GetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var region = LatticeApiMcpRegionScope.Current ?? CurrentRegion;
                if (LatticeActiveTenantContext.IsActive && region != CurrentRegion && !state.IsOnline(region))
                {
                    throw new InvalidOperationException(
                        $"Tenant '{Tenant}' is not online in this serving region.");
                }

                return Task.FromResult(new DataReadResult
                {
                    TreeId = call.ArgAt<string>(0),
                    Key = call.ArgAt<string>(1),
                    Found = true,
                    Value = [1],
                });
            });
        return api;
    }

    private sealed class WorkflowCredentialBridge : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => new("agent");
    }

    private sealed class WorkflowTenantBridge : ILatticeApiMcpActiveTenantBridge
    {
        public TenantId? Resolve(HttpContext context) => TenantId.Parse(Tenant);
    }

    private sealed class WorkflowPermissionResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
            LatticeCredential credential, CancellationToken cancellationToken)
            => new(access);
    }
}
