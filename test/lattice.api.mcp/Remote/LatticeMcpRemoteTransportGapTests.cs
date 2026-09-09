using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Coverage for the remaining unexercised arms of the MCP remote transport
/// layer: the range-delete member of <see cref="GrpcLatticeDataApi"/>, the
/// per-group region advertisement built by
/// <see cref="LatticeMcpRemoteServiceCollectionExtensions.AddLatticeMcpRemote"/>,
/// and the disabled-header arm of
/// <see cref="LatticeApiMcpCredentialForwardingInterceptor"/>.
/// </summary>
/// <remarks>
/// Each of these is load-bearing but was reachable only through a configuration
/// no existing fixture set up: the region advertisement is only computed when the
/// router singleton is actually resolved, and no prior test both configured every
/// facade group and resolved the router, so the backup / auth / replication /
/// tenant-admin advertisement arms never ran.
/// </remarks>
[TestFixture]
public sealed class LatticeMcpRemoteTransportGapTests
{
    private static readonly FakeCallInvoker Idle = new(_ => throw new InvalidOperationException());

    private static LatticeApiMcpRemoteEndpoint Endpoint(string address)
        => new() { Endpoint = address, CallInvoker = Idle };

    private static GrpcLatticeDataApi DataAdapter(FakeCallInvoker invoker)
        => new(RemoteTestSupport.DataClient(invoker));

    private static DataRangeDeleteResult Result(int deleted = 0)
        => new() { TreeId = "orders", DeletedCount = deleted };

    private static DataRangeDeleteRequest Request()
        => new() { TreeId = "orders", StartInclusive = "k0", EndExclusive = "k9" };

    // ----- GrpcLatticeDataApi.DeleteRangeAsync -----

    [Test]
    public async Task DeleteRangeAsync_forwards_the_request_and_returns_the_result()
    {
        var expected = new DataRangeDeleteResult { TreeId = "orders", DeletedCount = 3 };
        var invoker = new FakeCallInvoker(_ => expected);
        var request = new DataRangeDeleteRequest
        {
            TreeId = "orders",
            StartInclusive = "k0",
            EndExclusive = "k9",
        };

        var result = await DataAdapter(invoker).DeleteRangeAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(invoker.LastRequest, Is.SameAs(request));
            Assert.That(result, Is.SameAs(expected));
        });
    }

    [Test]
    public void DeleteRangeAsync_null_request_throws()
        => Assert.That(
            async () => await DataAdapter(new FakeCallInvoker(_ => Result())).DeleteRangeAsync(null!),
            Throws.ArgumentNullException);

    [Test]
    public void DeleteRangeAsync_translates_permission_denied_to_the_facade_exception()
    {
        // The facade contract is a typed authorization failure, not a transport
        // fault: a denied range delete must surface exactly as it does in-silo, so
        // callers cannot mistake it for a transient RPC error and retry it.
        var invoker = new FakeCallInvoker(
            _ => new RpcException(new Status(StatusCode.PermissionDenied, "nope")));

        Assert.That(
            async () => await DataAdapter(invoker).DeleteRangeAsync(Request()),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void DeleteRangeAsync_leaves_other_rpc_faults_untranslated()
    {
        var invoker = new FakeCallInvoker(
            _ => new RpcException(new Status(StatusCode.Unavailable, "transient")));

        Assert.That(
            async () => await DataAdapter(invoker).DeleteRangeAsync(Request()),
            Throws.InstanceOf<RpcException>(),
            "only PermissionDenied maps to the facade's denial type");
    }

    // ----- Per-group region advertisement -----

    [Test]
    public void Current_region_advertises_every_configured_facade_group()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.RegionId = "us";
                o.State = Endpoint("https://state:5001");
                o.Data = Endpoint("https://data:5002");
                o.Backup = Endpoint("https://backup:5004");
                o.Auth = Endpoint("https://auth:5003");
                o.Replication = Endpoint("https://replication:5005");
                o.TreeAdmin = Endpoint("https://treeadmin:5006");
                o.TenantAdmin = Endpoint("https://tenant:5007");
            })
            .BuildServiceProvider();

        var current = provider.GetRequiredService<ILatticeApiMcpRegionRouter>()
            .Snapshot()
            .Single(r => r.IsCurrent);

        string? EndpointFor(string group) => current.Groups.Single(g => g.Group == group).Endpoint;

        Assert.Multiple(() =>
        {
            Assert.That(EndpointFor("state"), Is.EqualTo("https://state:5001"));
            Assert.That(EndpointFor("data"), Is.EqualTo("https://data:5002"));
            Assert.That(EndpointFor("backup"), Is.EqualTo("https://backup:5004"));
            Assert.That(EndpointFor("auth"), Is.EqualTo("https://auth:5003"));
            Assert.That(EndpointFor("replication"), Is.EqualTo("https://replication:5005"));
            Assert.That(EndpointFor("treeadmin"), Is.EqualTo("https://treeadmin:5006"));
            Assert.That(EndpointFor("tenantadmin"), Is.EqualTo("https://tenant:5007"));
        });
    }

    [Test]
    public void Peer_region_advertises_only_the_groups_it_configures()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.RegionId = "us";
                o.State = Endpoint("https://state:5001");
                o.Backup = Endpoint("https://backup:5004");
                o.Auth = Endpoint("https://auth:5003");
                o.Replication = Endpoint("https://replication:5005");
                o.TenantAdmin = Endpoint("https://tenant:5007");
                o.Regions.Add(new LatticeApiMcpRemoteRegionOptions
                {
                    RegionId = "eu",
                    State = Endpoint("https://eu-state:5001"),
                    Auth = Endpoint("https://eu-auth:5003"),
                });
            })
            .BuildServiceProvider();

        var peer = provider.GetRequiredService<ILatticeApiMcpRegionRouter>()
            .Snapshot()
            .Single(r => !r.IsCurrent);

        Assert.Multiple(() =>
        {
            Assert.That(peer.Groups.Single(g => g.Group == "auth").Endpoint, Is.EqualTo("https://eu-auth:5003"));
            Assert.That(
                peer.Groups.Single(g => g.Group == "backup").Available,
                Is.False,
                "a peer must never inherit this head's own group routes");
            Assert.That(peer.Groups.Single(g => g.Group == "replication").Available, Is.False);
            Assert.That(peer.Groups.Single(g => g.Group == "tenantadmin").Available, Is.False);
        });
    }

    // ----- Credential forwarding with the header disabled -----

    [Test]
    public async Task Resolved_credential_is_not_stamped_when_the_header_name_is_cleared()
    {
        // Clearing the header name is the documented way to turn credential
        // forwarding off for a deployment whose edge injects its own. The
        // credential still resolves; it simply must not reach the wire.
        var invoker = new FakeCallInvoker(_ => new TreeCatalogPage());
        var interceptor = new LatticeApiMcpCredentialForwardingInterceptor(
            new HeaderlessCredentialSource(new LatticeCredential("tok-123")),
            RemoteTestSupport.OptionsMonitor(o => o.CredentialHeaderName = string.Empty));

        await LatticeStateApiGrpcClient.Create(invoker.Intercept(interceptor), RemoteTestSupport.Serializer)
            .ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(invoker.CallCount, Is.EqualTo(1), "the call must still go out");
            Assert.That(
                invoker.LastHeaders,
                Is.Null.Or.Empty,
                "with no header name configured the outbound call carries no forwarded credential");
        });
    }

    /// <summary>A credential source that always resolves the supplied credential.</summary>
    private sealed class HeaderlessCredentialSource(LatticeCredential credential)
        : ILatticeApiMcpRemoteCredentialSource
    {
        public LatticeCredential? ResolveOutbound() => credential;
    }
}
