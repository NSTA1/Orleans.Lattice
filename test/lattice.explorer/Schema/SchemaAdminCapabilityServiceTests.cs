using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// The Schema plugin's own access gate: the coarse schema authority keyed under
/// <see cref="SchemaPluginKeys.PluginId"/>, plus the per-tree capability probe
/// the panel drives its own action grey-out from.
/// </summary>
/// <remarks>
/// The coarse gate reads the capability flags rather than merely observing that
/// the probe RPC completed (issue #1854): "the schema control endpoint is
/// reachable" is not a grant, and reporting it as one invited a caller with no
/// schema authority into a surface every action of which the server refuses.
/// </remarks>
[TestFixture]
public class SchemaAdminCapabilityServiceTests
{
    private static readonly IExplorerPluginHostContext Context =
        PluginTestHost.Context(SchemaPluginKeys.PluginId);

    /// <summary>The capability set of a caller who holds some schema authority.</summary>
    private static LatticeSchemaCapabilities Granted => new()
    {
        TreeId = SchemaAdminCapabilityService.CapabilityProbeTreeId,
        CanViewPolicy = true,
    };

    private static IExplorerAuthSession SignedIn(bool authenticated = true)
    {
        var session = Substitute.For<IExplorerAuthSession>();
        session.IsAuthenticated.Returns(authenticated);
        return session;
    }

    private static SchemaAdminCapabilityService Create(
        FakeSchemaAdminClient client,
        bool authenticated = true) => new(client, SignedIn(authenticated));

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new SchemaAdminCapabilityService(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ProbeAsync_null_context_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(async () => await service.ProbeAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ProbeAsync_a_schema_grant_allows_the_plugin()
    {
        var client = new FakeSchemaAdminClient { CapabilitiesResult = Granted };
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(client.LastProbeTreeId, Is.EqualTo(SchemaAdminCapabilityService.CapabilityProbeTreeId));
        });
    }

    [Test]
    public async Task ProbeAsync_a_reachable_endpoint_without_a_schema_grant_denies_the_plugin()
    {
        // The probe answers an all-false capability set rather than throwing when
        // the caller holds nothing, so "the RPC completed" is not a grant.
        var client = new FakeSchemaAdminClient();
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.Remedy.Permission, Is.EqualTo("Admin"));
            Assert.That(access.Remedy.Audience, Is.EqualTo("a platform administrator"));
        });
    }

    [Test]
    public async Task ProbeAsync_an_anonymous_caller_is_invited_to_sign_in_rather_than_denied()
    {
        var client = new FakeSchemaAdminClient();
        var service = Create(client, authenticated: false);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
    }

    [Test]
    public async Task ProbeAsync_denied_denies_the_plugin()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_transport_failure_denies_the_plugin()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_unconfigured_session_denies_the_plugin()
    {
        var client = new FakeSchemaAdminClient
        {
            ProbeThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task ProbeAsync_never_reports_unavailable_so_a_denied_area_greys_out_rather_than_hiding()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.That(access.IsVisible, Is.True);
    }

    [Test]
    public async Task A_denied_plugin_re_opens_when_the_control_endpoint_becomes_reachable_again()
    {
        // The allowed -> denied -> allowed transition. It is the shape that has
        // twice turned an idempotent initializer into a second-call no-op, so
        // the gate is asserted to make the round trip rather than only the first
        // leg of it.
        var client = new FakeSchemaAdminClient { CapabilitiesResult = Granted };
        var service = Create(client);
        var first = await service.ProbeAsync(Context);

        client.ProbeThrows = new RpcException(new Status(StatusCode.Unavailable, "gone"));
        var denied = await service.ProbeAsync(Context);

        client.ProbeThrows = null;
        var reopened = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(first.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(denied.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(reopened.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
        });
    }

    [Test]
    public async Task ProbeTreeAsync_maps_all_capability_flags()
    {
        var client = new FakeSchemaAdminClient
        {
            CapabilitiesResult = new LatticeSchemaCapabilities
            {
                TreeId = "t",
                CanViewPolicy = true,
                CanViewDeadLetters = true,
                CanViewVersionConfig = true,
                CanViewRemediationStatus = true,
                CanScanCompliance = true,
                CanManagePolicy = true,
                CanManageVersion = true,
                CanRemediate = true,
            },
        };
        var service = Create(client);

        var snapshot = await service.ProbeTreeAsync("t");

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.CanViewPolicy, Is.True);
            Assert.That(snapshot.CanViewDeadLetters, Is.True);
            Assert.That(snapshot.CanViewVersionConfig, Is.True);
            Assert.That(snapshot.CanViewRemediationStatus, Is.True);
            Assert.That(snapshot.CanScanCompliance, Is.True);
            Assert.That(snapshot.CanManagePolicy, Is.True);
            Assert.That(snapshot.CanManageVersion, Is.True);
            Assert.That(snapshot.CanRemediate, Is.True);
            Assert.That(snapshot.HasAny, Is.True);
        });
    }

    [Test]
    public async Task ProbeTreeAsync_denied_fails_closed_to_none()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new LatticeAuthorizationDeniedException("denied") };
        var service = Create(client);

        var snapshot = await service.ProbeTreeAsync("t");

        Assert.That(snapshot.HasAny, Is.False);
    }

    [Test]
    public async Task ProbeTreeAsync_transport_failure_fails_closed_to_none()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var service = Create(client);

        var snapshot = await service.ProbeTreeAsync("t");

        Assert.That(snapshot, Is.SameAs(SchemaCapabilitySnapshot.None));
    }

    [Test]
    public async Task ProbeTreeAsync_unconfigured_session_fails_closed_to_none()
    {
        var client = new FakeSchemaAdminClient
        {
            ProbeThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var service = Create(client);

        var snapshot = await service.ProbeTreeAsync("t");

        Assert.That(snapshot, Is.SameAs(SchemaCapabilitySnapshot.None));
    }

    [Test]
    public void ProbeTreeAsync_empty_tree_throws()
    {
        var service = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ProbeTreeAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }
}
