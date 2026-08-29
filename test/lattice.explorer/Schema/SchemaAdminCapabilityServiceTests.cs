using Grpc.Core;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// The Schema plugin's own access gate. It reproduces the coarse
/// control-plane-reachability gate the area used to read from the shared
/// capability record, now as the plugin-level decision keyed under
/// <see cref="SchemaPluginKeys.PluginId"/>, and keeps the per-tree capability
/// probe the panel drives its own action grey-out from.
/// </summary>
[TestFixture]
public class SchemaAdminCapabilityServiceTests
{
    private static readonly IExplorerPluginHostContext Context =
        PluginTestHost.Context(SchemaPluginKeys.PluginId);

    private static SchemaAdminCapabilityService Create(FakeSchemaAdminClient client) => new(client);

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
    public async Task ProbeAsync_probe_reachable_allows_the_plugin()
    {
        var client = new FakeSchemaAdminClient();
        var service = Create(client);

        var access = await service.ProbeAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(client.LastProbeTreeId, Is.EqualTo(SchemaAdminCapabilityService.CapabilityProbeTreeId));
        });
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
