using Grpc.Core;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Schema;

[TestFixture]
public class SchemaAdminCapabilityServiceTests
{
    private static (SchemaAdminCapabilityService Service, ExplorerCapabilityStore Store) Create(FakeSchemaAdminClient client)
    {
        var store = new ExplorerCapabilityStore();
        return (new SchemaAdminCapabilityService(client, store), store);
    }

    [Test]
    public void Constructor_null_client_throws()
    {
        Assert.That(() => new SchemaAdminCapabilityService(null!, new ExplorerCapabilityStore()), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(() => new SchemaAdminCapabilityService(new FakeSchemaAdminClient(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task RefreshAsync_probe_reachable_sets_coarse_allowed()
    {
        var client = new FakeSchemaAdminClient();
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.SchemaAllowed, Is.True);
            Assert.That(client.LastProbeTreeId, Is.EqualTo(SchemaAdminCapabilityService.CapabilityProbeTreeId));
        });
    }

    [Test]
    public async Task RefreshAsync_denied_leaves_coarse_disabled()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new LatticeAuthorizationDeniedException("denied") };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.SchemaAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_transport_failure_leaves_coarse_disabled()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.SchemaAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_unconfigured_session_leaves_coarse_disabled()
    {
        var client = new FakeSchemaAdminClient
        {
            ProbeThrows = new InvalidOperationException("explorer is not configured with an endpoint"),
        };
        var (service, store) = Create(client);

        await service.RefreshAsync();

        Assert.That(store.Current.SchemaAllowed, Is.False);
    }

    [Test]
    public async Task RefreshAsync_preserves_other_capability_fields()
    {
        var client = new FakeSchemaAdminClient();
        var store = new ExplorerCapabilityStore();
        store.Set(ExplorerCapabilities.Empty with { AuthAdminAllowed = true });
        var service = new SchemaAdminCapabilityService(client, store);

        await service.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(store.Current.SchemaAllowed, Is.True);
            Assert.That(store.Current.AuthAdminAllowed, Is.True);
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
        var (service, _) = Create(client);

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
        var (service, _) = Create(client);

        var snapshot = await service.ProbeTreeAsync("t");

        Assert.That(snapshot.HasAny, Is.False);
    }

    [Test]
    public async Task ProbeTreeAsync_transport_failure_fails_closed_to_none()
    {
        var client = new FakeSchemaAdminClient { ProbeThrows = new RpcException(new Status(StatusCode.Unavailable, "gone")) };
        var (service, _) = Create(client);

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
        var (service, _) = Create(client);

        var snapshot = await service.ProbeTreeAsync("t");

        Assert.That(snapshot, Is.SameAs(SchemaCapabilitySnapshot.None));
    }

    [Test]
    public void ProbeTreeAsync_empty_tree_throws()
    {
        var (service, _) = Create(new FakeSchemaAdminClient());

        Assert.That(() => service.ProbeTreeAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }
}
