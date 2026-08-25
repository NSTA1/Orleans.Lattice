using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// End-to-end integration test for T18 per-tenant WAL isolation against a live
/// single-silo cluster: a tenant whose <see cref="TenantPlacement"/> requests a
/// dedicated WAL provider has its trees pinned to that provider at registration,
/// so their WAL shards route to the dedicated in-memory store while the baseline
/// store stays empty; a shared tenant and a non-tenant tree both stay on the
/// baseline; and the pin is immutable once a tree is registered - a later
/// placement change does not re-place the existing tree.
/// </summary>
/// <remarks>
/// This fixture is authored by the T18 feature work but is RUN BY THE COORDINATOR
/// (it carries <c>[Category("Integration")]</c> and is excluded from the unit
/// filter). WAL landing is observed by polling the inspectable provider to a
/// deadline - the same convergence pattern the WAL-move integration suite uses -
/// never by asserting an ordering or a fixed delay.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class TenantWalPlacementIntegrationTests
{
    private readonly TenantWalPlacementClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private static async Task<long> WaitForHighestAsync(
        InMemoryWalStorageProvider provider, string physicalTreeId, int partition, long atLeast)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var highest = await provider.GetHighestOffsetAsync(physicalTreeId, partition, CancellationToken.None);
            if (highest >= atLeast)
            {
                return highest;
            }
            await Task.Delay(50);
        }
        return await provider.GetHighestOffsetAsync(physicalTreeId, partition, CancellationToken.None);
    }

    private static async Task WriteKeysAsync(ISystemLattice tree, string prefix, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await tree.SetAsync($"{prefix}-{i}", Encoding.UTF8.GetBytes($"value-{prefix}-{i}"));
        }
    }

    private async Task SeedTenantAsync(string tenant, TenantPlacement placement)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            placement,
            Clock(1),
            "t18-integration");
        await _fixture.Registry.PutAsync(record);

        // Make the seeded record visible to the placement resolver deterministically:
        // the resolver reads an in-memory snapshot (not a live registry), so warm it
        // before the tree is registered. This mirrors production convergence off the
        // change-feed without a wall-clock wait.
        await _fixture.WarmPlacementSnapshotAsync();
    }

    [Test]
    public async Task DedicatedWal_tenant_tree_routes_its_wal_to_the_dedicated_provider()
    {
        // A tenant that requires a dedicated WAL bound to the "wal-acme" catalog key.
        await SeedTenantAsync("acme", new TenantPlacement
        {
            WalProviderName = TenantWalProviders.DedicatedKey,
            DedicatedWal = true,
        });

        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("acme"), "orders");
        var tree = await _fixture.RegisterTreeAsync(treeId);
        var routing = await tree.GetRoutingAsync();
        var physical = routing.PhysicalTreeId;

        // Primary routing proof: the registry pinned every partition to the
        // dedicated provider key at registration.
        var placement = await _fixture.Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(TenantWalProviders.DedicatedKey),
            "a dedicated-WAL tenant's tree must pin its WAL partition to the dedicated provider key");

        // Physical proof: the tree's WAL shards write to the dedicated store, and
        // the baseline store never sees this tree's WAL.
        await WriteKeysAsync(_fixture.SystemTree(treeId), "k", 8);
        var dedicatedHighest = await WaitForHighestAsync(TenantWalProviders.Dedicated, physical, 0, 0);
        Assert.That(dedicatedHighest, Is.GreaterThanOrEqualTo(0),
            "the dedicated provider must hold the tenant tree's WAL");
        var baselineHighest = await TenantWalProviders.Baseline.GetHighestOffsetAsync(physical, 0, CancellationToken.None);
        Assert.That(baselineHighest, Is.EqualTo(-1),
            "the baseline provider must never see a dedicated tenant tree's WAL");
    }

    [Test]
    public async Task DedicatedWal_tenant_tree_registration_completes_without_deadlocking()
    {
        // Regression for the registry re-entrancy deadlock (coordinator FINDING 1):
        // resolving a dedicated-WAL tenant's placement at registration must NOT call
        // back into the registry / tree / ILattice subsystem from inside
        // RegisterAsync's turn. With the in-memory snapshot resolver the placement
        // read is a pure lookup, so registration completes promptly; a reintroduced
        // live registry read would re-enter the singleton registry grain and
        // self-deadlock until the 30s turn timeout.
        await SeedTenantAsync("regress-co", new TenantPlacement
        {
            WalProviderName = TenantWalProviders.DedicatedKey,
            DedicatedWal = true,
        });

        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("regress-co"), "orders");

        // A generous ceiling: registration is a handful of grain calls and finishes
        // well within it. The assertion is the ABSENCE of the self-deadlock, not a
        // timing expectation - no Task.Delay drives correctness here.
        var registration = _fixture.RegisterTreeAsync(treeId);
        Assert.That(
            async () => await registration.WaitAsync(TimeSpan.FromSeconds(30)),
            Throws.Nothing,
            "tenant-scoped tree registration must not deadlock the registry grain");

        var placement = await _fixture.Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(TenantWalProviders.DedicatedKey),
            "and registration must still pin the dedicated provider key");
    }

    [Test]
    public async Task Shared_tenant_tree_stays_on_the_baseline_provider()
    {
        await SeedTenantAsync("shared-co", TenantPlacement.Shared);

        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("shared-co"), "orders");
        var tree = await _fixture.RegisterTreeAsync(treeId);
        var routing = await tree.GetRoutingAsync();
        var physical = routing.PhysicalTreeId;

        // A shared tenant pins nothing: the partition resolves to the default key
        // and its WAL lands on the baseline provider, exactly as a non-tenant tree.
        var placement = await _fixture.Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey),
            "a shared tenant's tree must stay on the default provider key");

        await WriteKeysAsync(_fixture.SystemTree(treeId), "k", 4);
        var baselineHighest = await WaitForHighestAsync(TenantWalProviders.Baseline, physical, 0, 0);
        Assert.That(baselineHighest, Is.GreaterThanOrEqualTo(0),
            "a shared tenant tree's WAL must land on the baseline provider");
    }

    [Test]
    public async Task Non_tenant_tree_stays_on_the_baseline_provider()
    {
        // A plain, non-tenant tree id is never touched by the tenant resolver.
        var treeId = $"legacy-orders-{Guid.NewGuid():N}";
        var tree = await _fixture.RegisterTreeAsync(treeId);
        var routing = await tree.GetRoutingAsync();
        var physical = routing.PhysicalTreeId;

        var placement = await _fixture.Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey),
            "a non-tenant tree must be unaffected by tenancy placement");

        await WriteKeysAsync(_fixture.SystemTree(treeId), "k", 4);
        var baselineHighest = await WaitForHighestAsync(TenantWalProviders.Baseline, physical, 0, 0);
        Assert.That(baselineHighest, Is.GreaterThanOrEqualTo(0),
            "a non-tenant tree's WAL must land on the baseline provider");
    }

    [Test]
    public async Task Pinned_tree_keeps_its_dedicated_provider_after_a_later_placement_change()
    {
        // Seed a dedicated-WAL tenant and register its tree, pinning wal-acme.
        await SeedTenantAsync("immutable-co", new TenantPlacement
        {
            WalProviderName = TenantWalProviders.DedicatedKey,
            DedicatedWal = true,
        });

        var treeId = LatticeTenantTrees.Compose(TenantId.Parse("immutable-co"), "orders");
        await _fixture.RegisterTreeAsync(treeId);
        var before = await _fixture.Admin.GetWalPlacementAsync(treeId);
        Assert.That(before.Partitions[0].ProviderKey, Is.EqualTo(TenantWalProviders.DedicatedKey));

        // The tenant's placement later flips back to shared. In v1 the local
        // physical binding is IMMUTABLE for a tenant once its trees are placed: a
        // migration would need data movement and is out of scope, so re-registration
        // is idempotent and the already-placed tree keeps its dedicated pin.
        await SeedTenantAsync("immutable-co", TenantPlacement.Shared);
        await _fixture.RegisterTreeAsync(treeId);

        var after = await _fixture.Admin.GetWalPlacementAsync(treeId);
        Assert.That(after.Partitions[0].ProviderKey, Is.EqualTo(TenantWalProviders.DedicatedKey),
            "an already-placed tree must keep its dedicated pin after a later placement change (v1 immutability)");
    }
}
