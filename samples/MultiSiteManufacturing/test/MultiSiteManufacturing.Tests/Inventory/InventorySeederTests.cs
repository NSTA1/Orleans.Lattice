using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Inventory;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Tests.Inventory;

/// <summary>
/// Verifies <see cref="InventorySeeder"/> deterministically produces the
/// documented seed spread and is safe to invoke more than once
/// (idempotent via <see cref="IInventorySeedStateGrain"/>).
/// </summary>
[TestFixture]
public sealed class InventorySeederTests
{
    private FederationTestClusterFixture _fixture = null!;

    // The CRDT-history seeding step writes through a PartCrdtStore bound to the
    // primary "us" silo, matching the production wiring where only that silo
    // runs the seeder.
    private static readonly SiloIdentity SeederSilo = new("a", IsPrimary: true);

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [Test]
    public async Task Seed_populates_exactly_five_parts()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);

        var parts = await lattice.ListPartsAsync();
        Assert.That(parts, Has.Count.EqualTo(InventorySeeder.TotalParts));
    }

    [Test]
    public async Task Seed_produces_expected_state_distribution()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);

        var counts = new Dictionary<ComplianceState, int>();
        foreach (var serial in await lattice.ListPartsAsync())
        {
            var state = await lattice.GetStateAsync(serial);
            counts[state] = counts.GetValueOrDefault(state) + 1;
        }

        // 2 Nominal (forge-only + FAI signed), 1 FlaggedForReview, 1 Rework, 1 Scrap.
        Assert.That(counts.GetValueOrDefault(ComplianceState.Nominal), Is.EqualTo(2));
        Assert.That(counts.GetValueOrDefault(ComplianceState.FlaggedForReview), Is.EqualTo(1));
        Assert.That(counts.GetValueOrDefault(ComplianceState.Rework), Is.EqualTo(1));
        Assert.That(counts.GetValueOrDefault(ComplianceState.Scrap), Is.EqualTo(1));
    }

    [Test]
    public async Task Seed_is_idempotent_across_invocations()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);
        var firstCount = (await lattice.ListPartsAsync()).Count;
        var firstFactCount = 0;
        foreach (var serial in await lattice.ListPartsAsync())
        {
            firstFactCount += (await lattice.GetFactsAsync(serial)).Count;
        }

        await seeder.SeedAsync(CancellationToken.None);
        var secondCount = (await lattice.ListPartsAsync()).Count;
        var secondFactCount = 0;
        foreach (var serial in await lattice.ListPartsAsync())
        {
            secondFactCount += (await lattice.GetFactsAsync(serial)).Count;
        }

        Assert.That(secondCount, Is.EqualTo(firstCount));
        Assert.That(secondFactCount, Is.EqualTo(firstFactCount));
    }

    [Test]
    public async Task Seed_serials_are_deterministic()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);

        var parts = await lattice.ListPartsAsync();
        var serials = parts.Select(p => p.Value).ToHashSet();
        for (var i = 1; i <= InventorySeeder.TotalParts; i++)
        {
            var expected = $"HPT-BLD-S1-2028-{i:D5}";
            Assert.That(serials, Does.Contain(expected), $"missing serial {expected}");
        }
    }

    [Test]
    public async Task Seed_reseeds_when_marker_set_but_lattice_empty()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        // Simulate a durable seed marker that survived a restart which wiped
        // the (non-durable) lattice fact tree: set the flag, leave the tree
        // empty. The seeder must re-seed rather than trust the stale marker.
        var seedFlag = _fixture.GrainFactory.GetGrain<IInventorySeedStateGrain>(IInventorySeedStateGrain.SingletonKey);
        await seedFlag.TryMarkSeededAsync();
        Assert.That(await lattice.ListPartsAsync(), Is.Empty, "precondition: lattice starts empty");

        await seeder.SeedAsync(CancellationToken.None);

        var parts = await lattice.ListPartsAsync();
        Assert.That(parts, Has.Count.EqualTo(InventorySeeder.TotalParts),
            "seeder should re-seed when the marker is set but the lattice tree is empty");
    }

    [Test]
    public async Task Seed_populates_crdt_change_history_for_the_showcase_part()
    {
        var (router, _, _) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-00002");
        var store = new PartCrdtStore(_fixture.GrainFactory, SeederSilo);

        // Final converged state: the last operator handoff wins; the two
        // removed labels are gone, leaving the live OR-Set membership.
        var op = await store.GetOperatorAsync(serial);
        Assert.That(op?.Value, Is.EqualTo(OperatorId.Demo.Value));
        Assert.That(await store.GetLabelsAsync(serial), Is.EquivalentTo(new[] { "priority", "qa-hold" }));

        // The operator last-writer-wins key carries a multi-revision timeline,
        // so the Explorer History tab has successive values to render.
        var operatorTree = _fixture.GrainFactory.GetGrain<ILattice>(PartCrdtStore.OperatorTreeId);
        var history = await operatorTree.ScanEntryHistoryAsync(serial.Value, null, null, 100, null, CancellationToken.None);
        Assert.That(history.Revisions.Count, Is.GreaterThanOrEqualTo(2),
            "the showcase part's operator key should expose multiple revisions");
    }

    [Test]
    public async Task Seed_baseline_and_lattice_agree_post_seed()
    {
        var (router, baseline, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, new PartCrdtStore(_fixture.GrainFactory, SeederSilo), NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);

        foreach (var serial in await lattice.ListPartsAsync())
        {
            var baselineState = await baseline.GetStateAsync(serial);
            var latticeState = await lattice.GetStateAsync(serial);
            Assert.That(baselineState, Is.EqualTo(latticeState),
                $"baseline/lattice diverged for {serial}");
        }
    }
}
