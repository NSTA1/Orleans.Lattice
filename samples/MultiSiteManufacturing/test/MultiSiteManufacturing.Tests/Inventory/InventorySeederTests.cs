using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Inventory;
using MultiSiteManufacturing.Tests.Federation;

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
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, NullLogger<InventorySeeder>.Instance);

        await seeder.SeedAsync(CancellationToken.None);

        var parts = await lattice.ListPartsAsync();
        Assert.That(parts, Has.Count.EqualTo(InventorySeeder.TotalParts));
    }

    [Test]
    public async Task Seed_produces_expected_state_distribution()
    {
        var (router, _, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, NullLogger<InventorySeeder>.Instance);

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
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, NullLogger<InventorySeeder>.Instance);

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
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, NullLogger<InventorySeeder>.Instance);

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
    public async Task Seed_baseline_and_lattice_agree_post_seed()
    {
        var (router, baseline, lattice) = _fixture.NewRouter();
        var seeder = new InventorySeeder(router, _fixture.GrainFactory, NullLogger<InventorySeeder>.Instance);

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
