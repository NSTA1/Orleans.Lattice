using Microsoft.Extensions.DependencyInjection;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// End-to-end coverage for <see cref="ITenantOverageStore.ListAsync"/> over the
/// real dogfooded overage tree.
/// </summary>
/// <remarks>
/// <para>
/// The enumeration projects every stored overage meter out of a scan of the
/// overage tree. Its unit fixture drives the store against a substituted
/// <c>ILattice</c> and covers the argument guards, the empty-increment no-op and
/// the optimistic-concurrency retry loop, but never the scan - and the one
/// existing enumeration test runs over an <i>empty</i> store, so the projection
/// loop body had never executed at all. An enumeration asserted only when it is
/// empty is satisfied by one that always yields nothing.
/// </para>
/// <para>
/// These run against the live tree rather than a substitute, because the loop
/// under test is the typed-scan projection itself; substituting the scan would
/// replace the very thing being covered.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class TenantOverageStoreEnumerationTests
{
    private readonly TenancyClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private ITenantOverageStore Store => _fixture.SiloServices.GetRequiredService<ITenantOverageStore>();

    private async Task<List<TenantOverageRecord>> ListAsync()
    {
        var records = new List<TenantOverageRecord>();
        await foreach (var record in Store.ListAsync())
        {
            records.Add(record);
        }

        return records;
    }

    [Test]
    public async Task ListAsync_yields_a_metered_tenants_record()
    {
        var tenant = TenantId.Parse("enum-single");

        await Store.MeterAsync(tenant, "east", Overage(100, 1, 10, 1));

        var listed = await ListAsync();

        var mine = listed.SingleOrDefault(r => r.Id == tenant);
        Assert.That(mine, Is.Not.Null,
            "A metered tenant must appear in the enumeration.");
        Assert.That(mine!.Fold(), Is.EqualTo(Overage(100, 1, 10, 1)),
            "The enumerated record must carry the metered amount, not an empty shell.");
    }

    [Test]
    public async Task ListAsync_yields_every_metered_tenant()
    {
        var first = TenantId.Parse("enum-multi-a");
        var second = TenantId.Parse("enum-multi-b");

        await Store.MeterAsync(first, "east", Overage(10, 1, 1, 1));
        await Store.MeterAsync(second, "west", Overage(20, 2, 2, 2));

        var ids = (await ListAsync()).Select(r => r.Id).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(ids, Does.Contain(first));
            Assert.That(ids, Does.Contain(second),
                "The scan must continue past its first entry.");
        });
    }

    [Test]
    public async Task ListAsync_reflects_the_converged_cross_cluster_fold()
    {
        // The enumeration must project the same converged record the point read
        // does; a projection that dropped a cluster slot would still satisfy the
        // per-tenant presence assertions above.
        var tenant = TenantId.Parse("enum-fold");

        await Store.MeterAsync(tenant, "east", Overage(100, 1, 10, 1));
        await Store.MeterAsync(tenant, "west", Overage(200, 2, 20, 2));

        var listed = (await ListAsync()).Single(r => r.Id == tenant);
        var read = await Store.GetAsync(tenant);

        Assert.That(read, Is.Not.Null);
        Assert.That(listed.Fold(), Is.EqualTo(read!.Fold()),
            "The enumerated projection and the point read must agree.");
    }

    [Test]
    public async Task ListAsync_yields_one_record_per_tenant_not_one_per_cluster_slot()
    {
        // The overage tree is keyed by tenant, so a tenant metered by two
        // clusters is still a single entry. This pins the scan's key shape.
        var tenant = TenantId.Parse("enum-onceper");

        await Store.MeterAsync(tenant, "east", Overage(1, 1, 1, 1));
        await Store.MeterAsync(tenant, "west", Overage(1, 1, 1, 1));

        var mine = (await ListAsync()).Where(r => r.Id == tenant).ToList();

        Assert.That(mine, Has.Count.EqualTo(1));
    }
}
