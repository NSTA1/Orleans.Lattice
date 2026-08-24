using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// End-to-end integration tests for <see cref="ITenantRegistry"/> over the real
/// dogfooded <c>sys-tenant-*</c> trees: the default tenant is seeded on first
/// use, a read-merge-write converges concurrent field updates, and list / delete
/// behave. Convergence is asserted by writing hand-stamped records directly and
/// reading back the merged result - never by timing, ordering, or delays.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TenantRegistryConvergenceIntegrationTests
{
    private readonly TenancyClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    private static TenantRecord Acme(string writer, Action<TenantRecord> mutate)
    {
        // Stamp the base record at the lowest clock so any mutation stamped with a
        // strictly higher clock supersedes it (an equal stamp is a no-op by the
        // LWW idempotency law, which is what a same-stamp mutation would hit).
        var record = TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            Clock(1),
            writer);
        mutate(record);
        return record;
    }

    [Test]
    public async Task GetAsync_default_tenant_returns_the_seeded_unbounded_record()
    {
        var record = await _fixture.Registry.GetAsync(TenantId.Default);

        Assert.That(record, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(record!.Id, Is.EqualTo(TenantId.Default));
            Assert.That(record.IsActive, Is.True);
            Assert.That(record.Quotas.IsUnbounded, Is.True);
        });
    }

    [Test]
    public async Task PutAsync_merges_concurrent_field_updates_from_two_writers()
    {
        var left = Acme("w1", r => r.SetStatus(TenantStatus.Suspended, Clock(20), "w1"));
        var right = Acme("w2", r => r.AddAdminSubject("admin-1", Clock(30), "w2"));

        await _fixture.Registry.PutAsync(left);
        await _fixture.Registry.PutAsync(right);

        var merged = await _fixture.Registry.GetAsync(TenantId.Parse("acme"));

        Assert.That(merged, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(merged!.Status, Is.EqualTo(TenantStatus.Suspended), "the status write survives the second put");
            Assert.That(merged.HasAdminSubject("admin-1"), Is.True, "the admin write survives the first put");
        });
    }

    [Test]
    public async Task PutAsync_reapplying_an_older_write_does_not_regress()
    {
        var newer = Acme("w1", r => r.SetQuotas(TenantQuotas.Unbounded, Clock(50), "w1"));
        var older = Acme("w1", r => r.SetQuotas(new TenantQuotas { MaxKeys = 1 }, Clock(10), "w1"));

        await _fixture.Registry.PutAsync(newer);
        var afterOlder = await _fixture.Registry.PutAsync(older);

        Assert.That(afterOlder.Quotas.IsUnbounded, Is.True, "the older quota write must not regress the newer one");
    }

    [Test]
    public async Task ExistsAsync_reflects_put_and_delete()
    {
        var tenant = TenantId.Parse("beta");
        var record = TenantRecord.Create(
            tenant, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(10), "w1");

        await _fixture.Registry.PutAsync(record);
        Assert.That(await _fixture.Registry.ExistsAsync(tenant), Is.True);

        await _fixture.Registry.DeleteAsync(tenant);
        Assert.That(await _fixture.Registry.ExistsAsync(tenant), Is.False);
    }

    [Test]
    public async Task ListAsync_yields_every_registered_tenant()
    {
        var gamma = TenantId.Parse("gamma");
        await _fixture.Registry.PutAsync(TenantRecord.Create(
            gamma, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(10), "w1"));

        var ids = new List<string>();
        await foreach (var record in _fixture.Registry.ListAsync())
        {
            ids.Add(record.Id.Value);
        }

        Assert.Multiple(() =>
        {
            Assert.That(ids, Does.Contain(TenantId.Default.Value), "the seeded default tenant is listed");
            Assert.That(ids, Does.Contain(gamma.Value), "an added tenant is listed");
        });
    }

    [Test]
    public async Task PutAsync_concurrent_writers_adding_different_grants_both_survive()
    {
        var tenant = TenantId.Parse("delta");
        await _fixture.Registry.PutAsync(TenantRecord.Create(
            tenant, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "seed"));

        var grantA = CrossTenantGrant.Create("sub-a", TenantGranteeKind.Subject, "tree-a", TenantGrantOperations.Read);
        var grantB = CrossTenantGrant.Create("sub-b", TenantGranteeKind.Subject, "tree-b", TenantGrantOperations.Write);

        var writeA = Grantable(tenant, r => r.AddGrant(grantA, Clock(10), "wA"));
        var writeB = Grantable(tenant, r => r.AddGrant(grantB, Clock(20), "wB"));

        // Two overlapping read-merge-write puts against the same tenant. The
        // optimistic-concurrency loop in PutAsync forces the writer that loses the
        // version race to re-read (now seeing the other writer's committed grant)
        // and re-merge, so both grants survive in every interleaving. The outcome
        // is therefore interleaving-independent - it never depends on timing or
        // ordering.
        await Task.WhenAll(_fixture.Registry.PutAsync(writeA), _fixture.Registry.PutAsync(writeB));

        var merged = await _fixture.Registry.GetAsync(tenant);

        Assert.That(merged, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(merged!.TryGetGrant(grantA.GrantId, out _), Is.True, "writer A's grant survives the race");
            Assert.That(merged.TryGetGrant(grantB.GrantId, out _), Is.True, "writer B's grant survives the race");
        });
    }

    private static TenantRecord Grantable(TenantId tenant, Action<TenantRecord> mutate)
    {
        var record = TenantRecord.Create(
            tenant, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(1), "base");
        mutate(record);
        return record;
    }

    [Test]
    public async Task DeleteAsync_unknown_tenant_returns_false()
    {
        var removed = await _fixture.Registry.DeleteAsync(TenantId.Parse("never-registered"));

        Assert.That(removed, Is.False);
    }
}
