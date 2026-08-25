namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantPlacementSnapshot"/>, the immutable per-tenant
/// placement map the resolver reads. All cases are pure in-memory - no registry,
/// no silo, no timing.
/// </summary>
[TestFixture]
public sealed class TenantPlacementSnapshotTests
{
    [Test]
    public void Empty_has_no_tenants_and_misses_every_lookup()
    {
        var snapshot = TenantPlacementSnapshot.Empty;

        var found = snapshot.TryGetPlacement(TenantId.Parse("acme"), out var placement);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count, Is.Zero);
            Assert.That(found, Is.False);
            Assert.That(placement, Is.EqualTo(default(TenantPlacement)));
        });
    }

    [Test]
    public void Build_indexes_each_tenants_placement()
    {
        var acme = TenantId.Parse("acme");
        var globex = TenantId.Parse("globex");
        var snapshot = TenantPlacementSnapshot.Build(new[]
        {
            new KeyValuePair<TenantId, TenantPlacement>(
                acme, new TenantPlacement { WalProviderName = "wal-acme", DedicatedWal = true }),
            new KeyValuePair<TenantId, TenantPlacement>(globex, TenantPlacement.Shared),
        });

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count, Is.EqualTo(2));
            Assert.That(snapshot.TryGetPlacement(acme, out var acmePlacement), Is.True);
            Assert.That(acmePlacement.WalProviderName, Is.EqualTo("wal-acme"));
            Assert.That(snapshot.TryGetPlacement(globex, out var globexPlacement), Is.True);
            Assert.That(globexPlacement.IsShared, Is.True);
        });
    }

    [Test]
    public void Build_deduplicates_a_repeated_tenant_last_writer_wins()
    {
        var acme = TenantId.Parse("acme");
        var snapshot = TenantPlacementSnapshot.Build(new[]
        {
            new KeyValuePair<TenantId, TenantPlacement>(
                acme, new TenantPlacement { WalProviderName = "wal-old", DedicatedWal = true }),
            new KeyValuePair<TenantId, TenantPlacement>(
                acme, new TenantPlacement { WalProviderName = "wal-new", DedicatedWal = true }),
        });

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count, Is.EqualTo(1));
            Assert.That(snapshot.TryGetPlacement(acme, out var placement), Is.True);
            Assert.That(placement.WalProviderName, Is.EqualTo("wal-new"));
        });
    }

    [Test]
    public void Build_null_placements_throws()
    {
        Assert.That(
            () => TenantPlacementSnapshot.Build(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetPlacement_misses_an_unknown_tenant()
    {
        var snapshot = TenantPlacementSnapshot.Build(new[]
        {
            new KeyValuePair<TenantId, TenantPlacement>(
                TenantId.Parse("acme"), TenantPlacement.Shared),
        });

        var found = snapshot.TryGetPlacement(TenantId.Parse("globex"), out _);

        Assert.That(found, Is.False);
    }
}
