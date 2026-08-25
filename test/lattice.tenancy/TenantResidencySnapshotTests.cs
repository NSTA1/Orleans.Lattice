namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantResidencySnapshot"/>, the immutable per-silo map
/// from a residency-configured tenant to its local-region status. A miss means the
/// tenant is unconfigured and resolves to online everywhere (admit-all), so the
/// hot-path <c>IsOnlineLocally</c> is <c>true</c> for a miss and for a present
/// <see cref="TenantRegionStatus.Online"/>, and <c>false</c> otherwise.
/// </summary>
[TestFixture]
public sealed class TenantResidencySnapshotTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantResidencySnapshot SnapshotWith(TenantRegionStatus status) =>
        TenantResidencySnapshot.Build(new[]
        {
            new KeyValuePair<TenantId, TenantRegionStatus>(Acme, status),
        });

    [Test]
    public void Empty_has_no_entries()
    {
        Assert.That(TenantResidencySnapshot.Empty.Count, Is.Zero);
    }

    [Test]
    public void Empty_resolves_every_tenant_to_online()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantResidencySnapshot.Empty.IsOnlineLocally(Acme), Is.True);
            Assert.That(TenantResidencySnapshot.Empty.TryGetStatus(Acme, out _), Is.False);
        });
    }

    [Test]
    public void Build_null_throws()
    {
        Assert.That(() => TenantResidencySnapshot.Build(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Build_deduplicates_with_last_write_winning()
    {
        var snapshot = TenantResidencySnapshot.Build(new[]
        {
            new KeyValuePair<TenantId, TenantRegionStatus>(Acme, TenantRegionStatus.Provisioning),
            new KeyValuePair<TenantId, TenantRegionStatus>(Acme, TenantRegionStatus.Online),
        });

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count, Is.EqualTo(1));
            Assert.That(snapshot.TryGetStatus(Acme, out var status), Is.True);
            Assert.That(status, Is.EqualTo(TenantRegionStatus.Online));
        });
    }

    [Test]
    public void TryGetStatus_returns_the_stored_status_for_a_configured_tenant()
    {
        var snapshot = SnapshotWith(TenantRegionStatus.Backfilling);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.TryGetStatus(Acme, out var status), Is.True);
            Assert.That(status, Is.EqualTo(TenantRegionStatus.Backfilling));
        });
    }

    [Test]
    public void TryGetStatus_misses_for_an_unconfigured_tenant()
    {
        var snapshot = SnapshotWith(TenantRegionStatus.Online);

        Assert.That(snapshot.TryGetStatus(TenantId.Parse("other"), out var status), Is.False);
        Assert.That(status, Is.EqualTo(TenantRegionStatus.None));
    }

    [Test]
    public void IsOnlineLocally_is_true_only_for_online_or_a_miss()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SnapshotWith(TenantRegionStatus.Online).IsOnlineLocally(Acme), Is.True);
            Assert.That(SnapshotWith(TenantRegionStatus.Provisioning).IsOnlineLocally(Acme), Is.False);
            Assert.That(SnapshotWith(TenantRegionStatus.Backfilling).IsOnlineLocally(Acme), Is.False);
            Assert.That(SnapshotWith(TenantRegionStatus.Draining).IsOnlineLocally(Acme), Is.False);
            Assert.That(SnapshotWith(TenantRegionStatus.None).IsOnlineLocally(Acme), Is.False);
            // A tenant absent from the snapshot (unconfigured) is admitted.
            Assert.That(SnapshotWith(TenantRegionStatus.Provisioning).IsOnlineLocally(TenantId.Parse("other")), Is.True);
        });
    }
}
