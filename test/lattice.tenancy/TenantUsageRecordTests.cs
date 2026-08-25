using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantUsageRecord"/>: the per-cluster-slot state CRDT.
/// Covers that a cluster writes only its own slot, the sum-fold over all slots, the
/// resident-cluster-restricted fold, and that the slot-map join is commutative,
/// associative, and idempotent (order-independent, re-merge is a no-op).
/// </summary>
[TestFixture]
public sealed class TenantUsageRecordTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Create_with_the_no_tenant_value_throws()
    {
        Assert.That(() => TenantUsageRecord.Create(default), Throws.ArgumentException);
    }

    [Test]
    public void Create_yields_an_empty_record()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(record.Id, Is.EqualTo(Acme));
            Assert.That(record.ClusterCount, Is.EqualTo(0));
            Assert.That(record.Fold(), Is.EqualTo(LocalUsageSample.Empty));
        });
    }

    [Test]
    public void SetLocalSample_writes_only_the_named_cluster_slot()
    {
        var record = TenantUsageRecord.Create(Acme);
        record.SetLocalSample("east", Sample(100, 1, 10, 1), Clock(1), "east");
        record.SetLocalSample("west", Sample(200, 2, 20, 1), Clock(1), "west");

        Assert.Multiple(() =>
        {
            Assert.That(record.ClusterCount, Is.EqualTo(2));
            Assert.That(record.LocalSample("east"), Is.EqualTo(Sample(100, 1, 10, 1)));
            Assert.That(record.LocalSample("west"), Is.EqualTo(Sample(200, 2, 20, 1)));
        });
    }

    [Test]
    public void SetLocalSample_null_cluster_throws()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.That(() => record.SetLocalSample(null!, Sample(1), Clock(1), null), Throws.ArgumentNullException);
    }

    [Test]
    public void SetLocalSample_keeps_the_superseding_stamp_per_slot()
    {
        var record = TenantUsageRecord.Create(Acme);
        record.SetLocalSample("east", Sample(100), Clock(5), "east");

        // A stale stamp does not regress the slot; a fresher one advances it.
        record.SetLocalSample("east", Sample(999), Clock(2), "east");
        Assert.That(record.LocalSample("east"), Is.EqualTo(Sample(100)), "a stale stamp does not overwrite the slot");

        record.SetLocalSample("east", Sample(300), Clock(9), "east");
        Assert.That(record.LocalSample("east"), Is.EqualTo(Sample(300)), "a fresher stamp advances the slot");
    }

    [Test]
    public void LocalSample_of_an_absent_cluster_is_empty()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.That(record.LocalSample("nowhere"), Is.EqualTo(LocalUsageSample.Empty));
    }

    [Test]
    public void LocalSample_null_cluster_throws()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.That(() => record.LocalSample(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Fold_sums_every_cluster_slot()
    {
        var record = UsageRecord(
            "acme",
            ("east", Sample(100, 1, 10, 1)),
            ("west", Sample(200, 2, 20, 1)),
            ("north", Sample(300, 3, 30, 1)));

        Assert.That(record.Fold(), Is.EqualTo(Sample(600, 6, 60, 3)));
    }

    [Test]
    public void Fold_restricted_to_resident_clusters_excludes_the_rest()
    {
        var record = UsageRecord(
            "acme",
            ("east", Sample(100, 1, 10, 1)),
            ("west", Sample(200, 2, 20, 1)),
            ("stale", Sample(999, 9, 99, 9)));

        var resident = new HashSet<string>(StringComparer.Ordinal) { "east", "west" };

        Assert.That(record.Fold(resident), Is.EqualTo(Sample(300, 3, 30, 2)), "the stale slot is excluded from the fold");
    }

    [Test]
    public void Fold_resident_null_throws()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.That(() => record.Fold(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var left = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)));
        var right = UsageRecord("acme", ("west", Sample(200, 2, 20, 1)));

        var leftFirst = TenantUsageRecord.Merge(left, right).Fold();
        var rightFirst = TenantUsageRecord.Merge(right, left).Fold();

        Assert.That(leftFirst, Is.EqualTo(rightFirst));
        Assert.That(leftFirst, Is.EqualTo(Sample(300, 3, 30, 2)));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)));
        var b = UsageRecord("acme", ("west", Sample(200, 2, 20, 1)));
        var c = UsageRecord("acme", ("north", Sample(300, 3, 30, 1)));

        var leftAssoc = TenantUsageRecord.Merge(TenantUsageRecord.Merge(a, b), c).Fold();
        var rightAssoc = TenantUsageRecord.Merge(a, TenantUsageRecord.Merge(b, c)).Fold();

        Assert.That(leftAssoc, Is.EqualTo(rightAssoc));
        Assert.That(leftAssoc, Is.EqualTo(Sample(600, 6, 60, 3)));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var record = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)), ("west", Sample(200, 2, 20, 1)));
        var foldBefore = record.Fold();

        var reMerged = TenantUsageRecord.Merge(record, record.Clone());

        Assert.That(reMerged.Fold(), Is.EqualTo(foldBefore), "re-merging a record with itself is a no-op on the fold");
        Assert.That(reMerged.ClusterCount, Is.EqualTo(2));
    }

    [Test]
    public void Merge_of_the_same_cluster_slot_keeps_the_superseding_stamp()
    {
        // Two clusters race on the same slot id; the higher stamp wins regardless
        // of merge order, so the join is deterministic.
        var earlier = TenantUsageRecord.Create(Acme);
        earlier.SetLocalSample("east", Sample(100), Clock(2), "east");
        var later = TenantUsageRecord.Create(Acme);
        later.SetLocalSample("east", Sample(500), Clock(9), "east");

        var merged = TenantUsageRecord.Merge(earlier, later);
        var mergedReverse = TenantUsageRecord.Merge(later, earlier);

        Assert.Multiple(() =>
        {
            Assert.That(merged.LocalSample("east"), Is.EqualTo(Sample(500)));
            Assert.That(mergedReverse.LocalSample("east"), Is.EqualTo(Sample(500)));
        });
    }

    [Test]
    public void MergeFrom_a_different_tenant_throws()
    {
        var acme = TenantUsageRecord.Create(Acme);
        var other = TenantUsageRecord.Create(TenantId.Parse("other"));

        Assert.That(() => acme.MergeFrom(other), Throws.ArgumentException);
    }

    [Test]
    public void MergeFrom_null_throws()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.That(() => record.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_null_arguments_throw()
    {
        var record = TenantUsageRecord.Create(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(() => TenantUsageRecord.Merge(null!, record), Throws.ArgumentNullException);
            Assert.That(() => TenantUsageRecord.Merge(record, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Clone_is_independent_of_the_original()
    {
        var record = UsageRecord("acme", ("east", Sample(100, 1, 10, 1)));
        var clone = record.Clone();

        record.SetLocalSample("west", Sample(200, 2, 20, 1), Clock(5), "west");

        Assert.Multiple(() =>
        {
            Assert.That(clone.ClusterCount, Is.EqualTo(1), "mutating the original does not affect the clone");
            Assert.That(record.ClusterCount, Is.EqualTo(2));
        });
    }
}
