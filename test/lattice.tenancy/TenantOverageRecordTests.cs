using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantOverageRecord"/>: the per-cluster grow-only
/// counter meter. Covers that a cluster advances only its own component, the
/// sum-fold across clusters, the resident-restricted fold, grow-only monotonicity,
/// and that the counter join is commutative, associative, and idempotent (proven by
/// final converged state, never by timing or order).
/// </summary>
[TestFixture]
public sealed class TenantOverageRecordTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void Create_with_the_no_tenant_value_throws()
    {
        Assert.That(() => TenantOverageRecord.Create(default), Throws.ArgumentException);
    }

    [Test]
    public void Create_yields_an_empty_record()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(record.Id, Is.EqualTo(Acme));
            Assert.That(record.ClusterCount, Is.EqualTo(0));
            Assert.That(record.Fold(), Is.EqualTo(TenantOverageSample.Empty));
        });
    }

    [Test]
    public void MeterLocal_advances_only_the_named_cluster_component()
    {
        var record = TenantOverageRecord.Create(Acme);
        record.MeterLocal("east", Overage(100, 1, 10, 1));
        record.MeterLocal("west", Overage(200, 2, 20, 2));

        Assert.Multiple(() =>
        {
            Assert.That(record.ClusterCount, Is.EqualTo(2));
            Assert.That(record.LocalOverage("east"), Is.EqualTo(Overage(100, 1, 10, 1)));
            Assert.That(record.LocalOverage("west"), Is.EqualTo(Overage(200, 2, 20, 2)));
        });
    }

    [Test]
    public void MeterLocal_is_grow_only_and_accumulates_per_component()
    {
        var record = TenantOverageRecord.Create(Acme);
        record.MeterLocal("east", Overage(100, 1, 10, 1));
        record.MeterLocal("east", Overage(50, 2, 5, 3));

        Assert.That(record.LocalOverage("east"), Is.EqualTo(Overage(150, 3, 15, 4)), "successive meters sum into the grow-only component");
    }

    [Test]
    public void MeterLocal_an_empty_increment_records_no_component()
    {
        var record = TenantOverageRecord.Create(Acme);
        record.MeterLocal("east", TenantOverageSample.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(record.ClusterCount, Is.EqualTo(0), "a zero increment records no replica");
            Assert.That(record.Fold(), Is.EqualTo(TenantOverageSample.Empty));
        });
    }

    [Test]
    public void MeterLocal_null_or_empty_cluster_throws()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(() => record.MeterLocal(null!, Overage(1)), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => record.MeterLocal(string.Empty, Overage(1)), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void LocalOverage_of_an_absent_cluster_is_empty()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.That(record.LocalOverage("nowhere"), Is.EqualTo(TenantOverageSample.Empty));
    }

    [Test]
    public void LocalOverage_null_or_empty_cluster_throws()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(() => record.LocalOverage(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => record.LocalOverage(string.Empty), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Fold_sums_every_cluster_component()
    {
        var record = OverageRecord(
            "acme",
            ("east", Overage(100, 1, 10, 1)),
            ("west", Overage(200, 2, 20, 2)),
            ("north", Overage(300, 3, 30, 3)));

        Assert.That(record.Fold(), Is.EqualTo(Overage(600, 6, 60, 6)));
    }

    [Test]
    public void Fold_restricted_to_resident_clusters_excludes_the_rest()
    {
        var record = OverageRecord(
            "acme",
            ("east", Overage(100, 1, 10, 1)),
            ("west", Overage(200, 2, 20, 2)),
            ("stale", Overage(999, 9, 99, 9)));

        var resident = new HashSet<string>(StringComparer.Ordinal) { "east", "west" };

        Assert.That(record.Fold(resident), Is.EqualTo(Overage(300, 3, 30, 3)), "the stale component is excluded from the fold");
    }

    [Test]
    public void Fold_resident_null_throws()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.That(() => record.Fold(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ClusterCount_is_the_union_of_components_across_dimensions()
    {
        var record = TenantOverageRecord.Create(Acme);
        record.MeterLocal("east", Overage(bytes: 100));   // only the bytes dimension
        record.MeterLocal("west", Overage(keys: 5));      // only the keys dimension

        Assert.That(record.ClusterCount, Is.EqualTo(2), "a cluster over on any single dimension is counted");
    }

    [Test]
    public void Merge_is_commutative()
    {
        var left = OverageRecord("acme", ("east", Overage(100, 1, 10, 1)));
        var right = OverageRecord("acme", ("west", Overage(200, 2, 20, 2)));

        var leftFirst = TenantOverageRecord.Merge(left, right).Fold();
        var rightFirst = TenantOverageRecord.Merge(right, left).Fold();

        Assert.Multiple(() =>
        {
            Assert.That(leftFirst, Is.EqualTo(rightFirst));
            Assert.That(leftFirst, Is.EqualTo(Overage(300, 3, 30, 3)));
        });
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = OverageRecord("acme", ("east", Overage(100, 1, 10, 1)));
        var b = OverageRecord("acme", ("west", Overage(200, 2, 20, 2)));
        var c = OverageRecord("acme", ("north", Overage(300, 3, 30, 3)));

        var leftAssoc = TenantOverageRecord.Merge(TenantOverageRecord.Merge(a, b), c).Fold();
        var rightAssoc = TenantOverageRecord.Merge(a, TenantOverageRecord.Merge(b, c)).Fold();

        Assert.Multiple(() =>
        {
            Assert.That(leftAssoc, Is.EqualTo(rightAssoc));
            Assert.That(leftAssoc, Is.EqualTo(Overage(600, 6, 60, 6)));
        });
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var record = OverageRecord("acme", ("east", Overage(100, 1, 10, 1)), ("west", Overage(200, 2, 20, 2)));
        var foldBefore = record.Fold();

        var reMerged = TenantOverageRecord.Merge(record, record.Clone());

        Assert.Multiple(() =>
        {
            Assert.That(reMerged.Fold(), Is.EqualTo(foldBefore), "re-merging a grow-only counter with itself does not double-count");
            Assert.That(reMerged.ClusterCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void Merge_of_the_same_component_keeps_the_pointwise_max()
    {
        // Two deliveries of the same cluster's component race; grow-only merge keeps
        // the pointwise-max per replica regardless of order, so a stale (smaller)
        // delivery never regresses a fresher (larger) one.
        var smaller = TenantOverageRecord.Create(Acme);
        smaller.MeterLocal("east", Overage(100, 1, 10, 1));
        var larger = TenantOverageRecord.Create(Acme);
        larger.MeterLocal("east", Overage(500, 5, 50, 5));

        var merged = TenantOverageRecord.Merge(smaller, larger);
        var mergedReverse = TenantOverageRecord.Merge(larger, smaller);

        Assert.Multiple(() =>
        {
            Assert.That(merged.LocalOverage("east"), Is.EqualTo(Overage(500, 5, 50, 5)));
            Assert.That(mergedReverse.LocalOverage("east"), Is.EqualTo(Overage(500, 5, 50, 5)));
        });
    }

    [Test]
    public void MergeFrom_a_different_tenant_throws()
    {
        var acme = TenantOverageRecord.Create(Acme);
        var other = TenantOverageRecord.Create(TenantId.Parse("other"));

        Assert.That(() => acme.MergeFrom(other), Throws.ArgumentException);
    }

    [Test]
    public void MergeFrom_null_throws()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.That(() => record.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_null_arguments_throw()
    {
        var record = TenantOverageRecord.Create(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(() => TenantOverageRecord.Merge(null!, record), Throws.ArgumentNullException);
            Assert.That(() => TenantOverageRecord.Merge(record, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Clone_is_independent_of_the_original()
    {
        var record = OverageRecord("acme", ("east", Overage(100, 1, 10, 1)));
        var clone = record.Clone();

        record.MeterLocal("east", Overage(50, 0, 0, 0));
        record.MeterLocal("west", Overage(200, 2, 20, 2));

        Assert.Multiple(() =>
        {
            Assert.That(clone.ClusterCount, Is.EqualTo(1), "mutating the original does not affect the clone");
            Assert.That(clone.LocalOverage("east"), Is.EqualTo(Overage(100, 1, 10, 1)));
            Assert.That(record.ClusterCount, Is.EqualTo(2));
        });
    }
}
