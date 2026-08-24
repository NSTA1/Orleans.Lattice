using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantRecord"/>: its construction and accessors, its
/// stamped mutators, and the composite CRDT merge. Every convergence case drives
/// <see cref="TenantRecord.Merge"/> / <see cref="TenantRecord.MergeFrom"/> directly
/// with hand-built clocks, so the field-wise join is proven deterministic,
/// commutative, associative, and idempotent independent of apply order.
/// </summary>
public sealed class TenantRecordTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantRecord AcmeRecord(long ticks = 10, string? writer = "w1") =>
        TenantRecord.Create(
            Acme,
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            Clock(ticks),
            writer);

    [Test]
    public void Create_populates_id_status_quotas_and_placement()
    {
        var record = AcmeRecord();

        Assert.Multiple(() =>
        {
            Assert.That(record.Id, Is.EqualTo(Acme));
            Assert.That(record.Status, Is.EqualTo(TenantStatus.Active));
            Assert.That(record.Quotas.MaxKeys, Is.EqualTo(1000));
            Assert.That(record.Placement.IsShared, Is.True);
            Assert.That(record.IsActive, Is.True);
            Assert.That(record.IsSuspended, Is.False);
        });
    }

    [Test]
    public void Create_with_the_no_tenant_value_throws()
    {
        Assert.That(
            () => TenantRecord.Create(default, TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(10), "w1"),
            Throws.ArgumentException);
    }

    [Test]
    public void CreateDefault_is_the_active_unbounded_shared_default_tenant()
    {
        var record = TenantRecord.CreateDefault(Clock(1), "cluster");

        Assert.Multiple(() =>
        {
            Assert.That(record.Id, Is.EqualTo(TenantId.Default));
            Assert.That(record.IsActive, Is.True);
            Assert.That(record.Quotas.IsUnbounded, Is.True);
            Assert.That(record.Placement.IsShared, Is.True);
        });
    }

    [Test]
    public void SetStatus_with_a_higher_stamp_wins()
    {
        var record = AcmeRecord(ticks: 10);

        record.SetStatus(TenantStatus.Suspended, Clock(20), "w1");

        Assert.That(record.Status, Is.EqualTo(TenantStatus.Suspended));
        Assert.That(record.IsSuspended, Is.True);
    }

    [Test]
    public void SetStatus_with_a_lower_stamp_is_a_no_op()
    {
        var record = AcmeRecord(ticks: 20);

        record.SetStatus(TenantStatus.Suspended, Clock(10), "w1");

        Assert.That(record.Status, Is.EqualTo(TenantStatus.Active));
    }

    [Test]
    public void SetQuotas_with_a_higher_stamp_wins()
    {
        var record = AcmeRecord(ticks: 10);

        record.SetQuotas(TenantQuotas.Unbounded, Clock(20), "w1");

        Assert.That(record.Quotas.IsUnbounded, Is.True);
    }

    [Test]
    public void SetPlacement_with_a_higher_stamp_wins()
    {
        var record = AcmeRecord(ticks: 10);
        var placement = new TenantPlacement { WalProviderName = "wal-a", DedicatedWal = true };

        record.SetPlacement(placement, Clock(20), "w1");

        Assert.That(record.Placement, Is.EqualTo(placement));
    }

    [Test]
    public void AddAdminSubject_makes_the_subject_present()
    {
        var record = AcmeRecord();

        record.AddAdminSubject("admin-1", Clock(20), "w1");

        Assert.That(record.HasAdminSubject("admin-1"), Is.True);
        Assert.That(record.AdminSubjects, Does.Contain("admin-1"));
    }

    [Test]
    public void RemoveAdminSubject_with_a_higher_stamp_removes_the_subject()
    {
        var record = AcmeRecord();
        record.AddAdminSubject("admin-1", Clock(20), "w1");

        record.RemoveAdminSubject("admin-1", Clock(30), "w1");

        Assert.That(record.HasAdminSubject("admin-1"), Is.False);
        Assert.That(record.AdminSubjects, Does.Not.Contain("admin-1"));
    }

    [Test]
    public void RemoveAdminSubject_with_a_lower_stamp_keeps_the_subject()
    {
        var record = AcmeRecord();
        record.AddAdminSubject("admin-1", Clock(20), "w1");

        record.RemoveAdminSubject("admin-1", Clock(10), "w1");

        Assert.That(record.HasAdminSubject("admin-1"), Is.True);
    }

    [Test]
    public void AdminSubjects_are_returned_in_ordinal_order()
    {
        var record = AcmeRecord();
        record.AddAdminSubject("charlie", Clock(20), "w1");
        record.AddAdminSubject("alice", Clock(20), "w1");
        record.AddAdminSubject("bob", Clock(20), "w1");

        Assert.That(record.AdminSubjects, Is.EqualTo(new[] { "alice", "bob", "charlie" }));
    }

    [Test]
    public void AddAdminSubject_null_throws()
    {
        var record = AcmeRecord();

        Assert.That(() => record.AddAdminSubject(null!, Clock(20), "w1"), Throws.ArgumentNullException);
    }

    [Test]
    public void RemoveAdminSubject_null_throws()
    {
        var record = AcmeRecord();

        Assert.That(() => record.RemoveAdminSubject(null!, Clock(20), "w1"), Throws.ArgumentNullException);
    }

    [Test]
    public void HasAdminSubject_null_throws()
    {
        var record = AcmeRecord();

        Assert.That(() => record.HasAdminSubject(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void AddGrant_makes_the_grant_live_and_lookupable()
    {
        var record = AcmeRecord();
        var grant = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);

        record.AddGrant(grant, Clock(20), "w1");

        Assert.That(record.TryGetGrant(grant.GrantId, out var found), Is.True);
        Assert.That(found, Is.EqualTo(grant));
        Assert.That(record.Grants, Does.Contain(grant));
    }

    [Test]
    public void AddGrant_reissue_updates_the_operation_set_in_place()
    {
        var record = AcmeRecord();
        var read = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);
        var readWrite = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.ReadWrite);

        record.AddGrant(read, Clock(20), "w1");
        record.AddGrant(readWrite, Clock(30), "w1");

        Assert.That(record.Grants, Has.Count.EqualTo(1));
        Assert.That(record.TryGetGrant(read.GrantId, out var found), Is.True);
        Assert.That(found.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
    }

    [Test]
    public void RemoveGrant_by_id_with_a_higher_stamp_revokes_the_grant()
    {
        var record = AcmeRecord();
        var grant = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);
        record.AddGrant(grant, Clock(20), "w1");

        record.RemoveGrant(grant.GrantId, Clock(30), "w1");

        Assert.That(record.TryGetGrant(grant.GrantId, out _), Is.False);
        Assert.That(record.Grants, Is.Empty);
    }

    [Test]
    public void RemoveGrant_by_value_revokes_the_grant()
    {
        var record = AcmeRecord();
        var grant = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);
        record.AddGrant(grant, Clock(20), "w1");

        record.RemoveGrant(grant, Clock(30), "w1");

        Assert.That(record.TryGetGrant(grant.GrantId, out _), Is.False);
    }

    [Test]
    public void Grants_are_returned_in_grant_id_order()
    {
        var record = AcmeRecord();
        var x = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read);
        var y = CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-y", TenantGrantOperations.Read);
        record.AddGrant(y, Clock(20), "w1");
        record.AddGrant(x, Clock(20), "w1");

        var ids = record.Grants.Select(g => g.GrantId).ToList();

        Assert.That(ids, Is.Ordered.Using<string>(StringComparer.Ordinal));
    }

    [Test]
    public void AddGrant_with_a_null_grantee_throws()
    {
        var record = AcmeRecord();
        var grant = new CrossTenantGrant { Scope = "tree-x", Operations = TenantGrantOperations.Read };

        Assert.That(() => record.AddGrant(grant, Clock(20), "w1"), Throws.ArgumentException);
    }

    [Test]
    public void RemoveGrant_null_id_throws()
    {
        var record = AcmeRecord();

        Assert.That(() => record.RemoveGrant((string)null!, Clock(20), "w1"), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetGrant_null_id_throws()
    {
        var record = AcmeRecord();

        Assert.That(() => record.TryGetGrant(null!, out _), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_is_an_independent_copy()
    {
        var record = AcmeRecord();
        record.AddAdminSubject("admin-1", Clock(20), "w1");

        var clone = record.Clone();
        clone.AddAdminSubject("admin-2", Clock(30), "w1");
        clone.SetStatus(TenantStatus.Suspended, Clock(40), "w1");

        Assert.Multiple(() =>
        {
            Assert.That(record.HasAdminSubject("admin-2"), Is.False, "mutating the clone must not touch the original set");
            Assert.That(record.Status, Is.EqualTo(TenantStatus.Active), "mutating the clone must not touch the original register");
            Assert.That(clone.HasAdminSubject("admin-1"), Is.True, "the clone carries the original's state");
        });
    }

    [Test]
    public void MergeFrom_null_throws()
    {
        var record = AcmeRecord();

        Assert.That(() => record.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void MergeFrom_a_different_tenant_throws()
    {
        var acme = AcmeRecord();
        var beta = TenantRecord.Create(
            TenantId.Parse("beta"), TenantStatus.Active, TenantQuotas.Unbounded, TenantPlacement.Shared, Clock(10), "w1");

        Assert.That(() => acme.MergeFrom(beta), Throws.ArgumentException);
    }

    [Test]
    public void Merge_null_left_throws()
    {
        Assert.That(() => TenantRecord.Merge(null!, AcmeRecord()), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_null_right_throws()
    {
        Assert.That(() => TenantRecord.Merge(AcmeRecord(), null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_does_not_mutate_its_inputs()
    {
        var left = AcmeRecord(ticks: 10);
        var right = AcmeRecord(ticks: 10);
        right.SetStatus(TenantStatus.Suspended, Clock(20), "w1");

        _ = TenantRecord.Merge(left, right);

        Assert.That(left.Status, Is.EqualTo(TenantStatus.Active), "left is left unchanged");
        Assert.That(right.Status, Is.EqualTo(TenantStatus.Suspended), "right is left unchanged");
    }

    [Test]
    public void Merge_converges_status_to_the_higher_stamp()
    {
        var left = AcmeRecord(ticks: 10);
        var right = AcmeRecord(ticks: 10);
        right.SetStatus(TenantStatus.Suspended, Clock(30), "w1");

        var merged = TenantRecord.Merge(left, right);

        Assert.That(merged.Status, Is.EqualTo(TenantStatus.Suspended));
    }

    [Test]
    public void Merge_takes_the_union_of_admin_subjects()
    {
        var left = AcmeRecord();
        left.AddAdminSubject("admin-1", Clock(20), "w1");
        var right = AcmeRecord();
        right.AddAdminSubject("admin-2", Clock(20), "w2");

        var merged = TenantRecord.Merge(left, right);

        Assert.That(merged.AdminSubjects, Is.EqualTo(new[] { "admin-1", "admin-2" }));
    }

    [Test]
    public void Merge_is_commutative_over_the_composite_record()
    {
        var (left, right) = DivergentPair();

        var ab = TenantRecord.Merge(left, right);
        var ba = TenantRecord.Merge(right, left);

        AssertSameState(ab, ba);
    }

    [Test]
    public void Merge_is_associative_over_the_composite_record()
    {
        var (a, b) = DivergentPair();
        var c = AcmeRecord();
        c.SetQuotas(new TenantQuotas { MaxBytes = 999 }, Clock(50), "w3");
        c.AddGrant(CrossTenantGrant.Create("gamma", TenantGranteeKind.Tenant, "tree-z", TenantGrantOperations.Write), Clock(50), "w3");

        var left = TenantRecord.Merge(TenantRecord.Merge(a, b), c);
        var right = TenantRecord.Merge(a, TenantRecord.Merge(b, c));

        AssertSameState(left, right);
    }

    [Test]
    public void Merge_is_idempotent_over_the_composite_record()
    {
        var (left, right) = DivergentPair();
        var merged = TenantRecord.Merge(left, right);

        var remerged = TenantRecord.Merge(merged, right);

        AssertSameState(merged, remerged);
    }

    /// <summary>
    /// Two records for the same tenant that have diverged on every field family:
    /// status, quotas, placement, admin subjects, and grants.
    /// </summary>
    private static (TenantRecord Left, TenantRecord Right) DivergentPair()
    {
        var left = AcmeRecord(ticks: 10, writer: "w1");
        left.SetStatus(TenantStatus.Suspended, Clock(25), "w1");
        left.AddAdminSubject("admin-1", Clock(25), "w1");
        left.AddGrant(CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-x", TenantGrantOperations.Read), Clock(25), "w1");

        var right = AcmeRecord(ticks: 10, writer: "w2");
        right.SetQuotas(TenantQuotas.Unbounded, Clock(40), "w2");
        right.SetPlacement(new TenantPlacement { WalProviderName = "wal-a" }, Clock(40), "w2");
        right.AddAdminSubject("admin-2", Clock(40), "w2");
        right.AddGrant(CrossTenantGrant.Create("beta", TenantGranteeKind.Tenant, "tree-y", TenantGrantOperations.Write), Clock(40), "w2");

        return (left, right);
    }

    private static void AssertSameState(TenantRecord a, TenantRecord b)
    {
        Assert.Multiple(() =>
        {
            Assert.That(a.Status, Is.EqualTo(b.Status));
            Assert.That(a.Quotas, Is.EqualTo(b.Quotas));
            Assert.That(a.Placement, Is.EqualTo(b.Placement));
            Assert.That(a.AdminSubjects, Is.EqualTo(b.AdminSubjects));
            Assert.That(a.Grants.Select(g => g.GrantId), Is.EqualTo(b.Grants.Select(g => g.GrantId)));
            Assert.That(a.Grants.Select(g => g.Operations), Is.EqualTo(b.Grants.Select(g => g.Operations)));
        });
    }
}
