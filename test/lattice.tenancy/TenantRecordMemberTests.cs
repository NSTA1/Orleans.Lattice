using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the tenant member set on <see cref="TenantRecord"/> (epic #4154,
/// T1): the add-wins LWW-element-set of member subjects, its zero-allocation
/// accessors, its merge and clone, the reserved default tenant's refusal of member
/// entries, and the group-aware <see cref="TenantRecord.IsAdmin"/> and
/// <see cref="TenantRecord.IsMember"/> probes the facades use. Every stamp is a
/// hand-built clock, so each outcome is exact.
/// </summary>
[TestFixture]
public sealed class TenantRecordMemberTests
{
    private const string ClusterGroup = "entra-sales";
    private const string TenantGroup = "t/acme/editors";

    private static TenantRecord NewRecord(string tenant = "acme") =>
        TenantRecord.Create(
            TenantId.Parse(tenant),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(1),
            "test");

    [Test]
    public void New_record_has_an_empty_member_set()
    {
        var record = NewRecord();

        Assert.Multiple(() =>
        {
            Assert.That(record.MemberSubjects, Is.Empty);
            Assert.That(record.MemberSubjectCount, Is.Zero);
            Assert.That(record.HasMemberSubject("carol"), Is.False);
        });
    }

    [Test]
    public void AddMemberSubject_adds_an_entry_listed_in_ordinal_order()
    {
        var record = NewRecord();

        record.AddMemberSubject("zed", Clock(2), "test");
        record.AddMemberSubject(ClusterGroup, Clock(3), "test");
        record.AddMemberSubject("Bob", Clock(4), "test");

        Assert.Multiple(() =>
        {
            Assert.That(record.MemberSubjects, Is.EqualTo(new[] { "Bob", ClusterGroup, "zed" }));
            Assert.That(record.MemberSubjectCount, Is.EqualTo(3));
            Assert.That(record.HasMemberSubject(ClusterGroup), Is.True);
            Assert.That(record.HasAdminSubject(ClusterGroup), Is.False, "the member set is not the admin set");
        });
    }

    [Test]
    public void RemoveMemberSubject_tombstones_the_entry()
    {
        var record = NewRecord();
        record.AddMemberSubject("carol", Clock(2), "test");

        record.RemoveMemberSubject("carol", Clock(3), "test");

        Assert.Multiple(() =>
        {
            Assert.That(record.HasMemberSubject("carol"), Is.False);
            Assert.That(record.MemberSubjects, Is.Empty);
            Assert.That(record.MemberSubjectCount, Is.Zero);
        });
    }

    [Test]
    public void Member_and_admin_entries_for_one_subject_are_stamped_independently()
    {
        var record = NewRecord();
        record.AddAdminSubject("carol", Clock(5), "test");
        record.AddMemberSubject("carol", Clock(2), "test");

        record.RemoveMemberSubject("carol", Clock(3), "test");

        Assert.That(record.HasAdminSubject("carol"), Is.True, "removing the member entry leaves the admin entry alone");
    }

    [Test]
    public void AddMemberSubject_on_the_default_tenant_is_refused()
    {
        var record = TenantRecord.CreateDefault(Clock(1), "test");

        Assert.That(
            () => record.AddMemberSubject("carol", Clock(2), "test"),
            Throws.InvalidOperationException.With.Message.Contain("does not accept member entries"));
        Assert.That(record.MemberSubjectCount, Is.Zero);
    }

    [Test]
    public void Member_mutators_and_probes_throw_on_null()
    {
        var record = NewRecord();

        Assert.Multiple(() =>
        {
            Assert.That(() => record.AddMemberSubject(null!, Clock(2), "test"), Throws.ArgumentNullException);
            Assert.That(() => record.RemoveMemberSubject(null!, Clock(2), "test"), Throws.ArgumentNullException);
            Assert.That(() => record.HasMemberSubject(null!), Throws.ArgumentNullException);
            Assert.That(() => record.IsAdmin(null!, []), Throws.ArgumentNullException);
            Assert.That(() => record.IsAdmin("carol", null!), Throws.ArgumentNullException);
            Assert.That(() => record.IsMember(null!, []), Throws.ArgumentNullException);
            Assert.That(() => record.IsMember("carol", null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void MergeFrom_converges_member_sets_in_either_order()
    {
        var left = NewRecord();
        left.AddMemberSubject("carol", Clock(2), "a");
        left.AddMemberSubject("dave", Clock(5), "a");
        var right = NewRecord();
        right.RemoveMemberSubject("carol", Clock(4), "b");
        right.AddMemberSubject(ClusterGroup, Clock(3), "b");

        var leftThenRight = TenantRecord.Merge(left, right);
        var rightThenLeft = TenantRecord.Merge(right, left);

        Assert.Multiple(() =>
        {
            Assert.That(leftThenRight.MemberSubjects, Is.EqualTo(new[] { "dave", ClusterGroup }));
            Assert.That(rightThenLeft.MemberSubjects, Is.EqualTo(leftThenRight.MemberSubjects), "commutative");
            Assert.That(left.HasMemberSubject("carol"), Is.True, "Merge leaves its inputs unchanged");
        });
    }

    [Test]
    public void Clone_copies_the_member_set_independently()
    {
        var record = NewRecord();
        record.AddMemberSubject("carol", Clock(2), "test");

        var clone = record.Clone();
        clone.AddMemberSubject("dave", Clock(3), "test");

        Assert.Multiple(() =>
        {
            Assert.That(clone.MemberSubjects, Is.EqualTo(new[] { "carol", "dave" }));
            Assert.That(record.MemberSubjects, Is.EqualTo(new[] { "carol" }), "the original is unaffected");
        });
    }

    [Test]
    public void IsAdmin_recognises_an_exact_id_or_a_group_admin_entry()
    {
        var record = NewRecord();
        record.AddAdminSubject("alice", Clock(2), "test");
        record.AddAdminSubject(ClusterGroup, Clock(3), "test");

        Assert.Multiple(() =>
        {
            Assert.That(record.IsAdmin("alice", []), Is.True);
            Assert.That(record.IsAdmin("dave", [ClusterGroup]), Is.True);
            Assert.That(record.IsAdmin("dave", new HashSet<string> { "other", ClusterGroup }), Is.True);
            Assert.That(record.IsAdmin("dave", ["other"]), Is.False);
        });
    }

    [Test]
    public void IsAdmin_ignores_a_removed_group_admin_entry()
    {
        var record = NewRecord();
        record.AddAdminSubject(ClusterGroup, Clock(2), "test");
        record.RemoveAdminSubject(ClusterGroup, Clock(3), "test");

        Assert.That(record.IsAdmin("dave", [ClusterGroup]), Is.False);
    }

    [Test]
    public void IsMember_accepts_admins_exact_members_and_group_members()
    {
        var record = NewRecord();
        record.AddAdminSubject("alice", Clock(2), "test");
        record.AddMemberSubject("carol", Clock(3), "test");
        record.AddMemberSubject(TenantGroup, Clock(4), "test");

        Assert.Multiple(() =>
        {
            Assert.That(record.IsMember("alice", []), Is.True, "admins are implicitly members");
            Assert.That(record.IsMember("carol", []), Is.True);
            Assert.That(record.IsMember("erin", new List<string> { TenantGroup }), Is.True);
            Assert.That(record.IsMember("mallory", ["other"]), Is.False);
            Assert.That(record.IsAdmin("carol", []), Is.False, "a member is not an admin");
        });
    }

    [Test]
    public void IsAdmin_and_IsMember_on_the_default_tenant_are_exact_id_only()
    {
        var record = TenantRecord.CreateDefault(Clock(1), "test");
        record.AddAdminSubject("alice", Clock(2), "test");
        record.AddAdminSubject(ClusterGroup, Clock(3), "test");

        Assert.Multiple(() =>
        {
            Assert.That(record.IsAdmin("alice", []), Is.True);
            Assert.That(record.IsMember("alice", []), Is.True);
            Assert.That(record.IsAdmin("dave", [ClusterGroup]), Is.False, "the default tenant stays operator-administered");
            Assert.That(record.IsMember("dave", [ClusterGroup]), Is.False);
        });
    }
}
