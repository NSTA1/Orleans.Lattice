using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// The admin-subject removal commits through the registry's guarded commit, so a
/// racing removal that would empty the admin set is refused with nothing written
/// (no remove-then-re-grant whose second write could fail), and a removal is
/// stamped later than the slot it removes, so a silo whose clock runs behind the
/// one that wrote the slot still removes it.
/// </summary>
public sealed partial class LatticeTenantAccessAdminTests
{
    [Test]
    public void A_racing_removal_against_a_guarded_registry_is_refused_inside_the_commit_with_no_repair_write()
    {
        var registry = new GuardedScriptedRegistry(put =>
        {
            if (put == 1)
            {
                return stored => stored.RemoveAdminSubject("alice@example.com", Stamp(1_000), "other-writer");
            }

            // A repair write would fail: it must never be attempted.
            throw new TenantRegistryConcurrencyException(TenantId.Parse(Tenant), 8);
        });
        registry.Seed(SeededRecord(Tenant, "alice@example.com", "bob@example.com"));
        var admin = Admin(registry, clock: new FixedStampClock(5_000));

        Assert.That(
            async () => await admin.RemoveAdminSubjectAsync(Tenant, "bob@example.com"),
            Throws.TypeOf<TenantLastAdminSubjectException>());
        Assert.Multiple(() =>
        {
            Assert.That(registry.Peek(Tenant)!.AdminSubjects, Is.EqualTo(new[] { "bob@example.com" }), "the tenant keeps an admin subject");
            Assert.That(registry.Puts, Is.EqualTo(1), "refused inside the one commit");
        });
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_from_a_silo_whose_clock_is_behind_the_slot_still_removes_the_subject()
    {
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(Tenant, "alice@example.com");
        record.AddAdminSubject("bob@example.com", Stamp(9_000_000), "region-b");
        registry.Seed(record);
        var admin = Admin(registry, clock: new FixedStampClock(100));

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "bob@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("bob@example.com"), Is.False, "Changed=true must mean the authority is gone");
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
        });
    }

    [Test]
    public void RemoveAdminSubjectAsync_refuses_the_last_real_admin_beside_another_tenants_group_entry()
    {
        // A replicated or restored record can carry another tenant's group, which
        // never admits anyone here and so must not hold the admin set open.
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(Tenant, "alice@example.com");
        record.AddAdminSubject("t/other/admins", Stamp(60), "peer");
        registry.Seed(record);
        var admin = Admin(registry);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.RemoveAdminSubjectAsync(Tenant, "alice@example.com"),
                Throws.TypeOf<TenantLastAdminSubjectException>());
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("alice@example.com"), Is.True);
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_removes_another_tenants_group_entry_beside_the_last_real_admin()
    {
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(Tenant, "alice@example.com");
        record.AddAdminSubject("t/other/admins", Stamp(60), "peer");
        registry.Seed(record);
        var admin = Admin(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "t/other/admins");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
        });
    }

    [Test]
    public void A_racing_removal_that_would_leave_only_another_tenants_group_entry_is_refused_inside_the_commit()
    {
        var registry = new GuardedScriptedRegistry(put =>
            put == 1 ? stored => stored.RemoveAdminSubject("alice@example.com", Stamp(1_000), "other-writer") : null);
        var record = SeededRecord(Tenant, "alice@example.com", "bob@example.com");
        record.AddAdminSubject("t/other/admins", Stamp(60), "peer");
        registry.Seed(record);
        var admin = Admin(registry, clock: new FixedStampClock(5_000));

        Assert.That(
            async () => await admin.RemoveAdminSubjectAsync(Tenant, "bob@example.com"),
            Throws.TypeOf<TenantLastAdminSubjectException>());
        Assert.That(registry.Peek(Tenant)!.HasAdminSubject("bob@example.com"), Is.True, "the tenant keeps a real admin");
    }

    /// <summary>A guarded merging registry whose n-th put first applies a scripted competing write, or throws.</summary>
    private sealed class GuardedScriptedRegistry(Func<int, Action<TenantRecord>?> script) : TenantAdminTestSupport.MergingTenantRegistry
    {
        protected override void OnBeforeMerge(int putNumber)
        {
            var competing = script(putNumber);
            if (competing is not null && Peek(Tenant) is { } stored)
            {
                competing(stored);
            }
        }
    }
}
