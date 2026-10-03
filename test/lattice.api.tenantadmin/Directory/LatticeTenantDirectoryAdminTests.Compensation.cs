using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// The second write of every verify-and-compensate step can itself fail. A cap
/// withdrawal is idempotent, so it is retried a bounded number of times; when it
/// still fails the caller gets a typed quota refusal saying the cap may stay
/// exceeded, and a warning carrying the tenant and dimension only is logged. The
/// last-admin guard is applied inside the registry's commit, so a racing removal
/// is refused with nothing written instead of being written and then repaired by
/// a second write that could fail. Every removal is stamped later than the slot
/// it removes, so a silo whose clock runs behind still removes it.
/// </summary>
public sealed partial class LatticeTenantDirectoryAdminTests
{
    // ---- cap withdrawal retried --------------------------------------------

    [Test]
    public void A_member_set_withdrawal_that_fails_transiently_is_retried_and_the_cap_holds()
    {
        var registry = new ScriptedRegistry
        {
            OnPut = (put, self) =>
            {
                if (put == 1)
                {
                    // A racer's add commits inside this call's read-to-write window.
                    self.Peek(Tenant)!.AddMemberSubject("racer", Stamp(70), "racer");
                }
                else if (put is 2 or 3)
                {
                    throw new TenantRegistryConcurrencyException(TenantId.Parse(Tenant), 8);
                }
            },
        };
        var harness = new Harness(new LatticeSubject(Alice), true, null, false, registry: registry);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMemberSubjects = 1 }, Stamp(60), "seed");

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(async () => await harness.Admin.AddMemberAsync(Tenant, "user-x"));

        var committed = harness.Committed(Tenant);
        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(TenantAccessCaps.MemberSubjectsDimension));
            Assert.That(ex.Current, Is.EqualTo(ex.Limit), "the ordinary refusal: the withdrawal landed");
            Assert.That(committed.HasMemberSubject("user-x"), Is.False, "this call's entry was withdrawn on the third attempt");
            Assert.That(committed.HasMemberSubject("racer"), Is.True, "only this call's own entry is withdrawn");
            Assert.That(registry.Puts, Is.EqualTo(4), "one add, two failed withdrawals, one that landed");
        });
    }

    [Test]
    public void A_member_set_withdrawal_that_never_lands_reports_the_overshoot_and_logs_a_warning_without_subject_ids()
    {
        var registry = new ScriptedRegistry
        {
            OnPut = (put, self) =>
            {
                if (put == 1)
                {
                    self.Peek(Tenant)!.AddMemberSubject("racer", Stamp(70), "racer");
                }
                else
                {
                    throw new TenantRegistryConcurrencyException(TenantId.Parse(Tenant), 8);
                }
            },
        };
        var logger = new CapturingLogger<LatticeTenantDirectoryAdmin>();
        var harness = new Harness(new LatticeSubject(Alice), true, null, false, registry: registry, logger: logger);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMemberSubjects = 1 }, Stamp(60), "seed");

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(async () => await harness.Admin.AddMemberAsync(Tenant, "user-x"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("may stay exceeded"));
            Assert.That(ex.Current, Is.EqualTo(2), "the over-cap count observed");
            Assert.That(ex.Limit, Is.EqualTo(1));
            Assert.That(ex.TenantId, Is.EqualTo(Tenant));
            Assert.That(registry.Puts, Is.EqualTo(1 + TenantCapCompensation.MaxAttempts), "bounded");
            Assert.That(logger.Entries, Has.Count.EqualTo(1));
            Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(logger.Entries[0].Message, Does.Contain(Tenant).And.Contain(TenantAccessCaps.MemberSubjectsDimension));
            Assert.That(logger.Entries[0].Message, Does.Not.Contain("user-x").And.Not.Contain("racer"), "no subject ids in logs");
        });
    }

    [Test]
    public void A_group_withdrawal_that_fails_transiently_is_retried_and_the_cap_holds()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");
        harness.Store.AfterNextUpsertGroup = store => store.SeedGroup("t/acme/racer");
        harness.Store.RemoveGroupCascadeFailures = 2;

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "mine" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Current, Is.EqualTo(ex.Limit), "the ordinary refusal: the withdrawal landed");
            Assert.That(harness.Store.HasGroup("t/acme/mine"), Is.False);
            Assert.That(harness.Store.HasGroup("t/acme/racer"), Is.True);
            Assert.That(harness.Store.RemovalAttempts, Is.EqualTo(3));
        });
    }

    [Test]
    public void A_group_withdrawal_that_never_lands_reports_the_overshoot()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");
        harness.Store.AfterNextUpsertGroup = store => store.SeedGroup("t/acme/racer");
        harness.Store.RemoveGroupCascadeFailures = int.MaxValue;

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "mine" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("may stay exceeded"));
            Assert.That(ex.Dimension, Is.EqualTo(TenantAccessCaps.GroupsDimension));
            Assert.That(ex.Current, Is.EqualTo(2));
            Assert.That(harness.Store.RemovalAttempts, Is.EqualTo(TenantCapCompensation.MaxAttempts));
        });
    }

    [Test]
    public void An_edge_withdrawal_that_fails_transiently_is_retried_and_the_cap_holds()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMembershipEdges = 1 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.AfterNextAddMember = store => store.SeedEdge("t/acme/eng", "racer");
        harness.Store.RemoveMemberFailures = 3;

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "user-x"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Current, Is.EqualTo(ex.Limit), "the ordinary refusal: the withdrawal landed");
            Assert.That(harness.Store.Edges.Select(e => e.MemberId), Is.EqualTo(new[] { "racer" }));
            Assert.That(harness.Store.RemovalAttempts, Is.EqualTo(4));
        });
    }

    [Test]
    public void An_edge_withdrawal_that_never_lands_reports_the_overshoot()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMembershipEdges = 1 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.AfterNextAddMember = store => store.SeedEdge("t/acme/eng", "racer");
        harness.Store.RemoveMemberFailures = int.MaxValue;

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await harness.Admin.AddGroupMemberAsync(Tenant, "eng", "user-x"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("may stay exceeded"));
            Assert.That(ex.Dimension, Is.EqualTo(TenantAccessCaps.MembershipEdgesDimension));
            Assert.That(harness.Store.RemovalAttempts, Is.EqualTo(TenantCapCompensation.MaxAttempts));
        });
    }

    // ---- last admin: guarded inside the commit --------------------------------

    [Test]
    public void A_racing_removal_of_the_other_admin_entry_is_refused_inside_the_commit_and_writes_nothing()
    {
        // The tenant has two admin entries: alice and the admins group. A racer
        // removes alice inside this call's read-to-write window. A failing second
        // write must not matter, because no second write is ever made.
        var registry = new ScriptedRegistry
        {
            OnPut = (put, self) =>
            {
                if (put == 1)
                {
                    self.Peek(Tenant)!.RemoveAdminSubject(Alice, Stamp(90), "racer");
                }
                else
                {
                    throw new TenantRegistryConcurrencyException(TenantId.Parse(Tenant), 8);
                }
            },
        };
        var harness = new Harness(new LatticeSubject(Operator), true, null, false, registry: registry);
        harness.Store.SeedGroup("t/acme/admins");
        harness.Committed(Tenant).AddAdminSubject("t/acme/admins", Stamp(50), "seed");

        var ex = Assert.ThrowsAsync<TenantLastAdminSubjectException>(async () => await harness.Admin.RemoveGroupAsync(Tenant, "admins"));

        var committed = harness.Committed(Tenant);
        Assert.Multiple(() =>
        {
            Assert.That(ex!.SubjectId, Is.EqualTo("t/acme/admins"));
            Assert.That(committed.AdminSubjects, Is.EqualTo(new[] { "t/acme/admins" }), "the tenant keeps an admin entry");
            Assert.That(registry.Puts, Is.EqualTo(1), "refused inside the one commit; no repair write");
            Assert.That(harness.Store.HasGroup("t/acme/admins"), Is.True, "nothing was cascaded");
        });
    }

    // ---- removal stamps supersede the slot -----------------------------------

    [Test]
    public async Task RemoveMemberAsync_from_a_silo_whose_clock_is_behind_the_slot_still_removes_the_entry()
    {
        // The entry was written by a silo whose clock is far ahead of this one.
        var harness = new Harness(new LatticeSubject(Alice), true, null, false, clock: new BehindClock());
        harness.Committed(Tenant).AddMemberSubject("bob", Stamp(1_000_000), "region-b");

        var result = await harness.Admin.RemoveMemberAsync(Tenant, "bob");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(harness.Committed(Tenant).HasMemberSubject("bob"), Is.False, "Changed=true must mean the entry is gone");
        });
    }

    [Test]
    public async Task RemoveGroupAsync_from_a_silo_whose_clock_is_behind_the_slots_still_removes_the_entries()
    {
        var harness = new Harness(new LatticeSubject(Operator), true, null, false, clock: new BehindClock());
        harness.Store.SeedGroup("t/acme/eng");
        var record = harness.Committed(Tenant);
        record.AddMemberSubject("t/acme/eng", Stamp(1_000_000), "region-b");
        record.AddAdminSubject("t/acme/eng", Stamp(1_000_000), "region-b");

        var result = await harness.Admin.RemoveGroupAsync(Tenant, "eng");

        var committed = harness.Committed(Tenant);
        Assert.Multiple(() =>
        {
            Assert.That(result.RemovedFromMemberSet && result.RemovedFromAdminSet, Is.True);
            Assert.That(committed.HasMemberSubject("t/acme/eng"), Is.False);
            Assert.That(committed.HasAdminSubject("t/acme/eng"), Is.False);
        });
    }

    /// <summary>A clock far behind the stamps the tests seed, as on a silo whose wall clock is slow.</summary>
    private sealed class BehindClock : ITenantAdminClock
    {
        private long _next = 100;

        public HybridLogicalClock Next() => new() { WallClockTicks = Interlocked.Increment(ref _next) };
    }

    /// <summary>A merging registry whose every put (plain or guarded) first runs a scripted step.</summary>
    private sealed class ScriptedRegistry : MergingTenantRegistry
    {
        public Action<int, ScriptedRegistry>? OnPut { get; init; }

        protected override void OnBeforeMerge(int putNumber) => OnPut?.Invoke(putNumber, this);
    }

    /// <summary>Captures every log entry with its level and formatted message.</summary>
    internal sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
