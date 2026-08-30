using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAccessAdmin"/>, the three-operation
/// tenant access-administration facade that manages a tenant's admin-subject set
/// (list, add, remove). Covers the projection, the idempotency of both mutations,
/// the unbypassable last-admin-subject guard, the reserved-default-tenant refusal,
/// the input guards, and the CRDT convergence of concurrent membership writes. The
/// authorization matrix lives in the sibling partial file. All doubles are
/// deterministic - no cluster, no timing, no ordering assumptions.
/// </summary>
[TestFixture]
public sealed partial class LatticeTenantAccessAdminTests
{
    private const string Tenant = "acme";

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static TenantRecord SeededRecord(string tenantId = Tenant, params string[] adminSubjects)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Stamp(1),
            "seed");

        var stamp = 2L;
        foreach (var subjectId in adminSubjects)
        {
            record.AddAdminSubject(subjectId, Stamp(stamp++), "seed");
        }

        return record;
    }

    /// <summary>
    /// Builds the facade with a uniformly allowing or denying gate. An allowing
    /// gate models a platform operator, which is the tier most of the behavioural
    /// tests below want; the authorization matrix uses the control-plane-faithful
    /// gate instead.
    /// </summary>
    private static LatticeTenantAccessAdmin Admin(
        ITenantRegistry registry, bool authorized = true, ITenantAdminClock? clock = null) =>
        new(
            registry,
            new TenantRegionResidencyAuthorizer(
                new FixedGate(allow: authorized), registry, new FixedMembershipContext(new LatticeSubject("op"))),
            clock ?? new IncrementingClock(),
            Options.Create(new ClusterOptions { ClusterId = "region-a" }));

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_registry_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantAccessAdmin(
                null!, authorizer, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_authorizer_throws() =>
        Assert.That(
            () => new LatticeTenantAccessAdmin(
                new FakeTenantRegistry(), null!, new IncrementingClock(), Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_clock_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantAccessAdmin(registry, authorizer, null!, Options.Create(new ClusterOptions())),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_cluster_options_throws()
    {
        var registry = new FakeTenantRegistry();
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), registry);

        Assert.That(
            () => new LatticeTenantAccessAdmin(registry, authorizer, new IncrementingClock(), null!),
            Throws.ArgumentNullException);
    }

    // ---- ListAdminSubjectsAsync ------------------------------------------

    [Test]
    public async Task ListAdminSubjectsAsync_projects_the_live_subjects_in_ordinal_order()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "zoe@example.com", "alice@example.com", "bob@example.com"));
        var admin = Admin(registry);

        var report = await admin.ListAdminSubjectsAsync(Tenant);

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo(Tenant));
            Assert.That(
                report.Subjects,
                Is.EqualTo(new[] { "alice@example.com", "bob@example.com", "zoe@example.com" }));
            Assert.That(registry.Puts, Is.Zero, "A read must never write.");
        });
    }

    [Test]
    public async Task ListAdminSubjectsAsync_of_a_subjectless_tenant_reports_an_empty_set()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord());
        var admin = Admin(registry);

        var report = await admin.ListAdminSubjectsAsync(Tenant);

        Assert.That(report.Subjects, Is.Empty);
    }

    [Test]
    public async Task ListAdminSubjectsAsync_omits_a_removed_subject()
    {
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(Tenant, "alice@example.com", "bob@example.com");
        record.RemoveAdminSubject("bob@example.com", Stamp(50), "seed");
        registry.Seed(record);
        var admin = Admin(registry);

        var report = await admin.ListAdminSubjectsAsync(Tenant);

        Assert.That(report.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
    }

    [Test]
    public void ListAdminSubjectsAsync_of_an_unregistered_tenant_is_not_found_for_an_operator()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.ListAdminSubjectsAsync("ghost"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("  ")]
    public void ListAdminSubjectsAsync_an_invalid_tenant_id_throws(string? tenantId) =>
        Assert.That(
            async () => await Admin(new FakeTenantRegistry()).ListAdminSubjectsAsync(tenantId!),
            Throws.InstanceOf<ArgumentException>());

    // ---- AddAdminSubjectAsync --------------------------------------------

    [Test]
    public async Task AddAdminSubjectAsync_grants_the_subject_and_persists_the_record()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        var result = await admin.AddAdminSubjectAsync(Tenant, "bob@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo(Tenant));
            Assert.That(result.SubjectId, Is.EqualTo("bob@example.com"));
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com", "bob@example.com" }));
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("bob@example.com"), Is.True);
            Assert.That(registry.Puts, Is.EqualTo(1), "The grant must land in a single write.");
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_onto_a_subjectless_tenant_grants_the_first_subject()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord());
        var admin = Admin(registry);

        var result = await admin.AddAdminSubjectAsync(Tenant, "alice@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_of_an_existing_member_is_an_idempotent_no_op()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        var result = await admin.AddAdminSubjectAsync(Tenant, "alice@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
            Assert.That(registry.Puts, Is.Zero, "An idempotent no-op must not write.");
        });
    }

    [Test]
    public async Task AddAdminSubjectAsync_re_grants_a_previously_removed_subject()
    {
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(Tenant, "alice@example.com", "bob@example.com");
        record.RemoveAdminSubject("bob@example.com", Stamp(50), "seed");
        registry.Seed(record);
        var admin = Admin(registry);

        var result = await admin.AddAdminSubjectAsync(Tenant, "bob@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True, "A tombstoned subject must be re-grantable.");
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com", "bob@example.com" }));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void AddAdminSubjectAsync_an_invalid_subject_id_throws(string? subjectId)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await Admin(registry).AddAdminSubjectAsync(Tenant, subjectId!),
                Throws.ArgumentException);
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void AddAdminSubjectAsync_an_invalid_tenant_id_throws() =>
        Assert.That(
            async () => await Admin(new FakeTenantRegistry()).AddAdminSubjectAsync("", "alice@example.com"),
            Throws.ArgumentException);

    [Test]
    public void AddAdminSubjectAsync_on_the_reserved_default_tenant_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(TenantId.DefaultId, "alice@example.com"));
        var admin = Admin(registry);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.AddAdminSubjectAsync(TenantId.DefaultId, "bob@example.com"),
                Throws.TypeOf<ReservedTenantOperationException>());
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void AddAdminSubjectAsync_of_an_unregistered_tenant_is_not_found_for_an_operator()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.AddAdminSubjectAsync("ghost", "alice@example.com"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    // ---- RemoveAdminSubjectAsync -----------------------------------------

    [Test]
    public async Task RemoveAdminSubjectAsync_revokes_the_subject_and_persists_the_record()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com", "bob@example.com"));
        var admin = Admin(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "bob@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.SubjectId, Is.EqualTo("bob@example.com"));
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("bob@example.com"), Is.False);
            Assert.That(registry.Puts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_of_a_non_member_is_an_idempotent_no_op()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "stranger@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.False);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com" }));
            Assert.That(registry.Puts, Is.Zero, "An idempotent no-op must not write.");
        });
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_of_a_non_member_on_a_single_subject_tenant_is_a_no_op_not_a_refusal()
    {
        // The idempotency check must precede the last-subject guard, so removing an
        // id that was never a member is never mistaken for emptying the set.
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "stranger@example.com");

        Assert.That(result.Changed, Is.False);
    }

    [Test]
    public void RemoveAdminSubjectAsync_of_the_last_admin_subject_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.RemoveAdminSubjectAsync(Tenant, "alice@example.com"),
                Throws.TypeOf<TenantLastAdminSubjectException>());
            Assert.That(registry.Peek(Tenant)!.HasAdminSubject("alice@example.com"), Is.True,
                "The refused removal must leave the tenant's membership untouched.");
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void RemoveAdminSubjectAsync_last_subject_refusal_names_the_tenant_and_subject()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));
        var admin = Admin(registry);

        var ex = Assert.ThrowsAsync<TenantLastAdminSubjectException>(
            async () => await admin.RemoveAdminSubjectAsync(Tenant, "alice@example.com"));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo(Tenant));
            Assert.That(ex.SubjectId, Is.EqualTo("alice@example.com"));
        });
    }

    [Test]
    public async Task RemoveAdminSubjectAsync_down_to_one_subject_is_allowed()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com", "bob@example.com"));
        var admin = Admin(registry);

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "alice@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "bob@example.com" }));
        });
    }

    [Test]
    public void RemoveAdminSubjectAsync_does_not_count_a_tombstoned_subject_towards_the_guard()
    {
        // Two slots exist but only one is live, so removing the live one is the
        // last-subject case and must be refused.
        var registry = new FakeTenantRegistry();
        var record = SeededRecord(Tenant, "alice@example.com", "bob@example.com");
        record.RemoveAdminSubject("bob@example.com", Stamp(50), "seed");
        registry.Seed(record);
        var admin = Admin(registry);

        Assert.That(
            async () => await admin.RemoveAdminSubjectAsync(Tenant, "alice@example.com"),
            Throws.TypeOf<TenantLastAdminSubjectException>());
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void RemoveAdminSubjectAsync_an_invalid_subject_id_throws(string? subjectId)
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(Tenant, "alice@example.com"));

        Assert.That(
            async () => await Admin(registry).RemoveAdminSubjectAsync(Tenant, subjectId!),
            Throws.ArgumentException);
    }

    [Test]
    public void RemoveAdminSubjectAsync_an_invalid_tenant_id_throws() =>
        Assert.That(
            async () => await Admin(new FakeTenantRegistry()).RemoveAdminSubjectAsync("not a tenant id!", "alice@example.com"),
            Throws.ArgumentException);

    [Test]
    public void RemoveAdminSubjectAsync_on_the_reserved_default_tenant_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(TenantId.DefaultId, "alice@example.com", "bob@example.com"));
        var admin = Admin(registry);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.RemoveAdminSubjectAsync(TenantId.DefaultId, "bob@example.com"),
                Throws.TypeOf<ReservedTenantOperationException>());
            Assert.That(registry.Puts, Is.Zero);
        });
    }

    [Test]
    public void RemoveAdminSubjectAsync_of_an_unregistered_tenant_is_not_found_for_an_operator()
    {
        var admin = Admin(new FakeTenantRegistry());

        Assert.That(
            async () => await admin.RemoveAdminSubjectAsync("ghost", "alice@example.com"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    // ---- CRDT convergence ------------------------------------------------

    [Test]
    public async Task Concurrent_add_and_remove_on_two_replicas_converge_independently_of_merge_order()
    {
        // Two replicas each hold their own copy of the same seeded record and take
        // one membership write apiece under distinct, non-overlapping stamp ranges.
        // The merge must be commutative: joining left-into-right must equal
        // right-into-left. No wall clock and no ordering assumption is involved -
        // the stamps are supplied explicitly.
        var seed = SeededRecord(Tenant, "alice@example.com", "bob@example.com");

        var replicaA = new FakeTenantRegistry();
        replicaA.Seed(seed.Clone());
        var adminA = Admin(replicaA, clock: new FixedStampClock(100));

        var replicaB = new FakeTenantRegistry();
        replicaB.Seed(seed.Clone());
        var adminB = Admin(replicaB, clock: new FixedStampClock(200));

        await adminA.AddAdminSubjectAsync(Tenant, "carol@example.com");
        await adminB.RemoveAdminSubjectAsync(Tenant, "bob@example.com");

        var left = replicaA.Peek(Tenant)!;
        var right = replicaB.Peek(Tenant)!;

        var leftIntoRight = TenantRecord.Merge(left, right);
        var rightIntoLeft = TenantRecord.Merge(right, left);

        Assert.Multiple(() =>
        {
            Assert.That(
                leftIntoRight.AdminSubjects,
                Is.EqualTo(new[] { "alice@example.com", "carol@example.com" }),
                "Both writes must survive the merge: the add lands and the remove tombstones.");
            Assert.That(
                rightIntoLeft.AdminSubjects,
                Is.EqualTo(leftIntoRight.AdminSubjects),
                "The merge must be commutative.");
        });
    }

    [Test]
    public async Task Concurrent_writes_to_the_same_subject_converge_on_the_later_stamp()
    {
        // Both replicas act on the same subject under disjoint, explicit stamp
        // ranges: one revokes it, the other revokes and re-grants it at a strictly
        // later stamp. The later write must win in both merge directions.
        var seed = SeededRecord(Tenant, "alice@example.com", "bob@example.com");

        var earlier = new FakeTenantRegistry();
        earlier.Seed(seed.Clone());
        await Admin(earlier, clock: new FixedStampClock(100)).RemoveAdminSubjectAsync(Tenant, "bob@example.com");

        var later = new FakeTenantRegistry();
        later.Seed(seed.Clone());
        var laterAdmin = Admin(later, clock: new FixedStampClock(500));
        await laterAdmin.RemoveAdminSubjectAsync(Tenant, "bob@example.com");
        await laterAdmin.AddAdminSubjectAsync(Tenant, "bob@example.com");

        var left = earlier.Peek(Tenant)!;
        var right = later.Peek(Tenant)!;

        var leftIntoRight = TenantRecord.Merge(left, right);
        var rightIntoLeft = TenantRecord.Merge(right, left);

        Assert.Multiple(() =>
        {
            Assert.That(
                leftIntoRight.AdminSubjects,
                Is.EqualTo(new[] { "alice@example.com", "bob@example.com" }),
                "The later re-grant must supersede the earlier revoke.");
            Assert.That(
                rightIntoLeft.AdminSubjects,
                Is.EqualTo(leftIntoRight.AdminSubjects),
                "The merge must be commutative.");
        });
    }

    /// <summary>
    /// A clock that hands out strictly increasing stamps from a fixed base, so two
    /// simulated replicas can be given disjoint, explicit stamp ranges without any
    /// dependence on the wall clock or on test execution order.
    /// </summary>
    private sealed class FixedStampClock(long baseTicks) : ITenantAdminClock
    {
        private long _next = baseTicks;

        public HybridLogicalClock Next() => new() { WallClockTicks = _next++ };
    }

    // ---- the concurrent-removal orphan guard -----------------------------

    [Test]
    public void Two_concurrent_removals_of_different_subjects_cannot_orphan_the_tenant()
    {
        // The in-facade last-subject guard is a read-check-write, so on its own it
        // is defeated by two concurrent removals of *different* subjects: each sees
        // two live subjects, each passes the check, and the tombstones land on
        // disjoint keys so both survive the per-subject CRDT merge, emptying the
        // set. This registry double reproduces exactly that by committing the
        // competing removal between this caller's read and its write, with explicit
        // stamps - no threads, no timing, no ordering assumption.
        var registry = new RacingRemovalRegistry(
            SeededRecord(Tenant, "alice@example.com", "bob@example.com"),
            concurrentlyRemoved: "alice@example.com",
            concurrentStamp: 1_000);
        var admin = Admin(registry, clock: new FixedStampClock(5_000));

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await admin.RemoveAdminSubjectAsync(Tenant, "bob@example.com"),
                Throws.TypeOf<TenantLastAdminSubjectException>(),
                "The removal must be refused once the merge reveals it would empty the set.");
            Assert.That(
                registry.Stored.AdminSubjects,
                Is.EqualTo(new[] { "bob@example.com" }),
                "The refused removal must be repaired, leaving the tenant with an admin subject.");
            Assert.That(registry.Stored.AdminSubjectCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_remove_reports_the_converged_set_not_the_callers_local_view()
    {
        // A concurrent grant from another replica must be visible in the response,
        // which is only true when the result is built from the registry's merged
        // record rather than this caller's pre-merge copy.
        var registry = new RacingGrantRegistry(
            SeededRecord(Tenant, "alice@example.com", "bob@example.com"),
            concurrentlyGranted: "carol@example.com",
            concurrentStamp: 1_000);
        var admin = Admin(registry, clock: new FixedStampClock(5_000));

        var result = await admin.RemoveAdminSubjectAsync(Tenant, "bob@example.com");

        Assert.That(
            result.Subjects,
            Is.EqualTo(new[] { "alice@example.com", "carol@example.com" }),
            "The concurrently granted subject must not be dropped from the reported set.");
    }

    [Test]
    public async Task An_add_reports_the_converged_set_not_the_callers_local_view()
    {
        var registry = new RacingGrantRegistry(
            SeededRecord(Tenant, "alice@example.com"),
            concurrentlyGranted: "carol@example.com",
            concurrentStamp: 1_000);
        var admin = Admin(registry, clock: new FixedStampClock(5_000));

        var result = await admin.AddAdminSubjectAsync(Tenant, "bob@example.com");

        Assert.That(
            result.Subjects,
            Is.EqualTo(new[] { "alice@example.com", "bob@example.com", "carol@example.com" }));
    }

    /// <summary>
    /// The shared read-merge-write shape of the real registry: a read hands out a
    /// clone (so a caller mutates its own copy), and a put folds the caller's
    /// record into the stored one with the CRDT join and returns the committed
    /// result. A subclass injects a competing write, applied once, immediately
    /// before the first merge - the exact window the real registry's optimistic
    /// read-merge-write leaves open.
    /// </summary>
    private abstract class MergingTenantRegistry : ITenantRegistry
    {
        protected MergingTenantRegistry(TenantRecord seed) => Stored = seed;

        public TenantRecord Stored { get; private set; }

        public int Puts { get; private set; }

        protected abstract void ApplyCompetingWrite(TenantRecord stored);

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult<TenantRecord?>(
                string.Equals(tenant.Value, Stored.Id.Value, StringComparison.Ordinal) ? Stored.Clone() : null);

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(string.Equals(tenant.Value, Stored.Id.Value, StringComparison.Ordinal));

        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return Stored;
            await Task.CompletedTask.ConfigureAwait(false);
        }

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(record);
            if (Puts++ == 0)
            {
                ApplyCompetingWrite(Stored);
            }

            Stored = Stored.MergeFrom(record);
            return Task.FromResult(Stored);
        }

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default)
            => Task.FromResult(false);
    }

    /// <summary>A registry whose competing writer revokes a different admin subject.</summary>
    private sealed class RacingRemovalRegistry(
        TenantRecord seed, string concurrentlyRemoved, long concurrentStamp)
        : MergingTenantRegistry(seed)
    {
        protected override void ApplyCompetingWrite(TenantRecord stored) =>
            stored.RemoveAdminSubject(
                concurrentlyRemoved, new HybridLogicalClock { WallClockTicks = concurrentStamp }, "other-writer");
    }

    /// <summary>A registry whose competing writer grants an additional admin subject.</summary>
    private sealed class RacingGrantRegistry(
        TenantRecord seed, string concurrentlyGranted, long concurrentStamp)
        : MergingTenantRegistry(seed)
    {
        protected override void ApplyCompetingWrite(TenantRecord stored) =>
            stored.AddAdminSubject(
                concurrentlyGranted, new HybridLogicalClock { WallClockTicks = concurrentStamp }, "other-writer");
    }
}
