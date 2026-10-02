using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Directory.DirectoryTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Directory;

/// <summary>
/// Concurrent additions at one below a cap. A count-based barrier holds every
/// racer's write until all of them have passed the pre-write check, which is the
/// interleaving that lets a plain check-then-write exceed the cap. The post-write
/// verification must withdraw every addition that took the tenant over its cap, so
/// the committed count never exceeds it.
/// </summary>
public sealed partial class LatticeTenantDirectoryAdminTests
{
    private const int Racers = 3;

    private static async Task<Exception?> OutcomeAsync(Func<Task> call)
    {
        try
        {
            await call();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static void AssertCapHeld(Exception?[] outcomes, int committed, int baseline, long cap, string dimension)
    {
        var succeeded = outcomes.Count(o => o is null);
        Assert.Multiple(() =>
        {
            Assert.That(committed, Is.LessThanOrEqualTo(cap), "the committed count never exceeds the cap");
            Assert.That(committed, Is.EqualTo(baseline + succeeded), "every refused racer withdrew exactly its own addition");
            Assert.That(succeeded, Is.LessThan(Racers), "at least one racer is refused");
            Assert.That(
                outcomes.Where(o => o is not null),
                Is.All.TypeOf<LatticeQuotaExceededException>().And.Property(nameof(LatticeQuotaExceededException.Dimension)).EqualTo(dimension));
        });
    }

    [Test]
    public async Task Concurrent_group_creation_at_the_cap_never_exceeds_MaxGroups()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 3 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/g1");
        harness.Store.SeedGroup("t/acme/g2");
        harness.Store.WriteBarrier = new AsyncBarrier(Racers);

        var outcomes = await Task.WhenAll(Enumerable.Range(0, Racers).Select(i =>
            OutcomeAsync(() => harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = $"race-{i}" })))
            .ToArray());

        var committed = await harness.Store.CountTenantGroupsAsync(TenantId.Parse(Tenant), CancellationToken.None);
        AssertCapHeld(outcomes, committed, baseline: 2, cap: 3, TenantAccessCaps.GroupsDimension);
    }

    [Test]
    public async Task Concurrent_edge_additions_at_the_cap_never_exceed_MaxMembershipEdges()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxMembershipEdges = 2 }, Stamp(60), "seed");
        harness.Store.SeedGroup("t/acme/eng");
        harness.Store.SeedEdge("t/acme/eng", "existing");
        harness.Store.WriteBarrier = new AsyncBarrier(Racers);

        var outcomes = await Task.WhenAll(Enumerable.Range(0, Racers).Select(i =>
            OutcomeAsync(() => harness.Admin.AddGroupMemberAsync(Tenant, "eng", $"user-{i}")))
            .ToArray());

        var committed = await harness.Store.CountTenantEdgesAsync(TenantId.Parse(Tenant), CancellationToken.None);
        AssertCapHeld(outcomes, committed, baseline: 1, cap: 2, TenantAccessCaps.MembershipEdgesDimension);
        Assert.That(harness.Store.Edges.Count(e => e.MemberId == "existing"), Is.EqualTo(1), "only racers' own edges are withdrawn");
    }

    [Test]
    public async Task Concurrent_member_set_additions_at_the_cap_never_exceed_MaxMemberSubjects()
    {
        var barrier = new AsyncBarrier(Racers);
        var harness = new Harness(new LatticeSubject(Alice), enabled: true, identityDirectory: null, validationRequired: false, registryBarrier: barrier);
        var record = harness.Committed(Tenant);
        record.SetQuotas(new TenantQuotas { MaxMemberSubjects = 2 }, Stamp(60), "seed");
        record.AddMemberSubject("existing", Stamp(61), "seed");

        var outcomes = await Task.WhenAll(Enumerable.Range(0, Racers).Select(i =>
            OutcomeAsync(() => harness.Admin.AddMemberAsync(Tenant, $"user-{i}")))
            .ToArray());

        var committed = harness.Committed(Tenant);
        AssertCapHeld(outcomes, committed.MemberSubjectCount, baseline: 1, cap: 2, TenantAccessCaps.MemberSubjectsDimension);
        Assert.That(committed.HasMemberSubject("existing"), Is.True, "only racers' own entries are withdrawn");
    }

    [Test]
    public async Task A_lone_addition_that_lands_exactly_on_the_cap_is_kept()
    {
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");

        await harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "only" });

        Assert.That(harness.Store.HasGroup("t/acme/only"), Is.True);
    }

    [Test]
    public async Task A_post_write_overshoot_withdraws_the_new_group_with_its_edges()
    {
        // Model the racer that lost: its pre-write count was under the cap, but by
        // the time it re-counts another creator has landed.
        var harness = Build(Alice);
        harness.Committed(Tenant).SetQuotas(new TenantQuotas { MaxGroups = 1 }, Stamp(60), "seed");
        harness.Store.WriteBarrier = new AsyncBarrier(2);

        var first = OutcomeAsync(() => harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "a" }));
        var second = OutcomeAsync(() => harness.Admin.UpsertGroupAsync(Tenant, new TenantGroupDescriptor { Name = "b" }));
        var outcomes = await Task.WhenAll(first, second);

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(o => o is null), Is.LessThanOrEqualTo(1));
            Assert.That(harness.Store.HasGroup("t/acme/a") && harness.Store.HasGroup("t/acme/b"), Is.False);
            Assert.That(harness.Store.Edges.Where(e => e.GroupId.StartsWith("t/acme/", StringComparison.Ordinal)), Is.Empty);
        });
    }
}
