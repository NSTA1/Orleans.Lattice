using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Reflection;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage of the same-silo leaf revision cookie
/// (<c>BPlusLeafGrain.TryGetLeafRevision</c>), pinned as an invariant rather
/// than assumed.
/// <para>
/// The cookie is the sole same-silo freshness basis for
/// <c>LeafCacheGrain</c>, which returns early - skipping its cross-grain
/// refresh entirely - when the cookie it last observed still compares equal.
/// The TTL beside it is explicitly demoted in that code to "a bandwidth bound
/// for cross-silo", so on the same silo nothing else stands between a missed
/// bump and a stale read. Despite that, no fixture asserted the property the
/// whole mechanism rests on: that every change to a leaf's visible rows
/// advances the cookie.
/// </para>
/// <para>
/// These arms pin it. They exist because a second consumer (scan-page leaf-read
/// retention, issue #2786) rents the same invariant, and an argument that rents
/// an invariant maintained in code it does not own has shipped an unenforced
/// assumption unless it pins that invariant itself.
/// </para>
/// <para>
/// The cookie is bumped from a hand-maintained set of call sites rather than
/// from a single choke point, so the set is not closed under new mutation
/// paths. That is precisely why these arms are written against the observable
/// property - "the cookie advanced" - and not against the call sites.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Reads the process-wide cookie for the leaf <see cref="CreateGrain"/>
    /// builds from <paramref name="replicaId"/>, or <c>null</c> when no
    /// activation has published one on this silo.
    /// <para>
    /// The registry is process-wide and keyed by <see cref="GrainId"/>, and a
    /// test process is one silo for its purposes, so every arm below mints a
    /// fresh <paramref name="replicaId"/>. Sharing one across arms would let a
    /// previous arm's activation supply the cookie this one measures.
    /// </para>
    /// </summary>
    private static long? RevisionCookie(string replicaId) =>
        BPlusLeafGrain.TryGetLeafRevision(GrainId.Create("leaf", replicaId), out var revision)
            ? revision
            : null;

    private static string FreshReplicaId(string arm) => $"rev-{arm}-{Guid.NewGuid():N}";

    /// <summary>
    /// Reads the process-wide expiry horizon for the same leaf, or <c>null</c>
    /// when no range read has published one on this silo.
    /// </summary>
    private static long? ExpiryHorizon(string replicaId) =>
        BPlusLeafGrain.TryGetLeafExpiryHorizon(GrainId.Create("leaf", replicaId), out var horizon)
            ? horizon
            : null;

    /// <summary>
    /// Pins the production half of the scan-page reuse gate: a range read must
    /// publish the earliest expiry among the rows it surfaced.
    /// <para>
    /// The gate refuses reuse once <c>now</c> reaches the horizon, so a leaf
    /// that published <see cref="long.MaxValue"/> for a row that does expire
    /// would be reusable straight past that row's expiry - which is exactly the
    /// defect this arm exists to prevent recurring. Testing the gate alone
    /// cannot catch it, because the gate would be behaving correctly on a wrong
    /// input.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_range_read_publishes_the_earliest_expiry_among_the_rows_it_surfaced()
    {
        var replicaId = FreshReplicaId("horizon-finite");
        var grain = CreateGrain(replicaId: replicaId);

        var near = DateTimeOffset.UtcNow.AddMinutes(5).Ticks;
        var far = DateTimeOffset.UtcNow.AddMinutes(50).Ticks;
        await grain.SetAsync("k-far", Encoding.UTF8.GetBytes("v"), far);
        await grain.SetAsync("k-near", Encoding.UTF8.GetBytes("v"), near);

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Has.Count.EqualTo(2),
            "precondition: both rows must be live and surfaced, or the horizon below would be "
            + "measuring a read that saw nothing");

        Assert.That(
            ExpiryHorizon(replicaId),
            Is.EqualTo(near),
            "the read surfaced a row expiring at 'near', so the horizon must be 'near': it is "
            + "the earliest instant at which this answer changes with nothing written. A later "
            + "horizon lets a settled page be served past that row's expiry");
    }

    /// <summary>
    /// The control for the arm above: rows that never expire must publish
    /// <see cref="long.MaxValue"/>, not a finite value.
    /// <para>
    /// Without this, a publication that always reported "expires imminently"
    /// would satisfy the finite-horizon arm while disabling reuse outright,
    /// turning the whole optimisation off silently and with every test green.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_range_read_over_rows_that_never_expire_publishes_an_unbounded_horizon()
    {
        var replicaId = FreshReplicaId("horizon-none");
        var grain = CreateGrain(replicaId: replicaId);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Has.Count.EqualTo(2), "precondition: both rows must be surfaced");

        Assert.That(
            ExpiryHorizon(replicaId),
            Is.EqualTo(long.MaxValue),
            "no surfaced row expires, so nothing but a write can change this answer and the "
            + "horizon must not bound reuse at all");
    }

    /// <summary>
    /// An expired row must not drag the horizon backwards. It has already left
    /// the answer and cannot re-enter it, so folding its expiry in would pin
    /// the horizon in the past and refuse every reuse for the life of the
    /// activation - safe, but a silent, permanent loss of the optimisation.
    /// </summary>
    [Test]
    public async Task An_already_expired_row_does_not_bound_the_published_horizon()
    {
        var replicaId = FreshReplicaId("horizon-past");
        var grain = CreateGrain(replicaId: replicaId);

        await grain.SetAsync("k-live", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync(
            "k-gone",
            Encoding.UTF8.GetBytes("v"),
            DateTimeOffset.UtcNow.AddMinutes(-5).Ticks);

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "k-live" }),
            "precondition: the expired row must already be filtered out of the answer");

        Assert.That(
            ExpiryHorizon(replicaId),
            Is.EqualTo(long.MaxValue),
            "the expired row was never surfaced, so it cannot change this answer again and must "
            + "not bound the horizon");
    }

    /// <summary>
    /// A later read that surfaces nothing expiring must not raise the horizon
    /// back to unbounded.
    /// <para>
    /// The horizon is published per read, from the rows <em>that</em> read
    /// surfaced, but it is consumed per leaf. A narrower range that happens to
    /// contain no expiring row therefore computes <see cref="long.MaxValue"/>
    /// for itself while a settled page over the wider range is still retained
    /// and still reusable. Storing that value would hand the wider page an
    /// unbounded horizon and serve it straight past the expiry the first read
    /// established, which is the same defect the gate exists to prevent,
    /// reintroduced one layer down where the gate cannot see it.
    /// </para>
    /// <para>
    /// Min-merging is what closes it: the horizon only ever falls within an
    /// activation, so it is a lower bound over every read of the leaf rather
    /// than a record of the most recent one.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_later_read_that_surfaces_nothing_expiring_does_not_raise_the_horizon()
    {
        var replicaId = FreshReplicaId("horizon-no-raise");
        var grain = CreateGrain(replicaId: replicaId);

        var near = DateTimeOffset.UtcNow.AddMinutes(5).Ticks;
        await grain.SetAsync("a-plain", Encoding.UTF8.GetBytes("v"));
        await grain.SetAsync("b-near", Encoding.UTF8.GetBytes("v"), near);

        var wide = await grain.GetKeysAsync();
        Assert.That(wide, Is.EqualTo(new[] { "a-plain", "b-near" }),
            "precondition: the wide read must surface the expiring row, or it never establishes "
            + "the finite horizon this arm goes on to defend");
        Assert.That(ExpiryHorizon(replicaId), Is.EqualTo(near),
            "precondition: the wide read must have published the expiring row's instant");

        var narrow = await grain.GetKeysAsync("a", "b");

        // Load-bearing, not decoration. If this range were to surface 'b-near'
        // after all, the second read would republish 'near' and the assertion
        // below would hold whether the horizon min-merges or overwrites, which
        // is precisely the shape of an arm that proves nothing.
        Assert.That(narrow, Is.EqualTo(new[] { "a-plain" }),
            "precondition: the narrow read must surface only the row that never expires, so its "
            + "own computed horizon is unbounded and a raise is genuinely attempted");

        Assert.That(
            ExpiryHorizon(replicaId),
            Is.EqualTo(near),
            "the expiring row is still live and still reachable by a retained wider page, so the "
            + "leaf's horizon must stay at its earliest observed value. Letting the narrow read "
            + "raise it to long.MaxValue would make that wider page reusable past 'near'");
    }

    /// <summary>
    /// Deactivation must drop the leaf's published horizon alongside its
    /// cookie.
    /// <para>
    /// Both registries are process-wide and keyed by <c>GrainId</c>, so an
    /// entry a deactivating activation leaves behind is inherited verbatim by
    /// the next activation of the same leaf on the same silo. Nothing in the
    /// horizon's own value records which activation published it.
    /// </para>
    /// <para>
    /// The reuse gate would survive the omission today, because it also
    /// requires the cookie, and the seed floor guarantees the next activation
    /// publishes a strictly higher one. That is defence in depth resting on a
    /// second mechanism, not on this one, and it does nothing about the leak:
    /// without the removal the horizon map grows once per leaf activation for
    /// the life of the silo and is never swept.
    /// </para>
    /// </summary>
    [Test]
    public async Task Deactivation_drops_the_leaf_expiry_horizon()
    {
        var replicaId = FreshReplicaId("horizon-deactivate");
        var grain = CreateGrain(replicaId: replicaId);

        var near = DateTimeOffset.UtcNow.AddMinutes(5).Ticks;
        await grain.SetAsync("k-near", Encoding.UTF8.GetBytes("v"), near);

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "k-near" }),
            "precondition: the read must surface the expiring row, or no horizon is published "
            + "and the assertion below would hold with the removal deleted");

        // Load-bearing, not decoration. Without it the closing assertion is
        // satisfied by a leaf that never published a horizon at all, which is
        // the exact shape of an arm that passes whether or not the clause it
        // names is present.
        Assert.That(ExpiryHorizon(replicaId), Is.EqualTo(near),
            "precondition: the leaf must be holding a published horizon to be deprived of");

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.That(
            ExpiryHorizon(replicaId),
            Is.Null,
            "the activation that published this horizon is gone, so the horizon must go with it: "
            + "leaving it keyed by GrainId hands it to the next activation of the same leaf and "
            + "grows the map once per activation for the life of the silo");
    }

    [Test]
    public async Task A_write_advances_the_leaf_revision_cookie()
    {
        var replicaId = FreshReplicaId("write");
        var grain = CreateGrain(replicaId: replicaId);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var afterFirst = RevisionCookie(replicaId);

        Assert.That(
            afterFirst,
            Is.Not.Null,
            "a leaf that has taken a write must have published a cookie; LeafCacheGrain "
            + "treats absence as 'refresh', so this failing means the fast path is simply "
            + "never taken rather than that it is taken wrongly");

        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        Assert.That(
            RevisionCookie(replicaId),
            Is.GreaterThan(afterFirst!.Value),
            "a second write left the cookie unchanged, so a same-silo reader holding the "
            + "earlier value would conclude 'provably fresh' and never observe it");
    }

    [Test]
    public async Task A_delete_advances_the_leaf_revision_cookie()
    {
        var replicaId = FreshReplicaId("delete");
        var grain = CreateGrain(replicaId: replicaId);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var afterWrite = RevisionCookie(replicaId);

        Assert.That(await grain.DeleteAsync("k1"), Is.True, "the delete must actually remove a row");

        Assert.That(
            RevisionCookie(replicaId),
            Is.GreaterThan(afterWrite!.Value),
            "a delete left the cookie unchanged; removal is a change to the visible row "
            + "set and a reader that misses it serves a deleted value");
    }

    [Test]
    public async Task A_division_advances_the_leaf_revision_cookie_when_rows_leave_the_donor()
    {
        var replicaId = FreshReplicaId("division");
        // The sibling must satisfy GetGrainId(), which the division path calls
        // on it, so it is substituted as IGrainBase as well and given a context.
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(Task.CompletedTask);
        sibling.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);
        var migrated = new List<string>();
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(call =>
            {
                migrated.AddRange(call.Arg<Dictionary<string, LwwValue<byte[]>>>().Keys);
                return Task.FromResult<SplitResult?>(null);
            });

        var grain = CreateGrain(replicaId: replicaId, siblingStub: sibling, maxLeafKeys: 64);

        for (var i = 0; i < 16; i++)
        {
            await grain.SetAsync($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // Taken after the seeding writes so that what follows measures the
        // division alone. A division triggered by a write would be masked by
        // that write's own bump.
        var beforeDivision = RevisionCookie(replicaId);
        Assert.That(beforeDivision, Is.Not.Null);

        var split = typeof(BPlusLeafGrain).GetMethod(
            "SplitAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(
            split,
            Is.Not.Null,
            "SplitAsync not found - was it renamed? It is the division entry point, and "
            + "driving it directly is what isolates the transfer from a triggering write");

        await (Task<SplitResult>)split!.Invoke(grain, [])!;

        // Vacuity control, and it is load-bearing: the cookie assertion below
        // reddens identically whether the division moved rows without bumping
        // or never moved a row at all. Only the first is the defect this arm
        // names, so the transfer must be shown to have genuinely happened
        // before the cookie is read at all.
        Assert.That(
            migrated,
            Is.Not.Empty,
            "no rows reached the sibling, so the division did not occur and the cookie "
            + "assertion below would redden vacuously rather than reporting a missed bump");

        foreach (var key in migrated)
        {
            Assert.That(
                await grain.GetAsync(key),
                Is.Null,
                $"migrated key '{key}' is still readable from the donor, so the rows did "
                + "not actually leave and the cookie had nothing to report");
        }

        Assert.That(
            RevisionCookie(replicaId),
            Is.GreaterThan(beforeDivision!.Value),
            "a division moved rows out of the donor without advancing the cookie. Every "
            + "migrated key is now absent from this leaf, so a same-silo reader holding "
            + "the pre-division cookie concludes 'provably fresh' and keeps serving rows "
            + "this leaf no longer owns");
    }

    [Test]
    public async Task Materialising_rows_without_changing_them_does_not_advance_the_leaf_revision_cookie()
    {
        var replicaId = FreshReplicaId("residency");
        var grain = CreateGrain(replicaId: replicaId);

        for (var i = 0; i < 8; i++)
        {
            await grain.SetAsync($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var afterWrites = RevisionCookie(replicaId);
        Assert.That(afterWrites, Is.Not.Null);

        // Pure reads. These traverse the cache and may materialise or evict
        // rows, which moves them in and out of the backing dictionary without
        // changing the value of any of them.
        for (var i = 0; i < 8; i++)
        {
            await grain.GetAsync($"k{i:D4}");
        }

        _ = grain.CacheForTest.Count;

        Assert.That(
            RevisionCookie(replicaId),
            Is.EqualTo(afterWrites),
            "reading advanced the cookie. Residency is not mutation: a row becoming "
            + "resident, or being evicted, does not change the visible row set, and "
            + "bumping there invalidates every dependent cache on a pure read. This arm "
            + "is the one that fails if a future maintainer 'fixes' an apparently missing "
            + "bump in EvictBlock, whose _rows.Remove(key) is textually identical to the "
            + "genuine removal in Remove and means the opposite thing");
    }
}
