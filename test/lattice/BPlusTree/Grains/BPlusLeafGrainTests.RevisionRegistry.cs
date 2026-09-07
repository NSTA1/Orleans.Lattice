using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests that lock in the same-silo revision-registry optimisation on
/// <see cref="BPlusLeafGrain"/>. The optimisation publishes a per-leaf
/// <see cref="StrongBox{T}"/> of <see cref="long"/> into a process-wide
/// dictionary on first state-advance and updates it via
/// <see cref="System.Threading.Volatile.Write(ref long, long)"/> on every
/// subsequent tick, so the steady-state bump path is allocation-free
/// and the cross-grain <see cref="LeafCacheGrain"/> can short-circuit
/// its refresh RPC when nothing has advanced. These tests are the
/// regression net for that contract: a future change that reverts the
/// dict value type to <see cref="long"/> (which would re-allocate per
/// indexer-set on the bump) or drops the lazy publish-once semantics
/// will fail the structural-invariant assertions below.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds a <see cref="BPlusLeafGrain"/> whose <see cref="GrainId"/>
    /// is uniquely scoped to the calling test, so the process-wide
    /// <see cref="ConcurrentDictionary{TKey, TValue}"/> backing the
    /// revision registry never sees collisions across parallel-fixture
    /// runs and per-test state cannot leak between assertions.
    /// </summary>
    private static (BPlusLeafGrain grain, GrainId leafId) CreateLeafWithUniqueId(
        string testName,
        FakePersistentState<LeafNodeState>? state = null,
        int maxLeafKeys = 128)
    {
        var unique = $"{testName}-{Guid.NewGuid():N}";
        var grain = CreateGrain(state, replicaId: unique, maxLeafKeys: maxLeafKeys);
        var leafId = GrainId.Create("leaf", unique);
        return (grain, leafId);
    }

    /// <summary>
    /// Cross-fixture accessor for <see cref="CreateGrain"/>. Exposed so
    /// the sibling <see cref="LeafCacheGrainTests"/> partial can wire a
    /// real <see cref="BPlusLeafGrain"/> whose writes populate the
    /// process-wide revision registry while the cache itself talks to
    /// a separate mocked <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/>. Internal so it
    /// stays inside the test assembly's own namespace surface.
    /// </summary>
    internal static BPlusLeafGrain CreateLeafGrainForCrossFixtureUse(string replicaId)
        => CreateGrain(replicaId: replicaId);

    /// <summary>
    /// Reflective accessor for the static
    /// <see cref="ConcurrentDictionary{TKey, TValue}"/> holding the
    /// published <see cref="StrongBox{T}"/> per leaf. Used by the
    /// structural-invariant tests below to assert that bumps reuse a
    /// stable <see cref="StrongBox{T}"/> reference and don't allocate
    /// per call. If a future refactor reverts the dict value type to
    /// <c>long</c> the cast in this helper fails and every dependent
    /// test fails loudly, surfacing the regression at unit-test time
    /// rather than only as a microbench allocation regression months
    /// later.
    /// </summary>
    private static StrongBox<long>? ReadRegistryBoxForTest(GrainId leafId)
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "LeafRevisionRegistry",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                "LeafRevisionRegistry field not found on BPlusLeafGrain - the static field's "
                + "name has changed; update this test helper to match.");

        var dict = field.GetValue(null)
            ?? throw new InvalidOperationException("LeafRevisionRegistry field returned null");

        if (dict is not ConcurrentDictionary<GrainId, StrongBox<long>> typed)
        {
            throw new InvalidOperationException(
                "LeafRevisionRegistry field is not a ConcurrentDictionary<GrainId, StrongBox<long>> "
                + $"(got {dict.GetType().FullName}). The optimisation contract requires the value "
                + "type to be StrongBox<long> so bumps can update a stable mutable field via "
                + "Volatile.Write without re-allocating per tick. If the registry's value type has "
                + "intentionally changed, also update this test helper.");
        }

        return typed.TryGetValue(leafId, out var box) ? box : null;
    }

    [Test]
    public void TryGetLeafRevision_returns_false_for_unbumped_leaf()
    {
        // Cross-silo simulation: a leaf that has never been activated on
        // this process has no entry in the registry, and TryGetLeafRevision
        // must return false so the cache falls through to its existing
        // cross-grain refresh path.
        var leafId = GrainId.Create("leaf", $"unbumped-{Guid.NewGuid():N}");

        var ok = BPlusLeafGrain.TryGetLeafRevision(leafId, out var revision);

        Assert.That(ok, Is.False);
        Assert.That(revision, Is.EqualTo(0));
    }

    [Test]
    public async Task Set_publishes_revision_cookie_observable_via_TryGetLeafRevision()
    {
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(Set_publishes_revision_cookie_observable_via_TryGetLeafRevision));

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var revision), Is.True);
        Assert.That(revision, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task Successive_writes_advance_revision_cookie_monotonically()
    {
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(Successive_writes_advance_revision_cookie_monotonically));

        long previous = 0;
        for (int i = 0; i < 5; i++)
        {
            await grain.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));
            Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var rev), Is.True);
            Assert.That(rev, Is.GreaterThan(previous), $"revision did not advance after write #{i}");
            previous = rev;
        }
    }

    [Test]
    public async Task Delete_advances_revision_cookie()
    {
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(Delete_advances_revision_cookie));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        BPlusLeafGrain.TryGetLeafRevision(leafId, out var afterSet);

        await grain.DeleteAsync("k1");

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var afterDelete), Is.True);
        Assert.That(afterDelete, Is.GreaterThan(afterSet));
    }

    [Test]
    public async Task DeleteRange_advances_revision_cookie()
    {
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(DeleteRange_advances_revision_cookie));
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        BPlusLeafGrain.TryGetLeafRevision(leafId, out var afterSet);

        await grain.DeleteRangeAsync("a", "z");

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var afterRange), Is.True);
        Assert.That(afterRange, Is.GreaterThan(afterSet));
    }

    [Test]
    public async Task OnDeactivateAsync_removes_revision_cookie_from_registry()
    {
        // The registry must be pruned on deactivation so a future
        // re-activation gets a fresh StrongBox; if the entry leaked the
        // cache could see the dangling old box's stale value as
        // "no advance" forever after a re-activation.
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(OnDeactivateAsync_removes_revision_cookie_from_registry));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.True, "precondition: cookie should be published");

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.False,
            "revision cookie should be removed from the registry on deactivation");
    }

    [Test]
    public async Task OnActivateAsync_publishes_a_revision_cookie()
    {
        // GUARD, not a discriminator (issue #2151 is explicit about this):
        // asserting merely that activation publishes SOME cookie passes even
        // when activations reuse each other's values, which is the more
        // serious of the two defects. The value of this test is narrow and
        // stated deliberately: it pins the presence of the entry, so a later
        // refactor that drops the publish from the activation path fails
        // here rather than silently returning caches to the TTL gate after
        // every projection rebuild. The equal-value collision is covered by
        // Re_activation_never_republishes_a_cookie_value_from_a_previous_activation.
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(OnActivateAsync_publishes_a_revision_cookie));

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.False,
            "precondition: nothing published before activation");

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var revision), Is.True,
            "activation must publish a cookie: the snapshot rehydrate and WAL replay rebuild the "
            + "projection without going through any bumping foreground site, so without an explicit "
            + "publish a re-activated leaf leaves the registry empty and a cache holding a cookie "
            + "from the previous activation falls back to its TTL gate.");
        Assert.That(revision, Is.GreaterThan(0));
    }

    [Test]
    public async Task Re_activation_never_republishes_a_cookie_value_from_a_previous_activation()
    {
        // ABA discriminator (issue #2151). The cache compares cookies for
        // EQUALITY only, so the whole requirement is that a value observed
        // under one activation is never republished by another. Seeding each
        // activation at 0 broke exactly that: the registry entry is removed
        // on deactivation and re-created on the next activation, so
        // activation B's first bump reproduced activation A's first bump and
        // a cache holding A's stamp returned early on "provably fresh".
        //
        // Classified as a DISCRIMINATOR, not a guard: it fails on the
        // pre-fix seed (both activations publish 1, 2, 3, ...) and passes
        // only once activations are seeded from disjoint ranges. A weaker
        // assertion - that the re-activated leaf simply publishes SOME
        // cookie - passes with the defect fully present.
        var unique = $"reactivate-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var first = CreateGrain(replicaId: unique);
        var observedUnderFirst = new List<long>();
        for (int i = 0; i < 5; i++)
        {
            await first.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));
            Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var rev), Is.True);
            observedUnderFirst.Add(rev);
        }

        await ((IGrainBase)first).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out _), Is.False);

        // Second activation of the same GrainId, driven through the same
        // number of writes as the first so that a per-activation counter
        // would reproduce the first activation's values one for one.
        var second = CreateGrain(replicaId: unique);
        var observedUnderSecond = new List<long>();
        for (int i = 0; i < 5; i++)
        {
            await second.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));
            Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var rev), Is.True);
            observedUnderSecond.Add(rev);
        }

        Assert.That(observedUnderSecond, Is.Not.Empty);
        Assert.That(
            observedUnderSecond.Intersect(observedUnderFirst),
            Is.Empty,
            "a re-activated leaf republished a cookie value that a cache may still hold from the "
            + "previous activation; the cache compares cookies for equality, so it would return "
            + "early on 'provably fresh' against different state (issue #2151 ABA).");

        // The values must also advance within the new activation, so the
        // seed change does not accidentally freeze the counter.
        Assert.That(observedUnderSecond, Is.Ordered.Ascending);
    }

    /// <summary>
    /// Reflective accessor for the private <c>RevisionSeedShift</c> const, so
    /// the overrun test below derives its arithmetic from the production
    /// constant rather than hard-coding a copy of it that could silently
    /// diverge if the width is ever retuned.
    /// </summary>
    private static int ReadRevisionSeedShiftForTest()
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "RevisionSeedShift",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                "RevisionSeedShift const not found on BPlusLeafGrain - the field's name has "
                + "changed; update this test helper to match.");

        return (int)(field.GetRawConstantValue()
            ?? throw new InvalidOperationException("RevisionSeedShift returned null"));
    }

    /// <summary>
    /// Reflective accessor for the private <c>_revisionSeed</c> ticket source,
    /// so the overrun test can assert the deactivate-time floor advance
    /// directly rather than only through its downstream effect on the next
    /// activation's seed.
    /// </summary>
    private static long ReadRevisionSeedForTest()
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "_revisionSeed",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                "_revisionSeed field not found on BPlusLeafGrain - the field's name has changed; "
                + "update this test helper to match.");

        return (long)(field.GetValue(null)
            ?? throw new InvalidOperationException("_revisionSeed returned null"));
    }

    [Test]
    public async Task Re_activation_seeds_above_an_activation_that_overran_its_ticket_range()
    {
        // DISCRIMINATOR for the deactivate-time seed floor advance (issue
        // #2151, PM ruling on PR #2166). The ticket shift reserves 2^24
        // cookie values per activation, but that width is a spacing HINT and
        // not the uniqueness guarantee: an activation that bumps past its
        // reserved range walks into the values a later ticket would seed at,
        // and the collision conditions are CORRELATED rather than
        // independent - a leaf hot enough to overrun is on a workload where
        // activations are rare, so the global ticket source has barely moved
        // and the next ticket is exactly the adjacent one.
        //
        // Uniqueness is therefore made unconditional by raising the ticket
        // source past the activation's final counter value when it
        // deactivates. This test drives the box past its range directly
        // rather than performing the real bumps, which would be the same
        // assertion at several minutes of runtime.
        //
        // THE OVERRUN MARGIN IS A FLAKE GUARD, NOT THE DISCRIMINATING
        // MECHANISM - stated precisely because an earlier version of this
        // comment claimed the opposite on the strength of a control arm that
        // had not in fact executed this test (the filter substring did not
        // match its name). Measured properly: the control fails at a
        // three-ticket margin AND at the margin below, so the margin is not
        // what earns the red. What the margin buys is immunity to a false
        // GREEN: the ticket source is process-wide and shared with every
        // other fixture in the run, and ambient ticket consumption can only
        // push the seed UP - satisfying the first assertion for a reason
        // unrelated to the floor advance. A margin beyond what a whole test
        // run can consume (tickets are bounded by leaf activations, order
        // 10^4) removes that window rather than narrowing it.
        var shift = ReadRevisionSeedShiftForTest();
        var unique = $"overrun-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var first = CreateGrain(replicaId: unique);
        await first.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var box = ReadRegistryBoxForTest(leafId)
            ?? throw new InvalidOperationException("first activation published no cookie");

        var overrun = box.Value + ((1L << 16) << shift);
        box.Value = overrun;

        await ((IGrainBase)first).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // The primary assertion: deactivation carried the activation's final
        // counter value into the ticket source. Asserted directly, because
        // the downstream effect alone is satisfiable by ambient ticket
        // consumption whereas this is not.
        Assert.That(
            ReadRevisionSeedForTest(),
            Is.GreaterThanOrEqualTo(overrun >> shift),
            "deactivation removed the registry entry without carrying the activation's final "
            + "counter value into the ticket source, so a later activation of the same leaf can "
            + "seed inside the range this one already published (issue #2151 ABA). Removal and "
            + "floor advance must happen together.");

        var second = CreateGrain(replicaId: unique);
        await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var republished), Is.True);
        Assert.That(
            republished,
            Is.GreaterThan(overrun),
            "a re-activated leaf seeded at or below a cookie value the PREVIOUS activation had "
            + "already published, so the two activations' value ranges overlap and a cache "
            + "holding a stamp from the first can compare equal to the second (issue #2151 ABA).");
    }

    [Test]
    public async Task Bump_publishes_to_a_stable_StrongBox_reference()
    {
        // Structural-invariant test: the same StrongBox<long> reference
        // is reused across bumps. This locks in the publish-once design
        // - lazy GetOrAdd on first bump, in-place Volatile.Write on every
        // subsequent bump. If a future change reverts to a per-tick
        // dict-indexer set (which would silently re-allocate on every
        // tick - the regression seen in the cycle-7 v1 candidate) the
        // StrongBox identity comparison below catches it before it
        // reaches benchmark history.
        var (grain, leafId) = CreateLeafWithUniqueId(nameof(Bump_publishes_to_a_stable_StrongBox_reference));

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v"));
        var box1 = ReadRegistryBoxForTest(leafId);
        Assert.That(box1, Is.Not.Null, "first bump should publish a StrongBox");
        // Snapshot the box's value BEFORE the second bump; box1 and
        // box2 are intentionally the same reference (that is the
        // identity invariant under test), so reading box1.Value after
        // the second write would see the same updated value as
        // box2.Value and the monotonic-advancement check would
        // tautologically fail.
        var valueBeforeSecondBump = box1!.Value;

        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v"));
        var box2 = ReadRegistryBoxForTest(leafId);

        Assert.That(box2, Is.SameAs(box1),
            "BumpLocalRevision must not allocate a new StrongBox on every tick. "
            + "The same reference must be reused so tight write loops (e.g. SetManyAsync over a "
            + "thousand-key batch) stay allocation-free in the steady state.");
        Assert.That(box2!.Value, Is.GreaterThan(valueBeforeSecondBump),
            "the published box's value must advance monotonically across bumps");
    }

    [Test]
    public async Task Steady_state_bumps_do_not_allocate_per_tick()
    {
        // Allocation-invariant test: after the first bump (which lazily
        // GetOrAdds the StrongBox into the registry), N subsequent bumps
        // must contribute zero allocations from the bump path itself.
        // BumpLocalRevision can't be isolated from SetAsync's ambient
        // allocations (LwwValue, mutation envelope, HLC tick), but we
        // can compare per-iteration allocation cost across two windows
        // of the same size: if the per-write cost is constant, the bump
        // path is alloc-free; if a future change reintroduces a per-bump
        // allocation (e.g. the cycle-7 v1 dict-indexer regression of
        // ~224 B per tick) the second window's mean per-write cost
        // diverges from the first by exactly the per-bump regression cost.
        // maxLeafKeys is set high so the warmup + measurement windows
        // (~1200 writes) never trigger a split, which would call into
        // a sibling-grain mock and inject split-path allocations into
        // the second window.
        var (grain, _) = CreateLeafWithUniqueId(
            nameof(Steady_state_bumps_do_not_allocate_per_tick),
            maxLeafKeys: int.MaxValue);

        // Warm-up: force JIT, dictionary node allocation for state.Entries,
        // and the initial StrongBox publish.
        for (int i = 0; i < 200; i++)
        {
            await grain.SetAsync($"warmup-{i}", Encoding.UTF8.GetBytes("v"));
        }

        const int iterationCount = 500;

        var beforeWindow1 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterationCount; i++)
        {
            await grain.SetAsync($"win1-{i}", Encoding.UTF8.GetBytes("v"));
        }
        var window1 = GC.GetAllocatedBytesForCurrentThread() - beforeWindow1;

        var beforeWindow2 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterationCount; i++)
        {
            await grain.SetAsync($"win2-{i}", Encoding.UTF8.GetBytes("v"));
        }
        var window2 = GC.GetAllocatedBytesForCurrentThread() - beforeWindow2;

        // The two windows should allocate roughly the same amount per
        // iteration. We allow a 25% tolerance for ambient noise (background
        // dictionary resizes inside state.Entries, async machinery, etc.)
        // - the regression we are guarding against was 224 B per bump,
        // i.e. ~+200% for the bump path alone, well outside the noise band.
        var window1PerIter = window1 / (double)iterationCount;
        var window2PerIter = window2 / (double)iterationCount;
        var ratio = window2PerIter / Math.Max(1.0, window1PerIter);
        Assert.That(ratio, Is.InRange(0.75, 1.25),
            $"per-write allocation diverged across windows (window1={window1PerIter:F1} B/op, "
            + $"window2={window2PerIter:F1} B/op, ratio={ratio:F2}). "
            + "BumpLocalRevision must remain allocation-free in the steady state - if this fails, "
            + "a recent change has reintroduced a per-tick allocation in the bump path.");
    }
}
