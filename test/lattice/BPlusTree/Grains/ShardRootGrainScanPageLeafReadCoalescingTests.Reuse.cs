using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2786. The arms for reusing a <em>settled</em> leaf read, which the
/// coalescing half of this fixture deliberately refused because it had no basis
/// for believing the leaf had not moved on.
/// <para>
/// <b>The basis, and why it is not recency.</b> A leaf publishes an
/// activation-fenced revision cookie that advances on every state-advancing
/// operation. The shard samples it <em>before</em> issuing a read and compares
/// it at attach time; equal cookies mean the leaf published no mutation across
/// a window that strictly contains the read, so its rows are provably
/// unchanged rather than merely fresh. Every other outcome refuses.
/// </para>
/// <para>
/// <b>The positive arm is the vacuity control for every refusal arm here, and
/// that is structural rather than incidental.</b> These arms drive the cookie
/// through the process-wide registry directly. Were that manipulation
/// ineffective - a renamed field, a changed value type, a leaf id that does not
/// match the one the shard reads - the cookie would be absent, absence refuses,
/// and every refusal arm would pass while proving nothing. Only
/// <see cref="A_settled_read_is_served_again_when_the_leaf_publishes_an_unchanged_cookie"/>
/// can distinguish those two worlds, because it is the only one that requires
/// the cookie to be genuinely present and genuinely stable.
/// </para>
/// </summary>
public partial class ShardRootGrainScanPageLeafReadCoalescingTests
{
    private readonly List<GrainId> _publishedRevisions = [];

    /// <summary>
    /// The registry is process-wide and outlives a test, so an entry left
    /// behind would make an unrelated fixture's settled read reusable. Removing
    /// it restores the default that every other arm depends on: a leaf that
    /// publishes nothing.
    /// </summary>
    [TearDown]
    public void ClearPublishedRevisions()
    {
        foreach (var leafId in _publishedRevisions)
        {
            RegistryForTest().TryRemove(leafId, out _);
            HorizonRegistryForTest().TryRemove(leafId, out _);
        }

        _publishedRevisions.Clear();
    }

    /// <summary>
    /// Reflective handle on the static registry <see cref="BPlusLeafGrain"/>
    /// publishes expiry horizons into, with the same fail-loud contract as
    /// <see cref="RegistryForTest"/>.
    /// </summary>
    private static ConcurrentDictionary<GrainId, StrongBox<long>> HorizonRegistryForTest()
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "LeafExpiryHorizonRegistry",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                "LeafExpiryHorizonRegistry field not found on BPlusLeafGrain - the static field's "
                + "name has changed. These arms drive it directly; update this helper to match.");

        var value = field.GetValue(null)
            ?? throw new InvalidOperationException("LeafExpiryHorizonRegistry field returned null");

        return value as ConcurrentDictionary<GrainId, StrongBox<long>>
            ?? throw new InvalidOperationException(
                "LeafExpiryHorizonRegistry is not a ConcurrentDictionary<GrainId, StrongBox<long>> "
                + $"(got {value.GetType().FullName}).");
    }

    /// <summary>
    /// Publishes <paramref name="horizonTicks"/> as the leaf's expiry horizon
    /// and asserts the shard's own accessor reads it back, for the same reason
    /// <see cref="PublishRevision"/> asserts its own read-back.
    /// </summary>
    private void PublishExpiryHorizon(GrainId leafId, long horizonTicks)
    {
        HorizonRegistryForTest().AddOrUpdate(
            leafId,
            _ => new StrongBox<long>(horizonTicks),
            (_, box) =>
            {
                Volatile.Write(ref box.Value, horizonTicks);
                return box;
            });

        if (!_publishedRevisions.Contains(leafId))
        {
            _publishedRevisions.Add(leafId);
        }

        Assert.That(BPlusLeafGrain.TryGetLeafExpiryHorizon(leafId, out var readBack), Is.True,
            "precondition: the horizon must be readable through the accessor the shard uses");
        Assert.That(readBack, Is.EqualTo(horizonTicks),
            "precondition: and must read back the value just published");
    }

    /// <summary>
    /// Reflective handle on the static registry <see cref="BPlusLeafGrain"/>
    /// publishes cookies into. It throws rather than returning null on every
    /// shape change, so a rename or a value-type change surfaces as a named
    /// failure here instead of silently reverting these arms to the
    /// absent-cookie path they are written to avoid.
    /// </summary>
    private static ConcurrentDictionary<GrainId, StrongBox<long>> RegistryForTest()
    {
        var field = typeof(BPlusLeafGrain).GetField(
            "LeafRevisionRegistry",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException(
                "LeafRevisionRegistry field not found on BPlusLeafGrain - the static field's name "
                + "has changed. These arms drive it directly; update this helper to match, and do "
                + "not assume the arms still reach the clause they name until the positive arm "
                + "passes again.");

        var value = field.GetValue(null)
            ?? throw new InvalidOperationException("LeafRevisionRegistry field returned null");

        return value as ConcurrentDictionary<GrainId, StrongBox<long>>
            ?? throw new InvalidOperationException(
                "LeafRevisionRegistry is not a ConcurrentDictionary<GrainId, StrongBox<long>> "
                + $"(got {value.GetType().FullName}).");
    }

    /// <summary>
    /// Publishes <paramref name="revision"/> for <paramref name="leafId"/> and
    /// asserts the shard's own accessor can read it back. The read-back is the
    /// point: publishing into a registry the production accessor does not
    /// consult would leave every arm here green and meaningless.
    /// </summary>
    private void PublishRevision(GrainId leafId, long revision)
    {
        RegistryForTest().AddOrUpdate(
            leafId,
            _ => new StrongBox<long>(revision),
            (_, box) =>
            {
                Volatile.Write(ref box.Value, revision);
                return box;
            });

        if (!_publishedRevisions.Contains(leafId))
        {
            _publishedRevisions.Add(leafId);
        }

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(leafId, out var readBack), Is.True,
            "precondition: the cookie must be readable through the accessor the shard uses, or "
            + "every arm in this file degrades to the absent-cookie refusal and proves nothing");
        Assert.That(readBack, Is.EqualTo(revision),
            "precondition: and must read back the value just published");

        // A real range read publishes a horizon alongside the cookie, so a
        // harness that published only the cookie would leave every arm on the
        // absent-horizon refusal and prove nothing. long.MaxValue is what a
        // read over rows that never expire publishes, which is the condition
        // these arms are written under; the arm that exercises a finite horizon
        // sets it explicitly.
        if (!HorizonRegistryForTest().ContainsKey(leafId))
        {
            PublishExpiryHorizon(leafId, long.MaxValue);
        }
    }

    /// <summary>
    /// The positive arm, and the vacuity control for the rest of the file. A
    /// settled read whose leaf still publishes its issue-time cookie is served
    /// again without a second read reaching the leaf.
    /// <para>
    /// The assertion is on the read count, not on the page being correct: a
    /// correct page is exactly what a fresh read also produces, so asserting
    /// correctness alone would pass under the pre-#2786 behaviour this arm
    /// exists to distinguish from.
    /// </para>
    /// <para>
    /// Correctness is still asserted alongside it, because a reused page that
    /// returned the wrong rows would otherwise satisfy the count.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_settled_read_is_served_again_when_the_leaf_publishes_an_unchanged_cookie()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-stable");
        PublishRevision(chain.LeafId, 4242);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        chain.ReleasePark();
        await Task.Yield();

        // Nothing writes to the leaf, so its cookie is unchanged.
        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(1),
                "the settled read must be served again: its leaf published the same cookie it "
                + "published before that read was issued, so no mutation can have intervened");
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "and the rows it serves must be the leaf's rows, not an empty or partial page");
            Assert.That(page.Entries, Is.Not.Empty,
                "vacuity control: a page with no rows would satisfy the count assertion above "
                + "while demonstrating nothing about what was served");
        });
    }

    /// <summary>
    /// The refusal arm that the pre-#2786 fixture could not reach: a cookie
    /// that is published and <em>moves</em>. The settled rows must be
    /// discarded and the leaf read again.
    /// <para>
    /// Reverting the equality comparison to an unconditional reuse reddens this
    /// and not the positive arm, which is what makes the two independent rather
    /// than two readings of one behaviour.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_settled_read_is_refused_when_the_leaf_revision_cookie_has_advanced()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-advanced");
        PublishRevision(chain.LeafId, 900);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        chain.ReleasePark();
        await Task.Yield();

        // A delete commits, and the leaf advances its cookie as a real leaf
        // does on any state-advancing operation.
        var deleted = chain.Rows[^1].Key;
        chain.Rows.RemoveAt(chain.Rows.Count - 1);
        PublishRevision(chain.LeafId, 901);

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "an advanced cookie must force a fresh read; serving the settled rows would "
                + "return a page taken before the delete");
            Assert.That(page.Entries.Select(e => e.Key), Does.Not.Contain(deleted),
                "and the page must observe the delete");
        });
    }

    /// <summary>
    /// The refusal arm for the half a revision cookie cannot see: a row that
    /// leaves the answer because its TTL elapsed, with nothing written.
    /// <para>
    /// <b>This is the arm that was missing when the mechanism first shipped,
    /// and CI caught it rather than any local run.</b> A range read filters
    /// rows against the wall clock sampled at read time, so it is not a pure
    /// function of mutation state. The cookie is unchanged here - correctly,
    /// because no writer ran - and reuse must still be refused, because the
    /// answer has moved underneath it. Without the horizon clause the settled
    /// page is served and the caller sees a row that expired.
    /// </para>
    /// <para>
    /// The cookie is deliberately held <em>equal</em> so this arm cannot pass
    /// for the wrong reason: every other refusal in this file works by moving
    /// or removing the cookie, and if that were what reddened here the arm
    /// would be a second reading of an existing behaviour rather than a test
    /// of the horizon. Remove the horizon clause and only this arm reddens.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_settled_read_is_refused_once_a_surfaced_row_has_expired()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-expired");
        PublishRevision(chain.LeafId, 5150);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        chain.ReleasePark();
        await Task.Yield();

        // The cookie does not move: nothing is written. What moves is the
        // clock, past the expiry of a row the settled read surfaced.
        PublishExpiryHorizon(chain.LeafId, DateTimeOffset.UtcNow.Ticks - TimeSpan.TicksPerSecond);

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(chain.LeafId, out var unchanged), Is.True);
        Assert.That(unchanged, Is.EqualTo(5150),
            "precondition: the cookie must still be the issue-time value, or this arm would be "
            + "reddening through the advanced-cookie clause and proving nothing about expiry");

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "an elapsed expiry horizon must force a fresh read: the settled page was taken "
                + "while the row was live, and serving it now would surface an expired row");
            Assert.That(page.Entries, Is.Not.Empty,
                "vacuity control: the fresh read must actually have produced a page");
        });
    }

    /// <summary>
    /// The discriminating arm for sampling the cookie <em>before</em> the read
    /// rather than at its completion, and the only arm that can tell those two
    /// designs apart.
    /// <para>
    /// A write is committed while the read is parked - that is, after the leaf
    /// would have materialised its answer and before the task completes. Under
    /// the shipped pre-stamp the entry carries the cookie as it was at issue,
    /// which no longer matches, so reuse is refused. Under a post-stamp the
    /// cookie would be sampled at completion, would equal the current value,
    /// and the next attempt would be served rows that predate the write while
    /// the comparison reported them provably unchanged.
    /// </para>
    /// <para>
    /// This is the arm to consult before "simplifying" the stamp to the
    /// settle continuation, where it would read more naturally and be wrong.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_write_landing_while_the_read_was_in_flight_refuses_reuse()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-prestamp");
        PublishRevision(chain.LeafId, 70);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls, read still parked");

        // The write lands while the read is in flight. ReleasePark materialises
        // the rows as they were BEFORE this, exactly as a real leaf read does.
        var deleted = chain.Rows[^1].Key;
        chain.Rows.RemoveAt(chain.Rows.Count - 1);
        PublishRevision(chain.LeafId, 71);

        chain.ReleasePark();
        await Task.Yield();

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "the cookie is sampled before the read is issued, so a write landing during its "
                + "flight is visible as a change and must force a fresh read - sampling at "
                + "completion would compare the post-write cookie against itself and reuse");
            Assert.That(page.Entries.Select(e => e.Key), Does.Not.Contain(deleted),
                "and the page must observe the write that landed mid-flight");
        });
    }

    /// <summary>
    /// The leaf deactivates after its read settles, so its cookie is no longer
    /// readable. An unreadable cookie is "unknown", never "unchanged", so reuse
    /// is refused.
    /// <para>
    /// This is the cross-silo case in miniature: a leaf activated on another
    /// silo publishes nothing into this process's registry, and the shard must
    /// treat that as no basis rather than as a matching one.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_settled_read_is_refused_when_the_leaf_no_longer_publishes_a_cookie()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(200), leafKey: "reuse-gone");
        PublishRevision(chain.LeafId, 5150);

        var stalled = await AttemptAsync(chain.Grain);
        Assert.That(stalled, Is.Null, "precondition: the first attempt stalls on the parked read");

        chain.ReleasePark();
        await Task.Yield();

        // The leaf deactivates; a deactivating leaf removes its registry entry.
        RegistryForTest().TryRemove(chain.LeafId, out _);
        Assert.That(BPlusLeafGrain.TryGetLeafRevision(chain.LeafId, out _), Is.False,
            "precondition: the cookie must genuinely be unreadable, or this arm passes for the "
            + "reason the arm above already covers");

        chain.Park = false;
        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "a leaf that no longer publishes a cookie supplies no basis for reuse, so the "
                + "read must be issued afresh");
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "and the fresh read must still serve a correct page");
        });
    }

    /// <summary>
    /// A faulted read is dropped on settle and is never retained for reuse,
    /// however stable the leaf's cookie is. Retaining it would convert one
    /// transient fault into a permanent one for every later identical walk -
    /// the same signature as the livelock this file exists to fix.
    /// <para>
    /// It is distinct from the existing faulted-read arm, which pins eviction
    /// with no cookie in play at all. Here the cookie is published and stable,
    /// so the reuse gate would serve the entry on the cookie alone were the
    /// success clause dropped.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_faulted_read_is_not_retained_for_reuse_even_when_the_cookie_is_stable()
    {
        var chain = CreateParkableLeaf(TimeSpan.FromSeconds(5), leafKey: "reuse-faulted");
        PublishRevision(chain.LeafId, 31337);

        var first = chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);
        chain.FaultPark();
        Assert.That(async () => await first, Throws.InstanceOf<InvalidOperationException>(),
            "precondition: the first attempt surfaces the leaf's fault");

        Assert.That(BPlusLeafGrain.TryGetLeafRevision(chain.LeafId, out var stable), Is.True,
            "precondition: the cookie is still published");
        Assert.That(stable, Is.EqualTo(31337),
            "precondition: and is unchanged, so only the success clause can refuse this entry");

        chain.Park = false;
        var second = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(chain.Reads, Has.Count.EqualTo(2),
                "the faulted entry must not be reusable; serving it would hand the same "
                + "exception to every later caller asking the same question");
            Assert.That(second.Entries.Select(e => e.Key), Is.EqualTo(chain.Rows.Select(r => r.Key)),
                "and the fresh read must succeed");
        });
    }

    /// <summary>
    /// The success clause in the reuse gate, reached directly.
    /// <para>
    /// <b>Why this arm exists at all.</b> Perturbing that clause away reddens
    /// nothing else in this fixture, because <c>SettleScanPageLeafRead</c>
    /// removes a faulted entry before any later walk can reach it. That makes
    /// the clause look dead, and it is not: the settle continuation runs on
    /// <see cref="TaskScheduler.Default"/> and is documented in the production
    /// file as a housekeeping optimisation that nothing may depend on having
    /// run, so a faulted entry still sitting in the map when a walk arrives is
    /// a state the production code itself declares reachable. It is only
    /// unreachable <em>on a timer</em>, which is a statement about scheduling
    /// and not about the component.
    /// </para>
    /// <para>
    /// So the state is reached at the map's own surface rather than waited for.
    /// The entry is established naturally, proven reusable, and only then
    /// faulted in place - which is exactly the race with the timing removed.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_settled_entry_that_faulted_before_the_sweep_reached_it_is_still_refused()
    {
        var chain = CreateParkableLeaf(
            TimeSpan.FromMilliseconds(200), leafKey: "reuse-faulted-inplace");
        PublishRevision(chain.LeafId, 4242);
        chain.Park = false;

        _ = await AttemptAsync(chain.Grain);
        Assert.That(chain.Reads, Has.Count.EqualTo(1),
            "precondition: one read was issued and has settled");

        // The control that makes the refusal below attributable. Without it the
        // arm cannot tell "refused because it carries a fault" from "was never
        // reusable in the first place", and would pass unchanged against a
        // build where reuse does not happen at all.
        _ = await AttemptAsync(chain.Grain);
        Assert.That(chain.Reads, Has.Count.EqualTo(1),
            "control: this entry IS served while it holds a success, so a refusal after the "
            + "plant below can only be attributable to the fault");

        FaultRetainedScanPageLeafRead(chain.Grain);

        _ = await AttemptAsync(chain.Grain);

        Assert.That(chain.Reads, Has.Count.EqualTo(2),
            "a retained entry carrying a fault must be refused and re-read even though its "
            + "cookie is unchanged; serving it would hand the same exception to every later "
            + "walk asking the same question, turning one transient fault into a permanent one");
    }

    /// <summary>
    /// Replaces the single retained entry's task with a faulted task of the
    /// <em>same closed generic type</em>, so the shard's cast on the reuse path
    /// behaves exactly as it would for a genuinely faulted read. Planting a
    /// bare <see cref="Task"/> would surface as a cast failure instead, which
    /// would redden the arm for a reason the arm is not about - a red for the
    /// wrong cause is no better evidence than a green for the wrong cause.
    /// <para>
    /// Every shape assumption throws with a named reason rather than returning
    /// null, so a rename cannot quietly turn this into a no-op and leave the
    /// success clause unmeasured while the arm still passes.
    /// </para>
    /// </summary>
    private static void FaultRetainedScanPageLeafRead(ShardRootGrain grain)
    {
        var field = typeof(ShardRootGrain).GetField(
            "_scanPageLeafReads",
            BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException(
                "_scanPageLeafReads field not found on ShardRootGrain - the field has been "
                + "renamed; update this helper, and treat the reuse gate's success clause as "
                + "unmeasured until it passes again.");

        var map = (System.Collections.IDictionary)(field.GetValue(grain)
            ?? throw new InvalidOperationException("_scanPageLeafReads returned null"));

        if (map.Count != 1)
        {
            throw new InvalidOperationException(
                $"expected exactly one retained entry to fault, found {map.Count}; this arm "
                + "cannot know which entry it would be planting into.");
        }

        object? entry = null;
        foreach (System.Collections.DictionaryEntry pair in map)
        {
            entry = pair.Value;
        }

        var read = entry?.GetType().GetProperty(
            "Read",
            BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException(
                "ScanPageLeafReadEntry.Read not found - update this helper.");

        var existing = (Task)read.GetValue(entry)!;

        var closed = existing.GetType();
        while (closed is not null
               && !(closed.IsGenericType && closed.GetGenericTypeDefinition() == typeof(Task<>)))
        {
            closed = closed.BaseType;
        }

        var resultType = closed is null
            ? throw new InvalidOperationException(
                $"the retained read is not a Task<T> (got {existing.GetType().FullName}); the "
                + "reuse path casts to Task<TList>, so planting a mismatched task would redden "
                + "this arm on a cast rather than on the clause it is written to pin.")
            : closed.GenericTypeArguments[0];

        var faulted = (Task)typeof(Task)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(Task.FromException)
                         && m.IsGenericMethodDefinition
                         && m.GetParameters().Length == 1)
            .MakeGenericMethod(resultType)
            .Invoke(null, [new InvalidOperationException("planted fault")])!;

        // Observe it here so the planted fault can never surface later as an
        // unobserved task exception attributed to an unrelated fixture.
        _ = faulted.Exception;

        var backing = entry!.GetType().GetField(
            "<Read>k__BackingField",
            BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException(
                "ScanPageLeafReadEntry.Read has no compiler backing field - if it is no longer "
                + "an auto-property this helper must be updated, and the success clause is "
                + "unmeasured until it is.");

        backing.SetValue(entry, faulted);
    }

    /// <summary>
    /// Retained settled entries are bounded. Entries no longer drain on
    /// completion, so without a cap a walk sweeping a wide keyspace would
    /// retain one entry per distinct page for the life of the activation -
    /// trading the unbounded queue this file fixed for an unbounded map.
    /// <para>
    /// The assertion is on the map's size, read reflectively, because there is
    /// no observable behaviour that distinguishes a bounded map from an
    /// unbounded one until the process runs out of memory. The cap is checked
    /// with a margin of one in-flight entry rather than for exact equality: the
    /// bound is a ceiling, and asserting an exact population would pin an
    /// eviction ordering the implementation deliberately leaves unspecified.
    /// </para>
    /// </summary>
    [Test]
    public async Task Retained_settled_reads_are_capped_so_the_map_cannot_grow_without_bound()
    {
        var chain = CreateParkableLeaf(
            TimeSpan.FromMilliseconds(200), rows: 2, leafKey: "reuse-capped");
        PublishRevision(chain.LeafId, 6001);
        chain.Park = false;

        // Each distinct lower bound is its own coalescing key, so each settles
        // its own retained entry.
        for (var i = 0; i < 200; i++)
        {
            _ = await AttemptFromAsync(chain.Grain, $"b{i:D4}");
        }

        var map = ReadScanPageLeafReadMapCount(chain.Grain);

        // Two-sided deliberately. An upper bound alone is satisfied by a map
        // that retains NOTHING, which is the pre-#2786 behaviour and the exact
        // regression the reuse path can suffer - so a one-sided assertion here
        // would report a working cap while measuring an absent feature. The
        // lower bound is the vacuity control: it fails if retention has stopped
        // happening, which is a different defect from the cap not holding, and
        // the message says which.
        Assert.That(map, Is.GreaterThan(1),
            $"retention itself must be happening before a cap means anything; {map} entries "
            + "means settled reads are not being retained at all, so this arm would report a "
            + "healthy cap while measuring nothing");

        Assert.That(map, Is.LessThanOrEqualTo(65),
            $"the retained-settled population must stay at its cap; {map} entries means the "
            + "sweep is not evicting, and the map grows with the keyspace a walk touches");
    }

    /// <summary>
    /// Reflective read of the per-activation coalescing map's size. Throws on
    /// any shape change so the cap arm cannot silently stop measuring the thing
    /// it names.
    /// </summary>
    private static int ReadScanPageLeafReadMapCount(ShardRootGrain grain)
    {
        var field = typeof(ShardRootGrain).GetField(
            "_scanPageLeafReads",
            BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException(
                "_scanPageLeafReads field not found on ShardRootGrain - the field's name has "
                + "changed; update this helper, and treat the cap as unmeasured until it passes.");

        var value = field.GetValue(grain)
            ?? throw new InvalidOperationException("_scanPageLeafReads returned null");

        return value is System.Collections.ICollection collection
            ? collection.Count
            : throw new InvalidOperationException(
                $"_scanPageLeafReads is not a collection (got {value.GetType().FullName}).");
    }
}
