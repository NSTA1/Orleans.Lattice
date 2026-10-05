using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Round-trip accounting for the tag index's orphan reconcile path.
/// </summary>
/// <remarks>
/// <para>
/// Reconcile re-verifies every candidate orphan row against the subject tree
/// before deleting it. Two properties of that verification are asserted here
/// and are invisible to a benchmark. The first is a <em>count</em>: candidates
/// are folded to their distinct keys and confirmed a window at a time, so a key
/// carrying T tags costs one share of one call rather than T separate reads.
/// The second is a <em>peak</em>: the confirmed rows are deleted in an
/// overlapped wave, which a count cannot distinguish from the serial loop it
/// replaced because both issue exactly the same number of deletes.
/// </para>
/// <para>
/// The confirmation also reads through the gate-accounting overload, because
/// this is a path that concludes from a key's <em>absence</em>. That is a
/// correctness property rather than a performance one, and it is pinned here
/// alongside the counts so the batching cannot later be re-implemented on the
/// plain overload without a test going red.
/// </para>
/// </remarks>
public partial class LatticeTagIndexRoundTripBatchingTests
{
    private const string SubjectTreeId = "orders";

    /// <summary>
    /// Builds a coordinator-scoped index over one counting index tree and a
    /// separate counting subject tree, so the reads the reconcile issues
    /// against the subject are counted apart from the index's own row traffic.
    /// </summary>
    private static (CountingTree index, CountingTree subject, LatticeTagIndexContext ctx) CreateWithSubject()
    {
        var indexTree = new CountingTree();
        var subjectTree = new CountingTree();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>())
            .Returns(ci => ci.ArgAt<string>(0) == SubjectTreeId ? subjectTree.Lattice : indexTree.Lattice);
        return (indexTree, subjectTree, LatticeTagIndexContext.CreateForCoordinator(grainFactory, IndexName));
    }

    /// <summary>
    /// Seeds <paramref name="keyCount"/> keys each carrying <paramref name="tagCount"/>
    /// tags into the index, then empties the subject tree so every membership
    /// row is an orphan candidate.
    /// </summary>
    private static async Task SeedOrphansAsync(
        LatticeTagIndexContext ctx,
        CountingTree subject,
        int keyCount,
        int tagCount)
    {
        var tags = new string[tagCount];
        for (var t = 0; t < tagCount; t++)
        {
            tags[t] = $"tag{t:D2}";
        }

        for (var k = 0; k < keyCount; k++)
        {
            var key = $"k{k:D3}";
            subject.Data[key] = [1];
            await ctx.AddTagsForKeyAsync(SubjectTreeId, key, tags, CancellationToken.None);
        }

        // The keys are gone from the subject tree but their membership rows
        // remain, which is exactly the state reconcile exists to repair.
        subject.Data.Clear();
        subject.ExistsAsyncCalls = 0;
        subject.GateAccountedCalls = 0;
        subject.GateAccountedWidths.Clear();
    }

    // ── Confirmation: one call per window of distinct keys, not per row ──

    [Test]
    public async Task Reconcile_confirms_candidate_keys_in_windows_rather_than_per_row()
    {
        var (index, subject, ctx) = CreateWithSubject();
        const int keys = 70;
        const int tags = 4;
        await SeedOrphansAsync(ctx, subject, keys, tags);

        var report = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        var window = LatticeTagIndexContext.OrphanVerifyWindow;
        var expectedCalls = (keys + window - 1) / window;

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(keys * tags), "every membership row is an orphan");

            // Per candidate row this cost 70 x 4 = 280 ExistsAsync calls: one
            // per row, four of them asking the identical question per key.
            Assert.That(subject.GateAccountedCalls, Is.EqualTo(expectedCalls));
            Assert.That(subject.ExistsAsyncCalls, Is.Zero, "the per-row probe must be gone entirely");

            // De-duplicated to 70 distinct keys, so the rows read fall with the
            // call count rather than merely being re-packaged.
            Assert.That(subject.GateAccountedWidths.Sum(), Is.EqualTo(keys));
            Assert.That(subject.GateAccountedWidths[0], Is.EqualTo(window));
        });

        Assert.That(index.Data, Has.No.Member($"\0tag00\0{SubjectTreeId}\0k000"));
    }

    [Test]
    public async Task Reconcile_confirmation_cost_does_not_grow_with_the_tag_count()
    {
        var (_, subjectNarrow, narrow) = CreateWithSubject();
        await SeedOrphansAsync(narrow, subjectNarrow, keyCount: 8, tagCount: 1);
        await narrow.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        var (_, subjectWide, wide) = CreateWithSubject();
        await SeedOrphansAsync(wide, subjectWide, keyCount: 8, tagCount: 20);
        await wide.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            // Eight distinct keys either way, so both fit one window and cost
            // one confirmation call. Per row the wide case cost 160.
            Assert.That(subjectNarrow.GateAccountedCalls, Is.EqualTo(1));
            Assert.That(subjectWide.GateAccountedCalls, Is.EqualTo(1));
            Assert.That(subjectWide.GateAccountedWidths, Is.EqualTo(new[] { 8 }));
        });
    }

    [Test]
    public async Task Reconcile_handles_a_partial_final_window()
    {
        var (_, subject, ctx) = CreateWithSubject();
        const int keys = 5;
        await SeedOrphansAsync(ctx, subject, keys, tagCount: 2);

        var report = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(subject.GateAccountedCalls, Is.EqualTo(1));
            Assert.That(subject.GateAccountedWidths, Is.EqualTo(new[] { keys }));
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(keys * 2));
        });
    }

    [Test]
    public async Task Reconcile_spares_a_key_that_reappeared_after_the_live_snapshot()
    {
        var (index, subject, ctx) = CreateWithSubject();
        await SeedOrphansAsync(ctx, subject, keyCount: 3, tagCount: 2);

        // k001 is written back between the live scan and the confirmation: it is
        // in the subject tree but withheld from the key scan. The confirmation
        // is the only thing standing between it and having its freshly
        // committed rows destroyed as false orphans.
        subject.Data["k001"] = [1];
        subject.HiddenFromScan.Add("k001");

        var report = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(4), "only k000 and k002 are genuine orphans");
            Assert.That(index.Data.Keys, Has.Some.Contains("k001"), "the reappeared key keeps its rows");
            Assert.That(index.Data.Keys, Has.None.Contains("k000"));
        });
    }

    // ── Gate correctness: absence under a gate is not an orphan ──

    [Test]
    public async Task Reconcile_deletes_nothing_when_the_access_gate_pruned_the_window()
    {
        var (index, subject, ctx) = CreateWithSubject();
        await SeedOrphansAsync(ctx, subject, keyCount: 4, tagCount: 3);
        var rowsBefore = index.Data.Count;

        // The gate hid part of the window, so an absence in the result is
        // uninterpretable: it may be the gate rather than a missing key.
        subject.PrunedByAccessGate = 1;

        var report = await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.Zero, "a gated window must not be classified");
            Assert.That(index.Data, Has.Count.EqualTo(rowsBefore), "no membership row may be deleted");
            Assert.That(index.DeleteAsyncCalls, Is.Zero);
        });
    }

    // ── Deletion: the confirmed rows are removed concurrently ──

    [Test]
    public async Task Reconcile_issues_its_orphan_row_deletions_concurrently()
    {
        var (index, subject, ctx) = CreateWithSubject();
        const int keys = 4;
        const int tags = 2;
        await SeedOrphansAsync(ctx, subject, keys, tags);

        // Two rows per orphan candidate, and every candidate is confirmed. The
        // gate does not release until all sixteen deletes are simultaneously in
        // flight, so a serial implementation cannot finish: its first delete
        // would wait forever on a sixteenth that is never issued. Completing at
        // all is the proof of overlap, and the peak assertion states it
        // explicitly.
        const int expected = keys * tags * 2;
        var arrived = 0;
        var peak = 0;
        var inFlight = 0;
        var allInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        index.DeleteGate = () =>
        {
            var now = Interlocked.Increment(ref inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) &&
                   Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            if (Interlocked.Increment(ref arrived) == expected)
            {
                allInFlight.TrySetResult();
            }

            return WaitThenLeave();
        };

        async Task WaitThenLeave()
        {
            await allInFlight.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref inFlight);
        }

        var reconcile = ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);
        var finished = await Task.WhenAny(reconcile, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.That(finished, Is.SameAs(reconcile), "the orphan deletions did not overlap; they were issued sequentially");
        var report = await reconcile;

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(keys * tags));
            Assert.That(index.DeleteAsyncCalls, Is.EqualTo(expected));
            // Equality, not a ceiling: a ceiling passes under the serial
            // implementation too, which peaks at one.
            Assert.That(Volatile.Read(ref peak), Is.EqualTo(expected),
                "peak simultaneous deletes must reach the whole confirmed set below the wave cap");
        });
    }

    [Test]
    public async Task Reconcile_caps_its_deletion_wave_at_the_row_concurrency_limit()
    {
        var (index, subject, ctx) = CreateWithSubject();
        // 40 keys x 1 tag is 80 row deletions, comfortably past the cap of 32.
        await SeedOrphansAsync(ctx, subject, keyCount: 40, tagCount: 1);

        // The gate releases the moment the cap's worth of deletes is in flight,
        // so the wave can drain. Peak is asserted for equality with the cap: an
        // unthrottled wave would reach 80 and a serial loop would reach 1, and
        // only equality rejects both.
        const int cap = LatticeTagIndexContext.RemoveRowConcurrencyLimit;
        var arrived = 0;
        var peak = 0;
        var inFlight = 0;
        var capReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        index.DeleteGate = () =>
        {
            var now = Interlocked.Increment(ref inFlight);
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) &&
                   Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }

            if (Interlocked.Increment(ref arrived) == cap)
            {
                capReached.TrySetResult();
            }

            return WaitThenLeave();
        };

        async Task WaitThenLeave()
        {
            await capReached.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref inFlight);
        }

        var reconcile = ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);
        var finished = await Task.WhenAny(reconcile, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.That(finished, Is.SameAs(reconcile), "the orphan deletions did not overlap");
        var report = await reconcile;

        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.EqualTo(40));
            Assert.That(index.DeleteAsyncCalls, Is.EqualTo(80));
            Assert.That(Volatile.Read(ref peak), Is.EqualTo(cap),
                "the wave must fill to the cap and no further");
        });
    }

    [Test]
    public async Task Reconcile_deletes_exactly_the_rows_the_serial_path_deleted()
    {
        var (index, subject, ctx) = CreateWithSubject();
        await SeedOrphansAsync(ctx, subject, keyCount: 3, tagCount: 2);

        await ctx.ReconcileSubjectAsync(SubjectTreeId, null, null, CancellationToken.None);

        // Six tag-major rows and six key-major mirrors are gone; the covered
        // tree marker is not a membership row and survives.
        Assert.That(
            index.Data.Keys.Where(k =>
                !k.StartsWith("\0covered\0", StringComparison.Ordinal)
                && k.Contains(SubjectTreeId, StringComparison.Ordinal)),
            Is.Empty);
    }
}
