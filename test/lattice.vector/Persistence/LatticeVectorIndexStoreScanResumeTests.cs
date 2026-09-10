namespace Orleans.Lattice.Vector.Tests.Persistence;

using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// The durable index reloads itself by walking corpus-sized key ranges, and a
/// walk that is abandoned by the Orleans response timeout used to discard every
/// record it had already delivered. The reload above it is all-or-nothing, so one
/// timed-out page restarted the whole O(corpus) open, which banks nothing and is
/// retried seconds later: a livelock in which elapsed time buys no progress
/// (#2539).
/// <para>
/// What is asserted here is not merely that the walk survives. It is that the
/// walk <i>resumes</i> - re-entering at the successor of the last record it
/// delivered rather than at the original prefix - because a walk that survived by
/// restarting would still re-read the corpus and would still not converge.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeVectorIndexStoreScanResumeTests
{
    private const string Prefix = "idx/key/";
    private const int Corpus = 50;
    private const int DeliveredBeforeTimeout = 20;

    private static string[] Keys() =>
        [.. Enumerable.Range(0, Corpus).Select(i => $"{Prefix}{i:D4}")];

    private static async Task<List<string>> DrainAsync(LatticeVectorIndexStore store)
    {
        var seen = new List<string>();
        await foreach (var entry in store.ScanAsync(Prefix))
        {
            seen.Add(entry.Key);
        }

        return seen;
    }

    [Test]
    public async Task A_walk_abandoned_by_the_response_timeout_still_delivers_every_record()
    {
        var keys = Keys();
        var tree = TimeoutingLatticeTree.Create(keys, DeliveredBeforeTimeout, timeouts: 1);
        var store = new LatticeVectorIndexStore(tree.Tree);

        var seen = await DrainAsync(store);

        // Ordinal order, every key once. Compared against the whole expected
        // sequence rather than a count, so a resume that skipped a record and
        // duplicated another could not pass.
        Assert.That(seen, Is.EqualTo(keys).AsCollection);
    }

    [Test]
    public async Task An_abandoned_walk_resumes_after_the_last_record_it_delivered_rather_than_restarting()
    {
        var keys = Keys();
        var tree = TimeoutingLatticeTree.Create(keys, DeliveredBeforeTimeout, timeouts: 1);
        var store = new LatticeVectorIndexStore(tree.Tree);

        await DrainAsync(store);

        Assert.Multiple(() =>
        {
            Assert.That(tree.StartBounds, Has.Count.EqualTo(2), "the walk should have been re-entered exactly once");
            Assert.That(tree.StartBounds[0], Is.EqualTo(Prefix), "the first walk starts at the prefix");

            // The successor of the last key delivered. This is the whole
            // distinction the fix turns on: restarting would re-enter at Prefix.
            Assert.That(
                tree.StartBounds[1],
                Is.EqualTo(keys[DeliveredBeforeTimeout - 1] + "\u0000"),
                "the resumed walk must re-enter after the last record already delivered");
        });
    }

    [Test]
    public async Task A_resumed_walk_re_reads_nothing_it_had_already_delivered()
    {
        var keys = Keys();
        var tree = TimeoutingLatticeTree.Create(keys, DeliveredBeforeTimeout, timeouts: 1);
        var store = new LatticeVectorIndexStore(tree.Tree);

        await DrainAsync(store);

        // The cost assertion, and the reason the livelock lifts: the tree handed
        // out exactly the corpus across both walks. A restart would have made it
        // hand out the first records twice.
        Assert.That(tree.RecordsDelivered, Is.EqualTo(Corpus));
    }

    [Test]
    public async Task A_walk_that_stalls_far_more_often_than_its_budget_still_converges_while_it_makes_progress()
    {
        var keys = Keys();

        // Ten abandonments against a budget of two. This is the regression that
        // matters most: the field measured reload scans absorbing dozens of
        // stalls apiece and still dying, because a budget consumed early in a
        // corpus-sized walk was never given back after the walk banked progress.
        var tree = TimeoutingLatticeTree.Create(keys, entriesBeforeTimeout: 1, timeouts: 10);
        Assert.That(
            10, Is.GreaterThan(LatticeVectorIndexStore.DefaultScanTimeoutResumeAttempts),
            "the fixture must abandon the walk more often than the budget allows, or it proves nothing");

        var store = new LatticeVectorIndexStore(tree.Tree);

        var seen = await DrainAsync(store);

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.EqualTo(keys).AsCollection);
            Assert.That(tree.RecordsDelivered, Is.EqualTo(Corpus), "progress must not be re-read");
        });
    }

    [Test]
    public void A_walk_that_makes_no_progress_at_all_still_reports_the_timeout()
    {
        var keys = Keys();

        // Abandoned before delivering anything, every time. Nothing replenishes
        // the budget, so the walk must fail rather than spin: the bound is on
        // CONSECUTIVE futile attempts, and this is what makes it a bound at all.
        var tree = TimeoutingLatticeTree.Create(
            keys,
            entriesBeforeTimeout: 0,
            timeouts: LatticeVectorIndexStore.DefaultScanTimeoutResumeAttempts + 1);
        var store = new LatticeVectorIndexStore(tree.Tree);

        Assert.That(async () => await DrainAsync(store), Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public void A_walk_that_progresses_then_wedges_reports_the_timeout_rather_than_spinning()
    {
        var keys = Keys();

        // Progress replenishes the budget, so the guarantee to prove is that a
        // replenished budget is still a budget: once the tree stops delivering,
        // the walk terminates on consecutive futile attempts.
        var tree = TimeoutingLatticeTree.Create(
            keys,
            entriesBeforeTimeout: 0,
            timeouts: LatticeVectorIndexStore.DefaultScanTimeoutResumeAttempts + 1,
            deliverBeforeWedging: 5);
        var store = new LatticeVectorIndexStore(tree.Tree);

        Assert.That(async () => await DrainAsync(store), Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public void A_timeout_observed_after_cancellation_is_not_resumed()
    {
        var keys = Keys();
        var tree = TimeoutingLatticeTree.Create(keys, DeliveredBeforeTimeout, timeouts: 1);
        var store = new LatticeVectorIndexStore(tree.Tree);
        using var cancellation = new CancellationTokenSource();

        // A resume budget is for making progress, not for spending during
        // shutdown.
        Assert.That(
            async () =>
            {
                await foreach (var entry in store.ScanAsync(Prefix, cancellation.Token))
                {
                    if (entry.Key == keys[0])
                    {
                        await cancellation.CancelAsync();
                    }
                }
            },
            Throws.InstanceOf<Exception>());

        Assert.That(tree.StartBounds, Has.Count.EqualTo(1), "no resume should have been attempted");
    }
}
