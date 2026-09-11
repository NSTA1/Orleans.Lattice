using Microsoft.Extensions.DependencyInjection;
using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The work half of the count walk's bound (#2447).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RepoContextVectorSource.CountAsync"/> walks a repository's entire
/// vector prefix. #1844 made that walk survive a reclaimed enumerator by
/// reconnecting up to sixty-four times - which bounds RETRIES, not WORK. The two
/// are not the same guarantee, and the gap between them is exactly the case that
/// hurt: a walk that never aborts is never retried, so it was bounded by nothing
/// at all and ran until it reached the end of the prefix, on a path that holds the
/// index build's turn while every other caller of that grain waits.
/// </para>
/// <para>
/// THE INTERESTING PART IS NOT THE BOUND, IT IS WHAT THE BOUND REPORTS. The
/// obvious implementation returns however many keys it managed to walk, and it is
/// wrong in a way that is invisible: the only consumer that reads the figure for a
/// decision compares it against the index's own count to decide whether the index
/// is BEHIND the store of record. A truncated walk always under-counts, an
/// under-count always reads as "not behind", and so the cheap fix would buy a
/// bounded walk at the price of an index that silently stops repairing itself.
/// These fixtures pin the fault as the contract, not the number.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextVectorSourceCountBudgetTests
{
    private const string RepoId = "acme";
    private static readonly string Prefix = RepoContextKeys.VectorsPrefix(RepoId);

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 4, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public void A_walk_that_outruns_its_budget_reports_no_count_at_all()
    {
        // Ten keys at two seconds each against a five second budget: the walk gets
        // three keys in and is out of time with seven still unread.
        var tree = TreeYielding(Keys(10));
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromSeconds(5),
            timeProvider: new SteppingTimeProvider(TimeSpan.FromSeconds(2)));

        var thrown = Assert.ThrowsAsync<RepoContextCountBudgetExceededException>(
            async () => await source.CountAsync(Ct));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.Counted, Is.EqualTo(3),
                "the partial figure is reported for diagnosis only, and reporting it here is what proves "
                + "the walk really did stop short rather than never start");
            Assert.That(thrown.Counted, Is.LessThan(10),
                "a walk that reached the end of the prefix would prove nothing about the bound");
            Assert.That(thrown.RepoId, Is.EqualTo(RepoId));
            Assert.That(thrown.Budget, Is.EqualTo(TimeSpan.FromSeconds(5)));
        });
    }

    [Test]
    public void The_partial_figure_is_never_returned_as_if_it_were_the_count()
    {
        // The whole design decision in one assertion. Returning three here would
        // compile, pass a naive test, and disable the shortfall repair on every
        // large repository forever, because three is less than the index's count
        // and "less than" is read as "not behind".
        var tree = TreeYielding(Keys(10));
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromSeconds(5),
            timeProvider: new SteppingTimeProvider(TimeSpan.FromSeconds(2)));

        Assert.That(
            async () => await source.CountAsync(Ct),
            Throws.TypeOf<RepoContextCountBudgetExceededException>(),
            "an under-count must be raised as a fault, because a caller cannot tell a truncated count "
            + "from a small repository");
    }

    [Test]
    public async Task A_prefix_walked_inside_the_budget_still_counts_exactly()
    {
        // The bound must be invisible in the case that matters most - the ordinary
        // one. A budget that fired early would degrade every build's reservation.
        var tree = TreeYielding(Keys(10));
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromSeconds(60),
            timeProvider: new SteppingTimeProvider(TimeSpan.FromSeconds(2)));

        Assert.That(await source.CountAsync(Ct), Is.EqualTo(10));
    }

    [Test]
    public void The_sampled_check_is_charged_after_a_key_so_it_cannot_consume_a_walk_without_advancing_it()
    {
        // A bound checked BEFORE the work can consume nothing: with a budget
        // smaller than a single key's cost, a pre-check walk yields zero every time
        // and the build never advances - a livelock dressed as a safety measure.
        // One key of progress is the minimum that keeps THIS check safe.
        //
        // READ THE SCOPE. The guarantee belongs to the sampled check, not to the
        // walk, and this fixture is deliberately built so that only the sampled
        // check can fire: the budget is thirty real seconds, which the millisecond
        // -scale test can never spend, while the FAKE clock steps thirty seconds per
        // reading so the in-loop comparison trips immediately after the first key.
        //
        // It previously used a one-TICK budget, which made it depend on losing a
        // race against a real timer, because SteppingTimeProvider does not override
        // CreateTimer and the deadline therefore runs on the system clock. Under
        // load that race was lost every time: thirty consecutive runs at full CPU
        // failed with "Expected: 1, But was: 0". Worse than the flake, the passing
        // version had been read as proof of a walk-wide minimum-progress property it
        // never tested - it passed only because the fake source answers
        // synchronously, so the deadline never armed at all. The paired fixture
        // below drives the case this one cannot.
        var tree = TreeYielding(Keys(4));
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromSeconds(30),
            timeProvider: new SteppingTimeProvider(TimeSpan.FromSeconds(30)));

        var thrown = Assert.ThrowsAsync<RepoContextCountBudgetExceededException>(
            async () => await source.CountAsync(Ct));

        Assert.That(thrown!.Counted, Is.EqualTo(1));
    }

    [Test]
    public void A_first_page_slower_than_the_budget_counts_nothing_and_zero_still_means_something()
    {
        // THE DETECTOR THE FIXTURE ABOVE CANNOT BE. Every other fixture in this file
        // drives a source whose first read completes synchronously, so the deadline
        // never arms and the only bound ever exercised is the sampled one. That left
        // the walk-wide bound - the half that actually fires in production, where
        // every read is a grain call and none completes synchronously - asserted by
        // nothing. A detector never observed to fire is indistinguishable from a
        // detector that cannot fire, so this drives it on one real clock.
        //
        // The assertion is the CONTRACT of a zero, not merely the number: a walk
        // whose first page outruns the budget reports Counted = 0, and that zero is
        // a measured absence rather than a walk that never started, because it
        // arrives as a FAULT. The paired half of the claim is the second assertion:
        // an empty prefix RETURNS zero and never throws. The two zeroes are
        // therefore distinguishable by their channel, which is the only reason a
        // caller may act on either.
        // THE MARGIN IS LOAD-CHOSEN, NOT ARBITRARY. A first draft used a 400ms page
        // against a 250ms budget and failed 2 runs in 30 at full CPU, reporting
        // Counted = 1: a timer callback delayed past 400ms lets the page land first,
        // and the sampled check then charges one key. A 150ms margin is simply not
        // enough room on a loaded machine. The page is now three seconds against a
        // 200ms budget, so no plausible scheduling delay can reorder them. It costs
        // nothing in wall-clock: cancelling the walk token aborts the delay, so the
        // fixture completes in about 200ms and never waits the three seconds. If the
        // deadline were removed entirely the delay WOULD elapse, five keys would be
        // counted, and this fixture reddens - which is the direction that matters.
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => SlowFirstPageKeys(ci.ArgAt<CancellationToken>(4)));

        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromMilliseconds(200),
            timeProvider: TimeProvider.System);

        var thrown = Assert.ThrowsAsync<RepoContextCountBudgetExceededException>(
            async () => await source.CountAsync(Ct));

        var empty = Substitute.For<ILattice>();
        empty.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedKeys([], abortAfter: int.MaxValue));
        var emptySource = new RepoContextVectorSource(
            FactoryFor(empty), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromSeconds(30),
            timeProvider: TimeProvider.System);

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.Counted, Is.Zero,
                "the walk-wide deadline is under no minimum-progress constraint, so a first page "
                + "slower than the whole budget legitimately counts nothing");
            Assert.That(thrown.Budget, Is.EqualTo(TimeSpan.FromMilliseconds(200)));
            Assert.That(
                async () => await emptySource.CountAsync(Ct),
                Throws.Nothing,
                "an empty prefix must REPORT zero rather than fault, or the caller cannot tell an "
                + "empty repository from one whose first page was too slow to read");
        });
    }

    /// <summary>
    /// A source whose first page costs more than the entire budget and which then
    /// yields promptly: the production shape, where every read is a grain call and
    /// none of them completes synchronously.
    /// </summary>
    private static async IAsyncEnumerable<string> SlowFirstPageKeys(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken).ConfigureAwait(false);

        for (var i = 0; i < 5; i++)
        {
            yield return $"{Prefix}vec-{i:D4}";
        }
    }

    [Test]
    public async Task A_disabled_budget_restores_the_unbounded_walk()
    {
        var clock = new SteppingTimeProvider(TimeSpan.FromHours(1));
        var tree = TreeYielding(Keys(10));
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.Zero,
            timeProvider: clock);

        var count = await source.CountAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(10));
            Assert.That(clock.Readings, Is.Zero,
                "a disabled bound must not even read the clock, so the opt-out is total rather than "
                + "a very large budget");
        });
    }

    [Test]
    public void The_default_budget_is_the_one_a_caller_gets_without_asking()
    {
        // The production construction passes neither argument, so the default is
        // the value that actually ships. Pinning it here is what stops it being
        // quietly widened back towards unbounded.
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextVectorSource.DefaultCountBudget, Is.EqualTo(TimeSpan.FromSeconds(10)));
            Assert.That(RepoContextVectorSource.DefaultCountBudget, Is.GreaterThan(TimeSpan.Zero),
                "a non-positive default would disable the bound for every production caller");
        });
    }

    [Test]
    public void The_bound_survives_the_reconnects_it_was_layered_on_top_of()
    {
        // #1844's resilience and #2447's bound are independent guarantees over the
        // same loop, so the fixture that proves they compose is worth more than
        // either alone: keys yielded across an abort must still be charged to the
        // budget, or a walk could evade the bound simply by aborting often.
        var callIndex = 0;
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => callIndex++ == 0
                ? ScriptedKeys([Prefix + "vec-0000", Prefix + "vec-0001"], abortAfter: 2)
                : ScriptedKeys([Prefix + "vec-0002", Prefix + "vec-0003"], abortAfter: int.MaxValue));

        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromSeconds(5),
            timeProvider: new SteppingTimeProvider(TimeSpan.FromSeconds(2)));

        var thrown = Assert.ThrowsAsync<RepoContextCountBudgetExceededException>(
            async () => await source.CountAsync(Ct));

        Assert.That(thrown!.Counted, Is.EqualTo(3),
            "the two keys read before the abort and the first read after it are all charged to the "
            + "same budget");
    }

    [Test]
    public void A_walk_whose_source_yields_nothing_is_still_bounded()
    {
        // THE PAIRED NEGATIVE (#2536). Every other fixture here drives a source
        // that ANSWERS, and none of them can see the case that wedged the rig: the
        // in-loop budget check sits after the key is counted, so a walk that yields
        // no key never reaches it and is bounded by nothing whatsoever. The field
        // evidence is the expected value on the other side of this assertion -
        // across 53,733 log lines of a wedged build, with the corpus reported
        // uncounted throughout, this exception was constructed exactly ZERO times.
        // A bound that never fires and an absent bound are the same bound.
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(),
            Arg.Any<CancellationToken>())
            .Returns(ci => StalledKeys(ci.ArgAt<CancellationToken>(4)));

        // A real budget against the real clock: the deadline is a timer, so a clock
        // that only advances when it is read would leave it unarmed and this test
        // would hang against the fixed code as readily as against the broken code.
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromMilliseconds(500),
            timeProvider: TimeProvider.System);

        var thrown = Assert.ThrowsAsync<RepoContextCountBudgetExceededException>(
            async () => await source.CountAsync(Ct).WaitAsync(TimeSpan.FromSeconds(30), Ct),
            "a walk that never yields must still be ended by its budget; before this change it ran "
            + "until the caller's own timeout, holding the build's turn for the whole of it");

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.Counted, Is.Zero,
                "and it reports honestly that it counted nothing, so a caller cannot mistake the "
                + "truncation for a small repository");
            Assert.That(thrown.RepoId, Is.EqualTo(RepoId));
            Assert.That(thrown.Budget, Is.EqualTo(TimeSpan.FromMilliseconds(500)));
        });
    }

    private static string[] Keys(int count)
    {
        var keys = new string[count];
        for (var i = 0; i < count; i++)
        {
            keys[i] = $"{Prefix}vec-{i:D4}";
        }

        return keys;
    }

    private static ILattice TreeYielding(string[] keys)
    {
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => ScriptedKeys(keys, abortAfter: int.MaxValue));
        return tree;
    }

    private static IGrainFactory FactoryFor(ILattice tree)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string>()).Returns(tree);
        return factory;
    }

    /// <summary>
    /// A walk that never yields a key and never ends, which is what a scan of a
    /// contended shard root does when its leaf chain cannot be read: it is neither
    /// a fault the loop can catch nor an answer the loop can count.
    /// </summary>
    private static async IAsyncEnumerable<string> StalledKeys(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    private static async IAsyncEnumerable<string> ScriptedKeys(string[] keys, int abortAfter)    {
        var yielded = 0;
        foreach (var key in keys)
        {
            if (yielded >= abortAfter) throw new EnumerationAbortedException();
            yielded++;
            yield return key;
            await Task.Yield();
        }

        if (yielded < abortAfter) yield break;
        throw new EnumerationAbortedException();
    }
}
