using Microsoft.Extensions.DependencyInjection;
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
    public void The_budget_is_charged_after_a_key_is_counted_so_the_walk_always_advances()
    {
        // A bound checked BEFORE the work can consume nothing: with a budget
        // smaller than a single key's cost, a pre-check walk yields zero every time
        // and the build never advances - a livelock dressed as a safety measure.
        // One key of progress is the minimum that keeps the bound safe.
        var tree = TreeYielding(Keys(4));
        var source = new RepoContextVectorSource(
            FactoryFor(tree), Serializer, RepoId, Space,
            countBudget: TimeSpan.FromTicks(1),
            timeProvider: new SteppingTimeProvider(TimeSpan.FromSeconds(30)));

        var thrown = Assert.ThrowsAsync<RepoContextCountBudgetExceededException>(
            async () => await source.CountAsync(Ct));

        Assert.That(thrown!.Counted, Is.EqualTo(1));
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

    private static async IAsyncEnumerable<string> ScriptedKeys(string[] keys, int abortAfter)
    {
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
