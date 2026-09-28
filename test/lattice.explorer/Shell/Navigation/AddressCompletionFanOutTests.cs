using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Explorer.Shell.Navigation.Completion;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// The completion fan-out: every source asked at once, each answer yielded as it
/// arrives, a slow source cut off at its own timeout, a failing one reported,
/// and neither ever holding back the rest.
/// </summary>
[TestFixture]
public sealed class AddressCompletionFanOutTests
{
    private static readonly AddressQuery Query = new("ord", AddressQueryMode.Search, ExplorerAddress.Home);

    private readonly ManualTimeProvider _time = new();

    [Test]
    public async Task Every_source_is_asked_in_parallel_with_the_same_query()
    {
        var first = FakeCompletionSource.Answering(Completion("orders"));
        var second = FakeCompletionSource.Answering(Completion("ordinals"));

        var batches = await CollectAsync(Entry("data", first), Entry("apps", second));

        Assert.Multiple(() =>
        {
            Assert.That(batches.Select(batch => batch.Source.Key), Is.EquivalentTo(new[] { "data", "apps" }));
            Assert.That(batches.All(batch => batch.Outcome == AddressCompletionOutcome.Completed), Is.True);
            Assert.That(first.Queries.Single(), Is.SameAs(Query));
            Assert.That(second.Queries.Single(), Is.SameAs(Query));
        });
    }

    [Test]
    public async Task Answers_are_yielded_in_the_order_they_arrive()
    {
        var slow = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var fanOut = new AddressCompletionFanOut(new ExplorerChromeOptions(), _time);

        await using var batches = fanOut
            .CompleteAsync(Query, [Entry("data", FakeCompletionSource.Gated(slow)), Entry("apps", FakeCompletionSource.Answering(Completion("a")))])
            .GetAsyncEnumerator();

        Assert.That(await batches.MoveNextAsync(), Is.True);
        Assert.That(batches.Current.Source.Key, Is.EqualTo("apps"), "the answer that is ready arrives first");

        slow.SetResult([Completion("orders")]);
        Assert.That(await batches.MoveNextAsync(), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(batches.Current.Source.Key, Is.EqualTo("data"));
            Assert.That(batches.Current.Completions.Single().Label, Is.EqualTo("orders"));
        });
        Assert.That(await batches.MoveNextAsync(), Is.False);
    }

    [Test]
    public async Task A_source_that_does_not_answer_in_time_is_cut_off_and_cancelled()
    {
        var never = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var slow = FakeCompletionSource.Gated(never);
        var fanOut = new AddressCompletionFanOut(new ExplorerChromeOptions(), _time);

        await using var batches = fanOut
            .CompleteAsync(Query, [Entry("slow", slow), Entry("data", FakeCompletionSource.Answering(Completion("orders")))])
            .GetAsyncEnumerator();

        Assert.That(await batches.MoveNextAsync(), Is.True);
        Assert.That(batches.Current.Source.Key, Is.EqualTo("data"), "the slow source does not hold the others back");

        var next = batches.MoveNextAsync();
        Assert.That(next.IsCompleted, Is.False, "nothing has timed out yet");

        _time.Advance(new ExplorerChromeOptions().CompletionTimeout);

        Assert.That(await next, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(batches.Current.Source.Key, Is.EqualTo("slow"));
            Assert.That(batches.Current.Outcome, Is.EqualTo(AddressCompletionOutcome.TimedOut));
            Assert.That(batches.Current.Completions, Is.Empty);
            Assert.That(slow.Tokens.Single().IsCancellationRequested, Is.True);
        });
    }

    [Test]
    public async Task A_source_that_throws_is_reported_and_the_others_still_answer()
    {
        var batches = await CollectAsync(
            Entry("broken", FakeCompletionSource.Throwing()),
            Entry("data", FakeCompletionSource.Answering(Completion("orders"))));

        Assert.Multiple(() =>
        {
            Assert.That(batches.Single(batch => batch.Source.Key == "broken").Outcome, Is.EqualTo(AddressCompletionOutcome.Failed));
            Assert.That(batches.Single(batch => batch.Source.Key == "broken").Completions, Is.Empty);
            Assert.That(batches.Single(batch => batch.Source.Key == "data").Completions.Single().Label, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task A_source_that_faults_asynchronously_is_reported_as_failed()
    {
        var faulting = new FakeCompletionSource((_, _) =>
            ValueTask.FromException<IReadOnlyList<AddressCompletion>>(new InvalidOperationException()));

        var batches = await CollectAsync(Entry("broken", faulting));

        Assert.That(batches.Single().Outcome, Is.EqualTo(AddressCompletionOutcome.Failed));
    }

    [Test]
    public async Task A_source_answering_more_than_the_limit_is_cut_to_it_and_nulls_are_dropped()
    {
        var many = Enumerable.Range(0, 30).Select(i => Completion("key-" + i)).ToList<AddressCompletion>();
        many.Insert(3, null!);

        var batches = await CollectAsync(Entry("data", new FakeCompletionSource((_, _) =>
            ValueTask.FromResult<IReadOnlyList<AddressCompletion>>(many))));

        Assert.Multiple(() =>
        {
            Assert.That(batches.Single().Completions, Has.Count.EqualTo(AddressQuery.MaximumResults));
            Assert.That(batches.Single().Completions.All(completion => completion is not null), Is.True);
            Assert.That(Query.Limit, Is.EqualTo(20));
        });
    }

    [Test]
    public async Task A_null_answer_is_an_empty_batch()
    {
        var batches = await CollectAsync(Entry("data", new FakeCompletionSource((_, _) =>
            ValueTask.FromResult<IReadOnlyList<AddressCompletion>>(null!))));

        Assert.That(batches.Single().Completions, Is.Empty);
    }

    [Test]
    public async Task No_sources_yield_nothing()
    {
        Assert.That(await CollectAsync(), Is.Empty);
    }

    [Test]
    public async Task Cancelling_the_request_cancels_every_source()
    {
        var never = new TaskCompletionSource<IReadOnlyList<AddressCompletion>>();
        var slow = FakeCompletionSource.Gated(never);
        var fanOut = new AddressCompletionFanOut(new ExplorerChromeOptions(), _time);
        using var request = new CancellationTokenSource();

        var enumerator = fanOut.CompleteAsync(Query, [Entry("slow", slow)], request.Token).GetAsyncEnumerator(request.Token);
        var next = enumerator.MoveNextAsync();
        request.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await next, Throws.InstanceOf<OperationCanceledException>());
            Assert.That(slow.Tokens.Single().IsCancellationRequested, Is.True);
        });

        await enumerator.DisposeAsync();
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var fanOut = new AddressCompletionFanOut(new ExplorerChromeOptions(), _time);

        Assert.Multiple(() =>
        {
            Assert.That(() => new AddressCompletionFanOut(null!, _time), Throws.ArgumentNullException);
            Assert.That(() => new AddressCompletionFanOut(new ExplorerChromeOptions(), null!), Throws.ArgumentNullException);
            Assert.That(async () => await fanOut.CompleteAsync(null!, []).GetAsyncEnumerator().MoveNextAsync(), Throws.ArgumentNullException);
            Assert.That(async () => await fanOut.CompleteAsync(Query, null!).GetAsyncEnumerator().MoveNextAsync(), Throws.ArgumentNullException);
        });
    }

    private static AddressCompletion Completion(string label) => new(label, ExplorerAddress.ForArea("data", label));

    private static AddressCompletionSourceEntry Entry(string key, IAddressCompletionSource source) => new(key, key.ToUpperInvariant(), source);

    private async Task<List<AddressCompletionBatch>> CollectAsync(params AddressCompletionSourceEntry[] sources)
    {
        var fanOut = new AddressCompletionFanOut(new ExplorerChromeOptions(), _time);
        var batches = new List<AddressCompletionBatch>();
        await foreach (var batch in fanOut.CompleteAsync(Query, sources))
        {
            batches.Add(batch);
        }

        return batches;
    }
}
