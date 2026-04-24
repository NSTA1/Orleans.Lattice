using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="MutationObserverDispatcher"/> covering fan-out
/// order, zero-observer fast path, and swallow-and-log semantics when an
/// observer throws.
/// </summary>
[TestFixture]
public class MutationObserverDispatcherTests
{
    private static LatticeMutation SampleSet(string key = "k") => new()
    {
        TreeId = "t",
        Kind = MutationKind.Set,
        Key = key,
        Value = [1, 2, 3],
    };

    [Test]
    public void Ctor_throws_on_null_observers()
    {
        Assert.That(
            () => new MutationObserverDispatcher(null!, NullLogger<MutationObserverDispatcher>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_throws_on_null_logger()
    {
        Assert.That(
            () => new MutationObserverDispatcher([], null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void HasObservers_returns_false_when_none_registered()
    {
        var dispatcher = TestMutationObservers.NoObservers();
        Assert.That(dispatcher.HasObservers, Is.False);
    }

    [Test]
    public void HasObservers_returns_true_when_at_least_one_registered()
    {
        var dispatcher = TestMutationObservers.With(new RecordingMutationObserver());
        Assert.That(dispatcher.HasObservers, Is.True);
    }

    [Test]
    public async Task PublishAsync_is_noop_when_no_observers_registered()
    {
        var dispatcher = TestMutationObservers.NoObservers();
        // Should complete synchronously without throwing.
        await dispatcher.PublishAsync(SampleSet());
    }

    [Test]
    public async Task PublishAsync_invokes_every_registered_observer_in_order()
    {
        var order = new List<int>();
        var a = new CallbackObserver(m => { order.Add(1); return Task.CompletedTask; });
        var b = new CallbackObserver(m => { order.Add(2); return Task.CompletedTask; });
        var c = new CallbackObserver(m => { order.Add(3); return Task.CompletedTask; });

        var dispatcher = TestMutationObservers.With(a, b, c);
        await dispatcher.PublishAsync(SampleSet());

        Assert.That(order, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task PublishAsync_continues_when_one_observer_throws_and_logs_warning()
    {
        var recorded = new RecordingMutationObserver();
        var thrower = new ThrowingMutationObserver(new InvalidOperationException("boom"));

        var logger = Substitute.For<ILogger<MutationObserverDispatcher>>();
        var dispatcher = new MutationObserverDispatcher([thrower, recorded], logger);

        await dispatcher.PublishAsync(SampleSet("key-a"));

        // The second observer still received the mutation.
        Assert.That(recorded.Mutations, Has.Count.EqualTo(1));
        Assert.That(recorded.Mutations[0].Key, Is.EqualTo("key-a"));

        // And the thrower's failure was logged at Warning.
        logger.Received().Log(
            LogLevel.Warning,
            Arg.Any<EventId>(),
            Arg.Any<object>(),
            Arg.Is<Exception?>(e => e is InvalidOperationException),
            Arg.Any<Func<object, Exception?, string>>());
    }

    [Test]
    public async Task PublishAsync_delivers_payload_verbatim()
    {
        var captured = new RecordingMutationObserver();
        var dispatcher = TestMutationObservers.With(captured);

        var mutation = new LatticeMutation
        {
            TreeId = "tree-42",
            Kind = MutationKind.Delete,
            Key = "user/7",
            IsTombstone = true,
        };
        await dispatcher.PublishAsync(mutation);

        Assert.That(captured.Mutations, Has.Count.EqualTo(1));
        Assert.That(captured.Mutations[0], Is.EqualTo(mutation));
    }

    private sealed class CallbackObserver(Func<LatticeMutation, Task> callback) : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken) =>
            callback(mutation);
    }
}
