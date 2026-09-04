using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalSaturationObserverDispatcher"/>
/// covering fan-out order, zero-observer fast path, and swallow-and-log
/// semantics when an observer throws. Mirrors the contract of the
/// sibling <see cref="MutationObserverDispatcher"/>.
/// </summary>
[TestFixture]
public class WalSaturationObserverDispatcherTests
{
    private static WalSaturationStateChange SampleTransition(
        string treeId = "tree-1",
        WalSaturationState previous = WalSaturationState.Healthy,
        WalSaturationState next = WalSaturationState.Throttled) => new()
    {
        TreeId = treeId,
        PreviousState = previous,
        NewState = next,
        ObservedAt = DateTimeOffset.UnixEpoch,
    };

    [Test]
    public void Ctor_throws_on_null_observers()
    {
        Assert.That(
            () => new WalSaturationObserverDispatcher(null!, NullLogger<WalSaturationObserverDispatcher>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_throws_on_null_logger()
    {
        Assert.That(
            () => new WalSaturationObserverDispatcher([], null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void HasObservers_returns_false_when_none_registered()
    {
        var dispatcher = new WalSaturationObserverDispatcher([], NullLogger<WalSaturationObserverDispatcher>.Instance);
        Assert.That(dispatcher.HasObservers, Is.False);
    }

    [Test]
    public void HasObservers_returns_true_when_at_least_one_registered()
    {
        var dispatcher = new WalSaturationObserverDispatcher(
            [new RecordingWalSaturationObserver()],
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        Assert.That(dispatcher.HasObservers, Is.True);
    }

    [Test]
    public async Task PublishAsync_is_noop_when_no_observers_registered()
    {
        var dispatcher = new WalSaturationObserverDispatcher([], NullLogger<WalSaturationObserverDispatcher>.Instance);

        // "Should complete synchronously without throwing" is an
        // assertable claim: the empty fan-out must short-circuit before
        // the first await, so the returned ValueTask is already completed
        // when it is handed back.
        var publish = dispatcher.PublishAsync(SampleTransition());

        Assert.That(publish.IsCompletedSuccessfully, Is.True,
            "with no observers registered PublishAsync must complete synchronously and without faulting.");
        await publish;
    }

    [Test]
    public async Task PublishAsync_invokes_every_registered_observer_in_order()
    {
        var order = new List<int>();
        var a = new CallbackObserver(_ => { order.Add(1); return ValueTask.CompletedTask; });
        var b = new CallbackObserver(_ => { order.Add(2); return ValueTask.CompletedTask; });
        var c = new CallbackObserver(_ => { order.Add(3); return ValueTask.CompletedTask; });

        var dispatcher = new WalSaturationObserverDispatcher(
            [a, b, c],
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        await dispatcher.PublishAsync(SampleTransition());

        Assert.That(order, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task PublishAsync_continues_when_one_observer_throws_and_logs_warning()
    {
        var recorded = new RecordingWalSaturationObserver();
        var thrower = new ThrowingWalSaturationObserver(new InvalidOperationException("boom"));

        var logger = Substitute.For<ILogger<WalSaturationObserverDispatcher>>();
        var dispatcher = new WalSaturationObserverDispatcher([thrower, recorded], logger);

        await dispatcher.PublishAsync(SampleTransition("tree-X"));

        Assert.That(recorded.Changes, Has.Count.EqualTo(1));
        Assert.That(recorded.Changes[0].TreeId, Is.EqualTo("tree-X"));

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
        var captured = new RecordingWalSaturationObserver();
        var dispatcher = new WalSaturationObserverDispatcher(
            [captured],
            NullLogger<WalSaturationObserverDispatcher>.Instance);

        var change = new WalSaturationStateChange
        {
            TreeId = "tree-42",
            PreviousState = WalSaturationState.Throttled,
            NewState = WalSaturationState.Saturated,
            AttributedPartition = 3,
            AttributedShard = 7,
            ObservedAt = new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero),
        };
        await dispatcher.PublishAsync(change);

        Assert.That(captured.Changes, Has.Count.EqualTo(1));
        Assert.That(captured.Changes[0], Is.EqualTo(change));
    }

    private sealed class CallbackObserver(Func<WalSaturationStateChange, ValueTask> callback) : IWalSaturationObserver
    {
        public ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken) =>
            callback(change);
    }
}
