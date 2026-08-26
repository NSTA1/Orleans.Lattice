using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAliasObserverDispatcher"/> covering fan-out
/// order, the zero-observer fast path, and swallow-and-log semantics when an
/// observer throws. Mirrors the contract of the sibling
/// <see cref="MutationObserverDispatcher"/> and
/// <see cref="WalSaturationObserverDispatcher"/>.
/// </summary>
[TestFixture]
public class TreeAliasObserverDispatcherTests
{
    private static TreeAliasChange SampleChange(
        string treeId = "tree-1",
        string oldPhysical = "tree-1",
        string newPhysical = "phys-1") => new()
    {
        TreeId = treeId,
        OldPhysicalTreeId = oldPhysical,
        NewPhysicalTreeId = newPhysical,
    };

    [Test]
    public void Ctor_throws_on_null_observers()
    {
        Assert.That(
            () => new TreeAliasObserverDispatcher(null!, NullLogger<TreeAliasObserverDispatcher>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_throws_on_null_logger()
    {
        Assert.That(
            () => new TreeAliasObserverDispatcher([], null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void HasObservers_returns_false_when_none_registered()
    {
        var dispatcher = new TreeAliasObserverDispatcher([], NullLogger<TreeAliasObserverDispatcher>.Instance);
        Assert.That(dispatcher.HasObservers, Is.False);
    }

    [Test]
    public void HasObservers_returns_true_when_at_least_one_registered()
    {
        var dispatcher = new TreeAliasObserverDispatcher(
            [new RecordingObserver()],
            NullLogger<TreeAliasObserverDispatcher>.Instance);
        Assert.That(dispatcher.HasObservers, Is.True);
    }

    [Test]
    public async Task PublishAsync_is_noop_when_no_observers_registered()
    {
        var dispatcher = new TreeAliasObserverDispatcher([], NullLogger<TreeAliasObserverDispatcher>.Instance);
        await dispatcher.PublishAsync(SampleChange());
    }

    [Test]
    public async Task PublishAsync_invokes_every_registered_observer_in_order()
    {
        var order = new List<int>();
        var a = new CallbackObserver(_ => { order.Add(1); return Task.CompletedTask; });
        var b = new CallbackObserver(_ => { order.Add(2); return Task.CompletedTask; });
        var c = new CallbackObserver(_ => { order.Add(3); return Task.CompletedTask; });

        var dispatcher = new TreeAliasObserverDispatcher(
            [a, b, c], NullLogger<TreeAliasObserverDispatcher>.Instance);
        await dispatcher.PublishAsync(SampleChange());

        Assert.That(order, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task PublishAsync_continues_when_one_observer_throws_and_logs_warning()
    {
        var recorded = new RecordingObserver();
        var thrower = new CallbackObserver(_ => throw new InvalidOperationException("boom"));

        var logger = Substitute.For<ILogger<TreeAliasObserverDispatcher>>();
        var dispatcher = new TreeAliasObserverDispatcher([thrower, recorded], logger);

        await dispatcher.PublishAsync(SampleChange("tree-X"));

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
        var captured = new RecordingObserver();
        var dispatcher = new TreeAliasObserverDispatcher(
            [captured], NullLogger<TreeAliasObserverDispatcher>.Instance);

        var change = new TreeAliasChange
        {
            TreeId = "tree-42",
            OldPhysicalTreeId = "phys-old",
            NewPhysicalTreeId = "phys-new",
        };
        await dispatcher.PublishAsync(change);

        Assert.That(captured.Changes, Has.Count.EqualTo(1));
        Assert.That(captured.Changes[0], Is.EqualTo(change));
    }

    private sealed class RecordingObserver : ITreeAliasObserver
    {
        public List<TreeAliasChange> Changes { get; } = [];

        public Task OnTreeAliasChangedAsync(TreeAliasChange change, CancellationToken cancellationToken)
        {
            Changes.Add(change);
            return Task.CompletedTask;
        }
    }

    private sealed class CallbackObserver(Func<TreeAliasChange, Task> callback) : ITreeAliasObserver
    {
        public Task OnTreeAliasChangedAsync(TreeAliasChange change, CancellationToken cancellationToken) =>
            callback(change);
    }
}
