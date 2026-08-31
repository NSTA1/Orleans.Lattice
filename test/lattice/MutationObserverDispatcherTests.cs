using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="MutationObserverDispatcher"/> covering fan-out
/// order, zero-observer fast path, swallow-and-log semantics when an
/// observer throws, and the per-observer latency telemetry recorded onto
/// <see cref="LatticeMetrics.ObserverDuration"/>.
/// </summary>
[TestFixture]
public class MutationObserverDispatcherTests
{
    /// <summary>
    /// Each telemetry test uses a tree id unique to itself. The Lattice meter
    /// is a process-wide static, so filtering the recorder on the tree tag is
    /// what keeps the assertions immune to a fixture running in parallel.
    /// </summary>
    private static LatticeMutation SampleSet(string key = "k", string treeId = "t") => new()
    {
        TreeId = treeId,
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

    [Test]
    public async Task PublishAsync_records_observer_duration_with_observer_and_tree_tags()
    {
        const string TreeId = "observer-duration-tags";
        var observer = new RecordingMutationObserver();

        using var recorder = new HistogramMeasurementRecorder(LatticeMetrics.ObserverDuration, TreeId);
        var dispatcher = TestMutationObservers.With(observer);

        await dispatcher.PublishAsync(SampleSet(treeId: TreeId));

        var measurements = recorder.Measurements;
        Assert.That(measurements, Has.Count.EqualTo(1),
            "Exactly one measurement is expected per observer invocation.");

        var measurement = measurements[0];
        Assert.Multiple(() =>
        {
            Assert.That(measurement.Value, Is.GreaterThanOrEqualTo(0d),
                "The recorded duration is an elapsed millisecond count.");
            Assert.That(measurement.Tag(LatticeMetrics.TagObserver),
                Is.EqualTo(typeof(RecordingMutationObserver).FullName),
                "The observer tag attributes the latency to a concrete observer type.");
            Assert.That(measurement.Tag(LatticeMetrics.TagTree), Is.EqualTo(TreeId));
            Assert.That(measurement.Tag(LatticeTenantLabel.TagTenant),
                Is.EqualTo(LatticeTenantLabel.DefaultTenant),
                "Every instrument carries the derived tenant dimension; a bare tree id "
                + "is adopted by the default tenant.");
        });
    }

    [Test]
    public async Task PublishAsync_records_observer_duration_with_the_owning_tenant_for_a_scoped_tree()
    {
        const string TreeId = "t/acme/observer-duration";

        using var recorder = new HistogramMeasurementRecorder(LatticeMetrics.ObserverDuration, TreeId);
        var dispatcher = TestMutationObservers.With(new RecordingMutationObserver());

        await dispatcher.PublishAsync(SampleSet(treeId: TreeId));

        Assert.That(recorder.Measurements, Has.Count.EqualTo(1));
        Assert.That(recorder.Measurements[0].Tag(LatticeTenantLabel.TagTenant), Is.EqualTo("acme"),
            "A tenant-scoped tree id must attribute its observer latency to that tenant.");
    }

    [Test]
    public async Task PublishAsync_records_observer_duration_when_the_observer_throws()
    {
        const string TreeId = "observer-duration-throws";
        var thrower = new ThrowingMutationObserver(new InvalidOperationException("boom"));

        using var recorder = new HistogramMeasurementRecorder(LatticeMetrics.ObserverDuration, TreeId);
        var dispatcher = new MutationObserverDispatcher(
            [thrower], NullLogger<MutationObserverDispatcher>.Instance);

        await dispatcher.PublishAsync(SampleSet(treeId: TreeId));

        Assert.That(recorder.Measurements, Has.Count.EqualTo(1),
            "An observer that throws slowly is exactly what the instrument exists to surface.");
        Assert.That(recorder.Measurements[0].Tag(LatticeMetrics.TagObserver),
            Is.EqualTo(typeof(ThrowingMutationObserver).FullName));
    }

    [Test]
    public async Task PublishAsync_records_no_observer_duration_when_no_observers_registered()
    {
        const string TreeId = "observer-duration-none";

        using var recorder = new HistogramMeasurementRecorder(LatticeMetrics.ObserverDuration, TreeId);
        var dispatcher = TestMutationObservers.NoObservers();

        await dispatcher.PublishAsync(SampleSet(treeId: TreeId));

        Assert.That(recorder.Measurements, Is.Empty,
            "The zero-observer fast path must not touch the histogram at all.");
    }

    [Test]
    public async Task PublishAsync_records_one_observer_duration_per_registered_observer()
    {
        const string TreeId = "observer-duration-fanout";
        var first = new RecordingMutationObserver();
        var second = new ThrowingMutationObserver();
        var third = new CallbackObserver(_ => Task.CompletedTask);

        using var recorder = new HistogramMeasurementRecorder(LatticeMetrics.ObserverDuration, TreeId);
        var dispatcher = new MutationObserverDispatcher(
            [first, second, third], NullLogger<MutationObserverDispatcher>.Instance);

        await dispatcher.PublishAsync(SampleSet(treeId: TreeId));

        var observers = recorder.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagObserver))
            .ToArray();

        Assert.That(observers, Is.EqualTo(new[]
        {
            typeof(RecordingMutationObserver).FullName,
            typeof(ThrowingMutationObserver).FullName,
            typeof(CallbackObserver).FullName,
        }), "One measurement per observer, in registration order, faulting observers included.");
    }

    private sealed class CallbackObserver(Func<LatticeMutation, Task> callback) : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken) =>
            callback(mutation);
    }
}
