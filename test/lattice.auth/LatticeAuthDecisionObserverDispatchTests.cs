using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Coverage for the arms of <see cref="LatticeAuthDecisionObserver"/> that only
/// an unusual host shape reaches: sinks handed in as a lazy sequence rather than
/// an array, a sink that faults <b>after</b> the fire-and-forget dispatch has
/// already returned, and the bound on how many operation/tree pairs the observer
/// will zero-prime. Each is on the observability seam, where a fault must stay
/// invisible to the decision it observes.
/// </summary>
[TestFixture]
public sealed class LatticeAuthDecisionObserverDispatchTests
{
    private static readonly LatticeSubject Subject = new("alice");

    private static LatticeAccessRequest Request(
        LatticeOperation operation = LatticeOperation.Read,
        string tree = "app") =>
        new(tree, operation, Subject, "k");

    private static LatticeAuthOptions AuditingOptions() => new() { EnableAuditSink = true };

    [Test]
    public void Sinks_supplied_as_a_lazy_sequence_are_materialised_once_at_construction()
    {
        // Registrations resolve as IEnumerable<T>, and a container (or a host
        // composing sinks with LINQ) can hand over a deferred sequence rather than
        // an array. Materialising it once at construction is what keeps the
        // dispatch path allocation-free and stops a deferred query being
        // re-enumerated on every decision - on the hot path of every gated call.
        var sink = new RecordingSink();
        var sequence = new CountingSequence(sink);

        var observer = new LatticeAuthDecisionObserver(
            sequence,
            new StubOptionsMonitor<LatticeAuthOptions>(AuditingOptions()),
            NullLogger<LatticeAuthDecisionObserver>.Instance);

        Assert.That(sequence.Enumerations, Is.EqualTo(1),
            "the sequence is materialised exactly once, by the constructor");

        var request = Request();
        var decision = LatticeAccessDecision.Deny("no rule");
        observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: 0);
        observer.Observe(in request, in decision, default, epoch: 2, startTimestamp: 0);

        Assert.Multiple(() =>
        {
            Assert.That(sequence.Enumerations, Is.EqualTo(1),
                "dispatching a decision must not re-enumerate the registration sequence");
            Assert.That(sink.Events, Has.Count.EqualTo(2),
                "positive control: the materialised sink really did receive both decisions");
        });
    }

    [Test]
    public async Task A_sink_that_faults_after_returning_is_logged_and_never_reaches_the_caller()
    {
        // The dispatch is fire-and-forget: a sink whose ValueTask has not yet
        // completed is observed on a background continuation. A fault arriving
        // there has no caller to surface to, so it must be logged rather than
        // left as an unobserved task exception.
        var logger = new CapturingLogger();
        var faulting = new DeferredFaultSink();
        var observer = new LatticeAuthDecisionObserver(
            new ILatticeAuthAuditSink[] { faulting },
            new StubOptionsMonitor<LatticeAuthOptions>(AuditingOptions()),
            logger);

        var request = Request();
        var decision = LatticeAccessDecision.Deny("no rule");

        // Returns while the sink's write is still pending, so the fault below
        // cannot possibly be observed synchronously.
        observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: 0);
        Assert.That(logger.Entries, Is.Empty, "the dispatch really was still in flight");

        faulting.Fault(new InvalidOperationException("sink went away"));

        await TestPoll.UntilAsync(
            () => logger.Entries.Count > 0,
            "the background continuation logged the sink fault");

        var entry = logger.Entries[0];
        Assert.Multiple(() =>
        {
            Assert.That(entry.Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(entry.Exception, Is.TypeOf<InvalidOperationException>());
            Assert.That(entry.Message, Does.Contain(nameof(DeferredFaultSink)),
                "the log names the sink that failed, so an operator can identify it");
        });
    }

    [Test]
    public void Priming_stops_at_the_pair_cap_while_decisions_keep_recording()
    {
        // The primed-pair set is bounded so a host with an unbounded tree space
        // cannot grow it without limit. Past the bound a pair is simply not
        // primed; its decisions are still counted, because dropping those would
        // trade a memory bound for a hole in the security counter.
        var cap = PairCap();

        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = new LatticeAuthDecisionObserver(
            Array.Empty<ILatticeAuthAuditSink>(),
            new StubOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()),
            NullLogger<LatticeAuthDecisionObserver>.Instance);
        var decision = LatticeAccessDecision.Allow();

        for (var i = 0; i < cap; i++)
        {
            var filling = Request(tree: $"tree-{i}");
            observer.Observe(in filling, in decision, default, epoch: 1, startTimestamp: 0);
        }

        var primesAtCap = collector.Measurements.Count(m => m.Value == 0);
        var decisionsAtCap = collector.Measurements.Count(m => m.Value == 1);

        var overflow = Request(tree: "tree-past-the-cap");
        observer.Observe(in overflow, in decision, default, epoch: 1, startTimestamp: 0);

        Assert.Multiple(() =>
        {
            Assert.That(primesAtCap, Is.EqualTo(cap * 2),
                "positive control: every pair below the cap primed both effect arms");
            Assert.That(collector.Measurements.Count(m => m.Value == 0), Is.EqualTo(primesAtCap),
                "a pair past the cap is not primed");
            Assert.That(collector.Measurements.Count(m => m.Value == 1), Is.EqualTo(decisionsAtCap + 1),
                "but its decision is still recorded - the cap bounds priming, not counting");
        });
    }

    // Read from the production constant rather than duplicated, so the test
    // cannot drift into filling the wrong number of pairs and silently stop
    // reaching the cap at all.
    private static int PairCap()
    {
        var field = typeof(LatticeAuthDecisionObserver).GetField(
            "MaxPrimedDecisionPairs",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        Assert.That(field, Is.Not.Null, "the primed-pair cap constant is still named MaxPrimedDecisionPairs");
        return (int)field!.GetRawConstantValue()!;
    }

    // A deferred sequence that counts how many times it is walked, so
    // "materialised once" is asserted rather than assumed.
    private sealed class CountingSequence(params ILatticeAuthAuditSink[] sinks) : IEnumerable<ILatticeAuthAuditSink>
    {
        public int Enumerations { get; private set; }

        public IEnumerator<ILatticeAuthAuditSink> GetEnumerator()
        {
            Enumerations++;
            return ((IEnumerable<ILatticeAuthAuditSink>)sinks).GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class RecordingSink : ILatticeAuthAuditSink
    {
        private readonly List<LatticeAuthDecisionEvent> _events = new();

        public IReadOnlyList<LatticeAuthDecisionEvent> Events
        {
            get
            {
                lock (_events)
                {
                    return _events.ToArray();
                }
            }
        }

        public ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default)
        {
            lock (_events)
            {
                _events.Add(decisionEvent);
            }

            return ValueTask.CompletedTask;
        }
    }

    // A sink whose write stays pending until the test faults it, so the observer
    // must take the background-observation path rather than the synchronous one.
    private sealed class DeferredFaultSink : ILatticeAuthAuditSink
    {
        private readonly TaskCompletionSource _pending =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Fault(Exception exception) => _pending.TrySetException(exception);

        public ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default) =>
            new(_pending.Task);
    }

    private sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

    private sealed class CapturingLogger : ILogger<LatticeAuthDecisionObserver>
    {
        private readonly List<LogEntry> _entries = new();

        public IReadOnlyList<LogEntry> Entries
        {
            get
            {
                lock (_entries)
                {
                    return _entries.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_entries)
            {
                _entries.Add(new LogEntry(logLevel, formatter(state, exception), exception));
            }
        }
    }

    private sealed class StubOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
