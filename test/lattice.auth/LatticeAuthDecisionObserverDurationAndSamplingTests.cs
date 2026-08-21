using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Supplementary unit tests for <see cref="LatticeAuthDecisionObserver"/> covering
/// the two branches the sibling fixture does not reach: the decision-latency
/// histogram <c>Record</c> path (a listener is attached and a non-zero start
/// timestamp was captured) and the partial (0 &lt; ratio &lt; 1) audit sampling
/// gate.
/// </summary>
[TestFixture]
public sealed class LatticeAuthDecisionObserverDurationAndSamplingTests
{
    private static readonly LatticeSubject Subject = new("alice");

    private static LatticeAuthDecisionObserver CreateObserver(
        LatticeAuthOptions options,
        params ILatticeAuthAuditSink[] sinks) =>
        new(
            sinks,
            new CovOptionsMonitor<LatticeAuthOptions>(options),
            NullLogger<LatticeAuthDecisionObserver>.Instance);

    [Test]
    public void Observe_records_decision_duration_when_a_listener_is_attached_and_start_is_nonzero()
    {
        using var collector = new MeterCollector<double>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionDurationName);
        var observer = CreateObserver(new LatticeAuthOptions());
        var request = new LatticeAccessRequest("app", LatticeOperation.Read, Subject, "k");
        var decision = LatticeAccessDecision.Allow();
        var start = Stopwatch.GetTimestamp();

        observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: start);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1), "the latency histogram must record once when a listener is attached");
        var tags = collector.Measurements.Single().Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.That(tags[LatticeAuthMetrics.TagTree], Is.EqualTo("app"));
    }

    [Test]
    public void Observe_with_a_partial_sampling_ratio_completes_and_still_records_metrics()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var sink = new CountingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions
            {
                EnableAuditSink = true,
                AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions,
                AuditSamplingRatio = 0.5,
            },
            sink);
        var request = new LatticeAccessRequest("app", LatticeOperation.Read, Subject, "k");
        var decision = LatticeAccessDecision.Deny("blocked");

        Assert.That(
            () => observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: 0),
            Throws.Nothing,
            "the partial-sampling gate must never throw back into the decision path");
        Assert.That(collector.Measurements, Has.Count.EqualTo(1), "the decision counter fires regardless of the sampling outcome");
        Assert.That(sink.Count, Is.InRange(0, 1), "a single observation dispatches at most one sampled event");
    }

    private sealed class CountingSink : ILatticeAuthAuditSink
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _count);
            return ValueTask.CompletedTask;
        }
    }
}
