using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAuthDecisionObserver"/>, the post-decision
/// observability seam. Covers metric emission with the expected tags, the audit
/// verbosity filter (deny-only vs all decisions), the sampling gate, the
/// well-formed decision event handed to sinks, and the zero-cost fast exit when
/// nothing is listening and auditing is off.
/// </summary>
[TestFixture]
public sealed class LatticeAuthDecisionObserverTests
{
    private static readonly LatticeSubject Subject = new("alice");

    private static LatticeAccessRequest Request(
        LatticeOperation operation = LatticeOperation.Read,
        string tree = "app",
        string? key = "k") =>
        new(tree, operation, Subject, key);

    private static LatticeAuthDecisionObserver CreateObserver(
        LatticeAuthOptions options,
        params ILatticeAuthAuditSink[] sinks) =>
        new(
            sinks,
            new StubOptionsMonitor<LatticeAuthOptions>(options),
            NullLogger<LatticeAuthDecisionObserver>.Instance);

    [Test]
    public void Observe_records_the_decisions_counter_with_operation_tree_and_effect_tags()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver(new LatticeAuthOptions());
        var request = Request(LatticeOperation.Write, "orders", "k1");
        var decision = LatticeAccessDecision.Deny("no rule");

        observer.Observe(in request, in decision, default, epoch: 7, LatticeAuthDecisionObserver.CaptureStart());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var tags = collector.Measurements.Single().Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.That(tags[LatticeAuthMetrics.TagOperation], Is.EqualTo("Write"));
        Assert.That(tags[LatticeAuthMetrics.TagTree], Is.EqualTo("orders"));
        Assert.That(tags[LatticeAuthMetrics.TagEffect], Is.EqualTo(LatticeAuthMetrics.EffectDeny));
    }

    [Test]
    public void Observe_tags_an_allowed_decision_with_the_allow_effect()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver(new LatticeAuthOptions());
        var request = Request();
        var decision = LatticeAccessDecision.Allow();

        observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: 0);

        var tags = collector.Measurements.Single().Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.That(tags[LatticeAuthMetrics.TagEffect], Is.EqualTo(LatticeAuthMetrics.EffectAllow));
    }

    [Test]
    public void Observe_tags_a_schema_admin_decision_with_the_schema_admin_operation()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver(new LatticeAuthOptions());
        var request = Request(LatticeOperation.SchemaAdmin, "orders", key: null);
        var decision = LatticeAccessDecision.Allow();

        observer.Observe(in request, in decision, default, epoch: 3, LatticeAuthDecisionObserver.CaptureStart());

        var tags = collector.Measurements.Single().Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.That(tags[LatticeAuthMetrics.TagOperation], Is.EqualTo("SchemaAdmin"),
            "a schema-admin decision is audited with a clear, distinct operation tag");
    }

    [Test]
    public void IsAuditEnabled_reflects_the_option()
    {
        Assert.That(CreateObserver(new LatticeAuthOptions()).IsAuditEnabled, Is.False);
        Assert.That(CreateObserver(new LatticeAuthOptions { EnableAuditSink = true }).IsAuditEnabled, Is.True);
    }

    [Test]
    public void Observe_with_audit_off_dispatches_no_event()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(new LatticeAuthOptions { EnableAuditSink = false }, sink);
        var request = Request();
        var decision = LatticeAccessDecision.Deny("no");

        observer.Observe(in request, in decision, default, 1, 0);

        Assert.That(sink.Events, Is.Empty, "auditing off must dispatch nothing");
    }

    [Test]
    public void DenyOnly_verbosity_dispatches_denied_but_not_allowed_decisions()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions { EnableAuditSink = true, AuditVerbosity = LatticeAuthAuditVerbosity.DenyOnly },
            sink);

        var allow = LatticeAccessDecision.Allow();
        var deny = LatticeAccessDecision.Deny("blocked");
        var request = Request();
        observer.Observe(in request, in allow, default, 1, 0);
        observer.Observe(in request, in deny, default, 1, 0);

        Assert.That(sink.Events, Has.Count.EqualTo(1), "deny-only audits refusals only");
        Assert.That(sink.Events.Single().Effect, Is.EqualTo(LatticeEffect.Deny));
    }

    [Test]
    public void AllDecisions_verbosity_dispatches_both_allowed_and_denied_decisions()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions { EnableAuditSink = true, AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions },
            sink);

        var allow = LatticeAccessDecision.Allow();
        var deny = LatticeAccessDecision.Deny("blocked");
        var request = Request();
        observer.Observe(in request, in allow, default, 1, 0);
        observer.Observe(in request, in deny, default, 1, 0);

        Assert.That(sink.Events, Has.Count.EqualTo(2), "all-decisions audits both effects");
    }

    [Test]
    public void Zero_sampling_ratio_suppresses_all_audit_dispatch()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions
            {
                EnableAuditSink = true,
                AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions,
                AuditSamplingRatio = 0.0,
            },
            sink);

        var deny = LatticeAccessDecision.Deny("blocked");
        var request = Request();
        for (var i = 0; i < 20; i++)
        {
            observer.Observe(in request, in deny, default, 1, 0);
        }

        Assert.That(sink.Events, Is.Empty, "a 0.0 sampling ratio suppresses every dispatch");
    }

    [Test]
    public void Full_sampling_ratio_dispatches_every_admissible_decision()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions
            {
                EnableAuditSink = true,
                AuditVerbosity = LatticeAuthAuditVerbosity.DenyOnly,
                AuditSamplingRatio = 1.0,
            },
            sink);

        var deny = LatticeAccessDecision.Deny("blocked");
        var request = Request();
        for (var i = 0; i < 10; i++)
        {
            observer.Observe(in request, in deny, default, 1, 0);
        }

        Assert.That(sink.Events, Has.Count.EqualTo(10), "a 1.0 sampling ratio dispatches every admissible decision");
    }

    [Test]
    public void Dispatched_event_carries_the_request_decision_and_matched_rule()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions { EnableAuditSink = true, AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions },
            sink);

        var request = Request(LatticeOperation.Write, "orders", "k9");
        var decision = LatticeAccessDecision.Deny("group rule denies");
        var match = new PolicyMatch(LatticeEffect.Deny, "rule-42", LatticeScopeKind.Prefix, "k");

        observer.Observe(in request, in decision, in match, epoch: 5, startTimestamp: 0);

        var evt = sink.Events.Single();
        Assert.Multiple(() =>
        {
            Assert.That(evt.SubjectId, Is.EqualTo("alice"));
            Assert.That(evt.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(evt.TreeId, Is.EqualTo("orders"));
            Assert.That(evt.Key, Is.EqualTo("k9"));
            Assert.That(evt.Effect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(evt.PolicyEpoch, Is.EqualTo(5));
            Assert.That(evt.MatchedRuleId, Is.EqualTo("rule-42"));
            Assert.That(evt.MatchedScopeKind, Is.EqualTo(LatticeScopeKind.Prefix));
            Assert.That(evt.MatchedScopeValue, Is.EqualTo("k"));
            Assert.That(evt.Reason, Is.EqualTo("group rule denies"));
        });
    }

    [Test]
    public void Dispatched_event_for_an_unmatched_decision_carries_null_rule_fields()
    {
        var sink = new CapturingSink();
        var observer = CreateObserver(
            new LatticeAuthOptions { EnableAuditSink = true, AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions },
            sink);

        var request = Request();
        var decision = LatticeAccessDecision.Deny("default deny");
        observer.Observe(in request, in decision, default, 1, 0);

        var evt = sink.Events.Single();
        Assert.That(evt.MatchedRuleId, Is.Null);
        Assert.That(evt.MatchedScopeKind, Is.Null);
        Assert.That(evt.MatchedScopeValue, Is.Null);
    }

    [Test]
    public void A_faulting_sink_never_throws_back_into_the_caller()
    {
        var observer = CreateObserver(
            new LatticeAuthOptions { EnableAuditSink = true, AuditVerbosity = LatticeAuthAuditVerbosity.AllDecisions },
            new ThrowingSink());

        var request = Request();
        var decision = LatticeAccessDecision.Deny("blocked");

        Assert.That(
            () => observer.Observe(in request, in decision, default, 1, 0),
            Throws.Nothing,
            "a sink fault must be swallowed so it cannot disturb the decision path");
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var options = new StubOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions());
        Assert.That(
            () => new LatticeAuthDecisionObserver(null!, options, NullLogger<LatticeAuthDecisionObserver>.Instance),
            Throws.ArgumentNullException);
        Assert.That(
            () => new LatticeAuthDecisionObserver(Array.Empty<ILatticeAuthAuditSink>(), null!, NullLogger<LatticeAuthDecisionObserver>.Instance),
            Throws.ArgumentNullException);
        Assert.That(
            () => new LatticeAuthDecisionObserver(Array.Empty<ILatticeAuthAuditSink>(), options, null!),
            Throws.ArgumentNullException);
    }

    private sealed class CapturingSink : ILatticeAuthAuditSink
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

    private sealed class ThrowingSink : ILatticeAuthAuditSink
    {
        public ValueTask WriteAsync(LatticeAuthDecisionEvent decisionEvent, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("sink is down");
    }

    private sealed class StubOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
