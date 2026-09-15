using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Tests that both <see cref="LatticeAuthMetrics.TagEffect"/> arms of
/// <see cref="LatticeAuthMetrics.Decisions"/> are zero-primed the first time the
/// gate decides an operation/tree pair.
/// </summary>
/// <remarks>
/// <para>
/// The defect these close: a denial counter with no series and a denial counter
/// reading zero are byte-identical at the query, and on a security surface they
/// have opposite readings. "Nothing was denied" is the reassuring one and was,
/// before this priming, indistinguishable from a gate that never ran, was never
/// registered, or is fail-open. The absence hid inside the healthy reading, which
/// is the shape an operator is least likely to interrogate.
/// </para>
/// <para>
/// Every assertion here is falsifiable by a named mutation: deleting the
/// <c>PrimeDecisionPairOnce</c> call reddens the two arm-existence tests, deleting
/// the dedupe reddens the repeat test, keying the dedupe on operation alone
/// reddens the second-pair test, and priming the latency histogram for symmetry
/// reddens <see cref="The_latency_histogram_is_deliberately_not_primed"/>.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeAuthDecisionPrimingTests
{
    private static readonly LatticeSubject Subject = new("alice");

    private static LatticeAuthDecisionObserver CreateObserver() =>
        new(
            [],
            new StubOptionsMonitor<LatticeAuthOptions>(new LatticeAuthOptions()),
            NullLogger<LatticeAuthDecisionObserver>.Instance);

    private static LatticeAccessRequest Request(
        LatticeOperation operation = LatticeOperation.Read,
        string tree = "app") =>
        new(tree, operation, Subject, "k");
    private static IReadOnlyDictionary<string, object?> TagsOf(RecordedMeasurement<long> measurement) =>
        measurement.Tags.ToDictionary(t => t.Key, t => t.Value);

    [Test]
    public void A_single_allowed_decision_publishes_a_zero_deny_arm()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver();
        var request = Request(LatticeOperation.Read, "orders");
        var decision = LatticeAccessDecision.Allow();

        observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: 0);

        var deny = collector.Measurements
            .Where(m => (string?)TagsOf(m)[LatticeAuthMetrics.TagEffect] == LatticeAuthMetrics.EffectDeny)
            .ToList();

        Assert.That(deny, Has.Count.EqualTo(1),
            "the deny arm must exist after an allowed decision. Without it, a scrape showing "
            + "only allow series cannot distinguish 'the gate denied nothing' from 'the gate "
            + "never ran', and those have opposite readings on a security surface.");
        Assert.That(deny.Single().Value, Is.EqualTo(0L),
            "the deny arm is primed, not incremented - priming must not manufacture a denial");
    }

    [Test]
    public void The_primed_deny_arm_carries_the_same_tags_as_a_real_denial()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver();
        var request = Request(LatticeOperation.Write, "orders");
        var allow = LatticeAccessDecision.Allow();

        observer.Observe(in request, in allow, default, epoch: 1, startTimestamp: 0);

        var primedDeny = TagsOf(collector.Measurements
            .Single(m => m.Value == 0
                && (string?)TagsOf(m)[LatticeAuthMetrics.TagEffect] == LatticeAuthMetrics.EffectDeny));

        Assert.Multiple(() =>
        {
            Assert.That(primedDeny[LatticeAuthMetrics.TagOperation], Is.EqualTo("Write"));
            Assert.That(primedDeny[LatticeAuthMetrics.TagTree], Is.EqualTo("orders"));
            Assert.That(primedDeny[LatticeAuthMetrics.TagEffect], Is.EqualTo(LatticeAuthMetrics.EffectDeny));
        });

        // The prime is only useful if it lands on the SAME series a real denial would.
        // A prime carrying a different tag set creates a second, parallel series and
        // leaves the real deny arm just as absent as before, while looking fixed.
        Assert.That(
            primedDeny.Keys.OrderBy(k => k, StringComparer.Ordinal),
            Is.EqualTo(RealDenialTagKeys(observer)).AsCollection,
            "the primed arm and a real denial must agree on their tag keys, or the prime "
            + "creates a parallel series instead of pre-creating the real one");
    }

    private static IEnumerable<string> RealDenialTagKeys(LatticeAuthDecisionObserver observer)
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var request = Request(LatticeOperation.Write, "orders");
        var deny = LatticeAccessDecision.Deny("no rule");

        observer.Observe(in request, in deny, default, epoch: 1, startTimestamp: 0);

        return TagsOf(collector.Measurements.Single(m => m.Value == 1))
            .Keys.OrderBy(k => k, StringComparer.Ordinal)
            .ToList();
    }

    [Test]
    public void A_pair_is_primed_once_however_many_decisions_it_sees()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver();
        var request = Request(LatticeOperation.Read, "orders");
        var decision = LatticeAccessDecision.Allow();

        for (var i = 0; i < 5; i++)
        {
            observer.Observe(in request, in decision, default, epoch: 1, startTimestamp: 0);
        }

        Assert.That(collector.Measurements.Count(m => m.Value == 0), Is.EqualTo(2),
            "priming is per pair, not per decision: five decisions on one pair emit exactly "
            + "the two zero-primes from the first of them");
        Assert.That(collector.Measurements.Count(m => m.Value == 1), Is.EqualTo(5),
            "every decision is still recorded");
    }

    [Test]
    public void A_different_operation_or_tree_is_primed_separately()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        var observer = CreateObserver();
        var decision = LatticeAccessDecision.Allow();

        var readOrders = Request(LatticeOperation.Read, "orders");
        var writeOrders = Request(LatticeOperation.Write, "orders");
        var readInvoices = Request(LatticeOperation.Read, "invoices");

        observer.Observe(in readOrders, in decision, default, epoch: 1, startTimestamp: 0);
        observer.Observe(in writeOrders, in decision, default, epoch: 1, startTimestamp: 0);
        observer.Observe(in readInvoices, in decision, default, epoch: 1, startTimestamp: 0);

        Assert.That(collector.Measurements.Count(m => m.Value == 0), Is.EqualTo(6),
            "three distinct operation/tree pairs prime two arms each. A dedupe keyed on only "
            + "one component of the pair would collapse these and silently leave real pairs "
            + "unprimed.");
    }

    [Test]
    public void The_latency_histogram_is_deliberately_not_primed()
    {
        // The Decisions collector is not incidental. Observe only enters the priming
        // branch when the counter has a listener, so a fixture that watches the
        // histogram alone never arms priming and passes whatever the priming code
        // does - it would report this boundary as held while the histogram was being
        // primed on every pair. Collecting both makes the premise asserted rather
        // than assumed: the counter assertion below is the positive control proving
        // priming actually ran, without which the histogram count is evidence of
        // nothing.
        using var decisions = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);
        using var durations = new MeterCollector<double>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionDurationName);
        var observer = CreateObserver();
        var request = Request(LatticeOperation.Read, "orders");
        var decision = LatticeAccessDecision.Allow();

        observer.Observe(in request, in decision, default, epoch: 1, LatticeAuthDecisionObserver.CaptureStart());

        Assert.That(decisions.Measurements.Count(m => m.Value == 0), Is.EqualTo(2),
            "positive control: priming must have run, or this test's histogram "
            + "assertion is vacuous");
        Assert.That(durations.Measurements, Has.Count.EqualTo(1),
            "one decision records exactly one latency sample. The histogram carries the same "
            + "tags as the decision counter and has the same absent-arm behaviour, but the "
            + "counter's remedy is a defect here: a counter is primed by adding zero, which "
            + "changes nothing, whereas priming a histogram means recording a 0 ms sample - a "
            + "real observation that moves count, sum and every bucket below the first, "
            + "corrupting the distribution it exists to measure.");
    }

    [Test]
    public void PrimeDecisions_rejects_a_null_operation_or_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeAuthMetrics.PrimeDecisions(null!, "orders"),
                Throws.ArgumentNullException);
            Assert.That(() => LatticeAuthMetrics.PrimeDecisions("Read", null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void PrimeDecisions_emits_exactly_the_two_effect_arms_at_zero()
    {
        using var collector = new MeterCollector<long>(
            LatticeAuthMetrics.MeterName, LatticeAuthMetrics.DecisionsName);

        LatticeAuthMetrics.PrimeDecisions("Read", "orders");

        var effects = collector.Measurements
            .Select(m => (string?)TagsOf(m)[LatticeAuthMetrics.TagEffect])
            .OrderBy(e => e, StringComparer.Ordinal)
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(collector.Measurements, Has.Count.EqualTo(2));
            Assert.That(collector.Measurements.Select(m => m.Value), Is.All.EqualTo(0L));
            Assert.That(effects, Is.EqualTo(new[]
            {
                LatticeAuthMetrics.EffectAllow,
                LatticeAuthMetrics.EffectDeny,
            }).AsCollection, "both arms are primed, not just the one that is usually absent");
        });
    }

    private sealed class StubOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
