using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the resolved-replay-gate startup record (issues #2278, #2279).
/// <para>
/// The replay concurrency gate defaults to
/// <see cref="Environment.ProcessorCount"/>, and a comment on that path used to
/// assert flatly that this "honours a container CPU quota". It does so only
/// while <c>DOTNET_PROCESSOR_COUNT</c> does not override it, and a deployed
/// repo-context host was found where it did: 16 permits against a 6-CPU quota,
/// each permit admitting one CPU-bound whole-window WAL replay. The divergence
/// was undetectable from inside the process because the resolved ceiling was
/// never written down anywhere - the defect lived between a C# comment and a
/// container environment variable, two artefacts that never meet.
/// </para>
/// <para>
/// Deliberately <b>not</b> tested here: that the gate resolves to any particular
/// number. The remedy ruled on in #2279 is explicitly not to have library code
/// second-guess a documented, supported .NET override, so there is no new sizing
/// behaviour to assert. What is new is the record, and its load-bearing property
/// is that it can never itself break an activation.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainReplayGateObservabilityTests
{
    [Test]
    public void Resolved_gate_record_reports_the_ceiling_the_configured_option_and_the_processor_count()
    {
        var sink = new CapturingLogger();

        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 6, configured: 6, () => sink);

        Assert.That(sink.Entries, Has.Count.EqualTo(1),
            "sizing the gate must leave exactly one record; this line is the only in-process evidence "
            + "of the resolved ceiling, and it is emitted once per process");

        var rendered = sink.Entries[0];
        Assert.Multiple(() =>
        {
            // All three figures together, because any one alone is what made the
            // deployed divergence invisible: a ceiling with nothing to compare it
            // against cannot show that it disagrees with the container's quota.
            Assert.That(rendered, Does.Contain("6"),
                "the resolved ceiling must appear, since it is the number an operator is trying to read");
            Assert.That(rendered, Does.Contain("WalMaterialiserMaxConcurrentReplays"),
                "the configured option must be named, because pinning it is the entire sizing remedy "
                + "and a reader who cannot see its current value cannot tell whether it is already set");
            Assert.That(rendered, Does.Contain(Environment.ProcessorCount.ToString()),
                "Environment.ProcessorCount must be reported alongside the ceiling; the whole failure "
                + "mode is these two disagreeing with the container quota, which is unreadable from one "
                + "figure in isolation");
            Assert.That(rendered, Does.Contain("DOTNET_PROCESSOR_COUNT"),
                "the override must be named explicitly - it is the mechanism by which "
                + "Environment.ProcessorCount stops reflecting the cgroup quota, and the sentence it "
                + "replaces asserted the opposite without qualification");
        });
    }

    [Test]
    public void Resolved_gate_record_distinguishes_an_unset_option_from_a_pinned_one()
    {
        var unset = new CapturingLogger();
        var pinned = new CapturingLogger();

        // A non-positive option means "unset", and the ceiling then follows
        // Environment.ProcessorCount. Reporting the raw configured value rather
        // than only the resolved one is what lets a reader tell "nobody pinned
        // this" apart from "somebody pinned it to exactly the processor count",
        // which are different situations with different remedies.
        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 16, configured: 0, () => unset);
        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 16, configured: 16, () => pinned);

        Assert.That(unset.Entries[0], Is.Not.EqualTo(pinned.Entries[0]),
            "an unset option and one pinned to the same resolved ceiling must not render identically, "
            + "otherwise the record cannot answer the first question an operator asks of it");
    }

    [Test]
    public void Resolved_gate_record_is_skipped_when_information_is_disabled()
    {
        var sink = new CapturingLogger { Enabled = false };

        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 6, configured: 6, () => sink);

        Assert.That(sink.Entries, Is.Empty,
            "the record must respect the level filter rather than formatting unconditionally");
    }

    [Test]
    public void Resolved_gate_record_swallows_a_logger_resolution_fault()
    {
        // This is the property that matters most, and it is not defensive
        // padding: issue #2256 established on this exact path that a throwing
        // logging sink is a real environmental fault, and that a throw here once
        // leaked a replay permit permanently. Adding observability that can
        // itself fail an activation would trade a diagnosis problem for an
        // availability one.
        Assert.DoesNotThrow(
            () => BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
                max: 6,
                configured: 6,
                () => throw new InvalidOperationException("Injected logging-sink fault.")),
            "a faulting logger resolution must not escape the gate-sizing record; the caller is on the "
            + "leaf activation path, where an escaping exception is an activation failure");
    }

    [Test]
    public void Resolved_gate_record_swallows_a_fault_raised_while_writing()
    {
        // The fault can land on either side of the accessor: resolving the
        // logger, or the sink's own Log call. Both are the same environmental
        // class and both must be contained, so neither arm is asserted alone.
        var sink = new CapturingLogger { ThrowOnLog = true };

        Assert.DoesNotThrow(
            () => BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 6, configured: 6, () => sink),
            "a sink that throws while writing must not escape either");
    }

    [Test]
    public void Resolved_gate_record_tolerates_an_unavailable_logger()
    {
        // ResolveLogger yields null when no logger factory is registered, which
        // is the ordinary shape in a bare unit-test host rather than an error.
        Assert.DoesNotThrow(
            () => BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 6, configured: 6, () => null),
            "a null logger must be treated as 'nowhere to record it', not as a fault");
    }

    /// <summary>
    /// Minimal sink that renders each entry through the supplied formatter, so
    /// assertions read the message an operator would actually see rather than
    /// the unformatted template.
    /// </summary>
    private sealed class CapturingLogger : ILogger
    {
        public List<string> Entries { get; } = [];

        public bool Enabled { get; init; } = true;

        public bool ThrowOnLog { get; init; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => Enabled;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (ThrowOnLog)
                throw new InvalidOperationException("Injected sink write fault.");

            Entries.Add(formatter(state, exception));
        }
    }
}
