using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the resolved-replay-gate startup record and for the default
/// ceiling it reports (issues #2278, #2279, #2816).
/// <para>
/// The replay concurrency gate used to default to
/// <see cref="Environment.ProcessorCount"/> alone, and a comment on that path
/// asserted flatly that this "honours a container CPU quota". It does so only
/// while <c>DOTNET_PROCESSOR_COUNT</c> does not override it, and a deployed
/// repo-context host was found where it did: 16 permits against a 6-CPU quota,
/// each permit admitting one CPU-bound whole-window WAL replay. The divergence
/// was undetectable from inside the process because the resolved ceiling was
/// never written down anywhere - the defect lived between a C# comment and a
/// container environment variable, two artefacts that never meet.
/// </para>
/// <para>
/// Issue #2279 ruled that library code must not second-guess that override, so
/// the first remedy shipped was the record alone. Issue #2816 overturned the
/// sizing half of that ruling, and the fixture now asserts the sizing behaviour
/// the earlier revision of this comment said would never exist. The reversal
/// turns on <b>minimum, not replacement</b>: the default takes the lesser of the
/// runtime figure and the enforced cgroup grant, so it can only ever lower the
/// ceiling. An operator lowering <c>DOTNET_PROCESSOR_COUNT</c> is still obeyed
/// exactly; what the gate refuses is to admit more concurrent CPU-bound replays
/// than the kernel will schedule in parallel, which is a different question from
/// how many threads the process should run. The tests below pin both halves: the
/// arithmetic in
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain.ResolveDefaultReplayCeiling"/>,
/// and the record's load-bearing property that it can never itself break an
/// activation.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainReplayGateObservabilityTests
{
    [Test]
    public void Default_ceiling_is_capped_by_the_enforced_container_cpu_grant()
    {
        // The measured deployed case: DOTNET_PROCESSOR_COUNT=16 on a 6.0-CPU
        // container grant, which sized the gate at 2.67x the CPU the process
        // could obtain. The grant must win, because it is the only one of the two
        // figures the kernel actually enforces.
        Assert.That(
            BPlusLeafGrain.ResolveDefaultReplayCeiling(processorCount: 16, containerCpuGrant: 6),
            Is.EqualTo(6),
            "an overridden processor count above the enforced grant must not size this gate; the "
            + "permits it hands out are concurrent CPU-bound whole-window replays");
    }

    [Test]
    public void Default_ceiling_falls_back_to_the_processor_count_when_no_grant_is_readable()
    {
        // Null means UNKNOWN, not zero: an unconstrained host, a non-Linux host,
        // and an unreadable cgroup are all cases where nothing is being enforced,
        // so nothing should be constrained. Treating null as zero here would
        // deadlock every leaf activation in the silo on a SemaphoreSlim(0, 0).
        Assert.That(
            BPlusLeafGrain.ResolveDefaultReplayCeiling(processorCount: 16, containerCpuGrant: null),
            Is.EqualTo(16),
            "an unreadable or unlimited quota must impose no constraint at all");
    }

    [Test]
    public void Default_ceiling_keeps_the_processor_count_when_it_is_already_below_the_grant()
    {
        // The minimum is symmetric, and this is the arm that shows it never
        // RAISES the ceiling - which is the whole basis on which #2816 overturned
        // #2279. A host that deliberately lowers DOTNET_PROCESSOR_COUNT under a
        // generous grant is still obeyed to the letter.
        Assert.That(
            BPlusLeafGrain.ResolveDefaultReplayCeiling(processorCount: 6, containerCpuGrant: 16),
            Is.EqualTo(6),
            "a deliberately lowered processor count must still be honoured; taking the minimum must "
            + "never raise the ceiling above the runtime figure");
    }

    [Test]
    public void Default_ceiling_from_a_fractional_grant_is_at_least_one_permit()
    {
        // Acceptance item 3 of #2816, pinned end to end at its source rather than
        // defended by a second floor in the resolver. A 0.01-CPU grant is the
        // smallest thing Docker will express; the reader's own ceiling rounding
        // turns it into 1, so the resolver cannot produce a zero-permit gate
        // without that contract changing first. A Math.Max(1, ...) in the
        // resolver would be unreachable, and an unreachable safety net is worse
        // than none because it reads as protection.
        var grant = Orleans.Lattice.Runtime.ContainerCpuGrant.ParseCpuQuota("1000", "100000");

        Assert.Multiple(() =>
        {
            Assert.That(grant, Is.EqualTo(1),
                "the reader rounds a fractional grant up, so the smallest expressible grant is one CPU");
            Assert.That(
                BPlusLeafGrain.ResolveDefaultReplayCeiling(processorCount: 16, containerCpuGrant: grant),
                Is.EqualTo(1),
                "a fractional grant must resolve to exactly one permit, never zero");
        });
    }

    [Test]
    public void Gate_sizing_prefers_an_explicitly_configured_ceiling_over_both_derived_figures()
    {
        // The operator's pin still wins. A minimum that could override an explicit
        // setting would be exactly the "library defeats the operator" objection
        // #2279 raised, and it would be a fair one.
        var sizing = BPlusLeafGrain.ResolveGateSizing(
            configured: 3, processorCount: 16, containerCpuGrant: 6);

        Assert.That(sizing.Ceiling, Is.EqualTo(3),
            "a positive configured ceiling supersedes both the processor count and the grant");
    }

    [Test]
    public void Gate_sizing_reports_the_container_cpu_grant_even_when_a_pin_supersedes_it()
    {
        // The diagnostic case that motivates returning the grant rather than
        // reading it at the call site: the operator pinned 3 on a 6-CPU grant with
        // the runtime claiming 16, and all three numbers are worth writing down.
        // Asserting on 6 rather than on the ceiling is what makes this falsifiable -
        // the ceiling here is 3, so a resolver that dropped the grant entirely would
        // still satisfy an assertion phrased against the ceiling.
        var sizing = BPlusLeafGrain.ResolveGateSizing(
            configured: 3, processorCount: 16, containerCpuGrant: 6);

        Assert.That(sizing.ContainerCpuGrant, Is.EqualTo(6),
            "the grant must be reported whether or not it constrained the ceiling, because a pin "
            + "that disagrees with the grant is the case an operator most needs to see");
    }

    [Test]
    public void Gate_sizing_derives_the_ceiling_when_the_option_is_left_unset()
    {
        var sizing = BPlusLeafGrain.ResolveGateSizing(
            configured: 0, processorCount: 16, containerCpuGrant: 6);

        Assert.Multiple(() =>
        {
            Assert.That(sizing.Ceiling, Is.EqualTo(6),
                "an unset option must fall through to the lesser of the two derived figures");
            Assert.That(sizing.ContainerCpuGrant, Is.EqualTo(6),
                "and the grant it resolved against must still be reported");
        });
    }

    [Test]
    public void Resolved_gate_record_reports_the_ceiling_the_configured_option_and_the_processor_count()
    {
        var sink = new CapturingLogger();

        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
            max: 6, configured: 6, containerCpuGrant: 6, () => sink);

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
        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 16, configured: 0, containerCpuGrant: 16, () => unset);
        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(max: 16, configured: 16, containerCpuGrant: 16, () => pinned);

        Assert.That(unset.Entries[0], Is.Not.EqualTo(pinned.Entries[0]),
            "an unset option and one pinned to the same resolved ceiling must not render identically, "
            + "otherwise the record cannot answer the first question an operator asks of it");
    }

    [Test]
    public void Resolved_gate_record_reports_the_container_cpu_grant_it_resolved_against()
    {
        // Acceptance item 5 of #2816. The grant is the third figure, and it is
        // the one that was missing: a reader could previously see the ceiling and
        // the processor count disagree with the container's quota only by going
        // and reading the quota out of band.
        var sink = new CapturingLogger();

        // 37 is deliberately a value that appears nowhere else in the record, so
        // the assertion cannot be satisfied by the ceiling or the processor count
        // happening to render the same digits.
        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
            max: 6, configured: 0, containerCpuGrant: 37, () => sink);

        Assert.That(sink.Entries[0], Does.Contain("37"),
            "the quota-derived grant must be written down, because the whole failure mode is the "
            + "runtime figure and the enforced grant disagreeing");
    }

    [Test]
    public void Resolved_gate_record_distinguishes_an_unreadable_grant_from_a_readable_one()
    {
        var readable = new CapturingLogger();
        var unreadable = new CapturingLogger();

        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
            max: 6, configured: 0, containerCpuGrant: 6, () => readable);
        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
            max: 6, configured: 0, containerCpuGrant: null, () => unreadable);

        Assert.Multiple(() =>
        {
            Assert.That(unreadable.Entries[0], Does.Contain("unreadable or unlimited"),
                "a null grant must render as an explicit statement that nothing was enforced, not as "
                + "an empty slot a reader would mistake for a zero or for a formatting fault");
            Assert.That(readable.Entries[0], Does.Not.Contain("unreadable or unlimited"),
                "a grant that was read must not render as though it had not been");
        });
    }

    [Test]
    public void Resolved_gate_record_is_skipped_when_information_is_disabled()
    {
        var sink = new CapturingLogger { Enabled = false };

        BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
            max: 6, configured: 6, containerCpuGrant: 6, () => sink);

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
                containerCpuGrant: 6,
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
            () => BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
                max: 6, configured: 6, containerCpuGrant: 6, () => sink),
            "a sink that throws while writing must not escape either");
    }

    [Test]
    public void Resolved_gate_record_tolerates_an_unavailable_logger()
    {
        // ResolveLogger yields null when no logger factory is registered, which
        // is the ordinary shape in a bare unit-test host rather than an error.
        Assert.DoesNotThrow(
            () => BPlusLeafGrain.LogResolvedReplayConcurrencyGate(
                max: 6, configured: 6, containerCpuGrant: 6, () => null),
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
