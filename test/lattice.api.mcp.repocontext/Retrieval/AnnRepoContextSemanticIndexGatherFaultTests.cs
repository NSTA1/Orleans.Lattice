using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests the fault classification issue #2749 was filed for: the exact-scan ladder
/// absorbed <see cref="ScanPageStalledException"/> and nothing else, and that type
/// is a <i>subclass</i> of the <see cref="TimeoutException"/> the deployment
/// actually raises.
/// <para>
/// <b>Why that mattered is not that the query failed.</b> It falls back to keyword
/// recall on either branch, so no caller sees an error. It is that
/// <see cref="RepoContextExactScanBreaker.Trip"/> is the only thing that grows the
/// probe delay, so a fault that propagated past the single catch left the breaker
/// pinned at its initial delay forever instead of doubling towards its ceiling. A
/// census over one deployment's whole uptime found <c>TimeoutException</c> ten
/// times, <c>OperationCanceledException</c> three, <c>OutOfMemoryException</c>
/// twice, and <c>ScanPageStalledException</c> not once - and that deployment's own
/// guard summary read "open for 00:21:43 across 1 stall(s) ... 13 half-open
/// probe(s)", where a correctly escalating backoff permits about four. Each of
/// those extra probes re-ran a full-prefix gather over the very tree the
/// approximate build was streaming, which is the contention the breaker exists to
/// remove.
/// </para>
/// <para>
/// <b>The fixture therefore asserts a VALUE, not a status.</b> "The breaker is
/// open" was already true before the fix, on the very first stall, and stayed true
/// forever - so a fixture that asserted only openness would have passed against
/// the defect. What distinguishes the fixed ladder is that the delay <i>grows</i>,
/// and the escalation tests below read
/// <see cref="RepoContextExactScanBreaker.ProbeDueIn"/> and compare it against the
/// doubling it is supposed to follow.
/// </para>
/// <para>
/// <b>The load-bearing constraint is the other half.</b> Absorbing more faults is
/// only safe because the predicate recognises timing and memory faults <i>only</i>.
/// A fault that says something about the index's contents must still propagate to
/// <c>keyword.index_degraded</c>, loud, or this change would trade a noisy breaker
/// for a silently broken index. Two tests below assert that direction, and they are
/// the ones that would redden if the predicate were ever widened to a bare
/// <c>catch (Exception)</c>.
/// </para>
/// </summary>
[TestFixture]
public sealed class AnnRepoContextSemanticIndexGatherFaultTests
{
    private const string RepoId = "acme";

    private static readonly TimeSpan ProbeDelay = TimeSpan.FromSeconds(60);

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 3, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static float[] Query => [1f, 0f, 0f];

    /// <summary>
    /// The positive control, run first on purpose. Every absorption test below
    /// concludes something from an empty result set, and an empty result set is
    /// also what a harness that can serve nothing produces. This establishes the
    /// wiring can carry a served answer to the caller before any absence is read as
    /// meaning.
    /// </summary>
    [Test]
    public async Task Control_a_gather_that_completes_is_served_through_the_index()
    {
        var exact = new FaultingGather { Fault = null };
        var index = Create(UncountedBootstrappingPlane(), exact, Breaker(new FaultClock()));

        var matches = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Is.Not.Empty,
                "If a healthy gather cannot reach the caller through this harness, every empty result below "
                + "measures the harness rather than the ladder and this whole fixture is vacuous.");
            Assert.That(exact.Searches, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// The defect itself, in its smallest form. A bare
    /// <see cref="TimeoutException"/> - the base type, which is what an unanswered
    /// grain call raises - must now open the breaker. Before the fix it propagated
    /// past the only catch and the breaker stayed closed, so the next query paid
    /// for the same gather again.
    /// </summary>
    [Test]
    public async Task A_bare_timeout_opens_the_breaker_although_it_is_not_the_stall_subclass()
    {
        var exact = new FaultingGather
        {
            Fault = () => new TimeoutException("Response did not arrive on time in 00:00:30."),
        };
        var breaker = Breaker(new FaultClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        var first = await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var second = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Empty, "Absorbed, so the caller gets the same no-matches answer a stall gives.");
            Assert.That(second, Is.Empty);
            Assert.That(breaker.IsTripped(RepoId), Is.True,
                "ScanPageStalledException derives from TimeoutException, so catching only the derived type "
                + "left the base type - the one the deployment actually raises - falling straight through.");
            Assert.That(exact.Searches, Is.EqualTo(1),
                "One gather, not two. The second query is suppressed rather than re-paying the fault, which "
                + "is the whole purpose of the breaker and is exactly what did not happen before.");
        });
    }

    /// <summary>
    /// A memory fault is capacity, not corruption, so it is absorbed too. Called
    /// out separately from the timeout because the two arrive by different routes -
    /// a timeout from a call that never answered, an allocation failure from this
    /// process's own heap - and a predicate that handled one and not the other
    /// would look correct from either test alone.
    /// </summary>
    [Test]
    public async Task An_allocation_failure_opens_the_breaker()
    {
        var exact = new FaultingGather { Fault = () => new OutOfMemoryException() };
        var breaker = Breaker(new FaultClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        var matches = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Is.Empty);
            Assert.That(breaker.IsTripped(RepoId), Is.True);
        });
    }

    /// <summary>
    /// The fault the predicate must reach through a wrapper. Orleans and the task
    /// machinery both deliver faults inside
    /// <see cref="AggregateException"/> or as an inner exception, and a predicate
    /// that only inspected the outermost type would classify a wrapped timeout as
    /// an index defect - which is the original bug's failure mode with an extra
    /// layer on it.
    /// </summary>
    [Test]
    public async Task A_timeout_wrapped_in_an_aggregate_is_still_recognised()
    {
        var exact = new FaultingGather
        {
            Fault = () => new AggregateException(
                new InvalidOperationException("outer"),
                new TimeoutException("inner")),
        };
        var breaker = Breaker(new FaultClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        var matches = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Is.Empty);
            Assert.That(breaker.IsTripped(RepoId), Is.True,
                "The transient fault is on the second branch of the aggregate and the first branch is an "
                + "integrity-shaped fault, so inspecting only the outermost type - or only the first inner - "
                + "would miss it.");
        });
    }

    /// <summary>
    /// <b>The value assertion, and the one that actually captures issue #2749.</b>
    /// Openness was already true before the fix and stayed true forever; what was
    /// broken is that the delay never grew, because only
    /// <see cref="RepoContextExactScanBreaker.Trip"/> grows it and non-stall faults
    /// never reached it. Each figure below is predicted exactly rather than
    /// asserted to be merely larger, because "larger" would also pass against a
    /// backoff that grew by one second.
    /// </summary>
    [Test]
    public async Task Repeated_timeouts_double_the_probe_delay_rather_than_pinning_it()
    {
        var exact = new FaultingGather { Fault = () => new TimeoutException("no answer") };
        var clock = new FaultClock();
        var breaker = Breaker(clock);
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var afterFirst = breaker.ProbeDueIn(RepoId);

        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var afterSecond = breaker.ProbeDueIn(RepoId);

        clock.Advance((ProbeDelay * 2) + TimeSpan.FromSeconds(1));
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);
        var afterThird = breaker.ProbeDueIn(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(ProbeDelay),
                "The first fault arms the initial delay.");
            Assert.That(afterSecond, Is.EqualTo(ProbeDelay * 2),
                "The second must DOUBLE it. This is the assertion the defect fails: before the fix the "
                + "probe's own timeout propagated past the catch, Stalls stayed at one, and this read 60 "
                + "seconds again - forever. Asserting merely that the breaker was open would have passed "
                + "against that, which is why the figure is predicted rather than compared.");
            Assert.That(afterThird, Is.EqualTo(ProbeDelay * 4),
                "And again, so the growth is geometric rather than a single one-off step.");
            Assert.That(exact.Searches, Is.EqualTo(3),
                "Three gathers for three windows. The deployment that produced this issue ran thirteen in "
                + "the time a correct backoff permits about four.");
        });
    }

    /// <summary>
    /// The load-bearing constraint, stated as a test. A fault about the index's
    /// contents is not capacity and must keep propagating, so the caller still
    /// learns the index is degraded rather than being told the plane is merely
    /// busy. This is the test that would redden if the new catch were ever widened
    /// into a bare <c>catch (Exception)</c>.
    /// </summary>
    [Test]
    public void An_index_integrity_fault_still_propagates_and_does_not_open_the_breaker()
    {
        var exact = new FaultingGather
        {
            Fault = () => new InvalidDataException("vector record 41 could not be deserialised"),
        };
        var breaker = Breaker(new FaultClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await index.SearchAsync(RepoId, Query, Space, 5, Ct),
                Throws.InstanceOf<InvalidDataException>(),
                "Absorbing this would trade a noisy breaker for a silently broken index, which is a worse "
                + "defect than the one being fixed.");
            Assert.That(breaker.IsTripped(RepoId), Is.False,
                "And it must not arm a backoff either: backing off would withhold the fallback from every "
                + "other caller because of a fault that retrying cannot clear.");
        });
    }

    /// <summary>
    /// The caller's own cancellation is not the plane's fault and must not arm a
    /// backoff. The breaker is shared by every caller of a repository, so one
    /// client walking away would otherwise withhold the exact fallback from all the
    /// rest - a denial of service reachable by any single caller with a short
    /// deadline.
    /// </summary>
    [Test]
    public void A_caller_that_cancels_does_not_arm_a_backoff_shared_with_every_other_caller()
    {
        using var caller = new CancellationTokenSource();
        var exact = new FaultingGather
        {
            Fault = () =>
            {
                caller.Cancel();
                return new OperationCanceledException(caller.Token);
            },
        };
        var breaker = Breaker(new FaultClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await index.SearchAsync(RepoId, Query, Space, 5, caller.Token),
                Throws.InstanceOf<OperationCanceledException>());
            Assert.That(breaker.IsTripped(RepoId), Is.False,
                "One caller's deadline must not become every caller's backoff.");
        });
    }

    /// <summary>
    /// The other side of the cancellation rule. A cancellation the caller did not
    /// ask for is a deadline this process owns, which is a capacity statement and is
    /// absorbed - so the two cancellations, identical in type, are separated by
    /// whose token is set rather than by the exception at all.
    /// </summary>
    [Test]
    public async Task A_cancellation_the_caller_did_not_ask_for_is_absorbed_as_capacity()
    {
        var exact = new FaultingGather
        {
            Fault = () => new OperationCanceledException("the gather's own budget elapsed"),
        };
        var breaker = Breaker(new FaultClock());
        var index = Create(UncountedBootstrappingPlane(), exact, breaker);

        var matches = await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(matches, Is.Empty);
            Assert.That(breaker.IsTripped(RepoId), Is.True);
        });
    }

    /// <summary>
    /// The census instrument. Issue #2749 had to be diagnosed by counting exception
    /// type names in a container's log, which is not a reading any deployment can be
    /// asked to produce, so the classification the ladder turns on is now exported.
    /// </summary>
    [Test]
    public async Task Each_fault_class_is_counted_on_its_own_arm_including_the_propagated_one()
    {
        using var faults = new GatherFaultMeasurements();
        var exact = new FaultingGather();
        var clock = new FaultClock();
        var index = Create(UncountedBootstrappingPlane(), exact, Breaker(clock));

        exact.Fault = () => new TimeoutException("no answer");
        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        exact.Fault = () => new InvalidDataException("record 41 is corrupt");
        Assert.That(
            async () => await index.SearchAsync("other", Query, Space, 5, Ct),
            Throws.InstanceOf<InvalidDataException>());

        Assert.Multiple(() =>
        {
            Assert.That(faults.Count(RepoContextExactGatherFault.TimedOutTag), Is.EqualTo(1));
            Assert.That(faults.Count(RepoContextExactGatherFault.PropagatedTag), Is.EqualTo(1),
                "The unabsorbed half is counted too. Recording only the absorbed faults would reproduce the "
                + "original defect one level out: 'no integrity faults occurred' and 'this instrument was "
                + "never wired' would again be the same observation.");
            Assert.That(faults.Count(RepoContextExactGatherFault.ExhaustedTag), Is.Zero,
                "A measured zero, not an absent series - see the priming assertion below.");
        });
    }

    /// <summary>
    /// The zero-priming rule. Four of the five arms are meant to read zero on a
    /// healthy deployment, so an arm that only appeared on its first occurrence
    /// would make the healthy case and the never-wired case identical - which is
    /// the ambiguity the whole epic exists to remove. The listener is started
    /// <i>before</i> the reporter is constructed, so it observes the priming itself
    /// rather than a later measurement.
    /// </summary>
    [Test]
    public void Every_fault_arm_is_minted_at_zero_when_the_reporter_is_constructed()
    {
        using var faults = new GatherFaultMeasurements();

        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            foreach (var arm in new[]
            {
                RepoContextExactGatherFault.StalledTag,
                RepoContextExactGatherFault.TimedOutTag,
                RepoContextExactGatherFault.ExhaustedTag,
                RepoContextExactGatherFault.AbandonedTag,
                RepoContextExactGatherFault.PropagatedTag,
            })
            {
                Assert.That(faults.Seen(arm), Is.True,
                    $"Arm '{arm}' must exist from construction. An absent arm and a zero arm are different "
                    + "observations, and only one of them is evidence.");
                Assert.That(faults.Count(arm), Is.Zero);
            }
        });
    }

    /// <summary>
    /// The operator-facing reading. The summary previously said "N stall(s)" while
    /// counting faults that were not stalls, and reported no fault census at all,
    /// so the line that is supposed to diagnose the state in one read was the thing
    /// asserting the wrong cause.
    /// </summary>
    [Test]
    public async Task The_summary_separates_absorbed_faults_from_propagated_ones()
    {
        using var logs = new CapturingLoggerProvider();
        var exact = new FaultingGather { Fault = () => new TimeoutException("no answer") };
        var index = Create(UncountedBootstrappingPlane(), exact, Breaker(new FaultClock()), logs);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        var summary = logs.Entries
            .Where(e => e.Level == LogLevel.Information)
            .Select(e => e.Message)
            .Last(m => m.Contains("retrieval-ladder guards", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(summary, Does.Contain("1 absorbed as capacity"));
            Assert.That(summary, Does.Contain("0 propagated as a degraded index"));
            Assert.That(summary, Does.Not.Contain("stall(s)"),
                "The breaker now opens on four fault classes, only one of which is a stall, so a line that "
                + "still said 'stall(s)' would be telling an operator the wrong cause with more confidence "
                + "than before - which is the exact failure this epic keeps finding.");
        });
    }

    /// <summary>
    /// Classification precedence, asserted directly. The ladder's behaviour tests
    /// above cannot distinguish <c>exhausted</c> from <c>timed_out</c> because both
    /// are absorbed identically, so the tag - which is the whole diagnostic value of
    /// the instrument - would otherwise be untested.
    /// </summary>
    [Test]
    public void Classification_puts_the_most_specific_reading_first()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextExactGatherFault.Classify(new OutOfMemoryException()),
                Is.EqualTo(RepoContextExactGatherFault.ExhaustedTag));
            Assert.That(
                RepoContextExactGatherFault.Classify(
                    new ScanPageStalledException("page fill ceiling exceeded")),
                Is.EqualTo(RepoContextExactGatherFault.StalledTag),
                "A stall is a TimeoutException, so classifying by the base type first would erase the one "
                + "fault class the ladder already knew how to name.");
            Assert.That(
                RepoContextExactGatherFault.Classify(new TimeoutException()),
                Is.EqualTo(RepoContextExactGatherFault.TimedOutTag));
            Assert.That(
                RepoContextExactGatherFault.Classify(new OperationCanceledException()),
                Is.EqualTo(RepoContextExactGatherFault.AbandonedTag));
            Assert.That(
                RepoContextExactGatherFault.Classify(new InvalidDataException()),
                Is.EqualTo(RepoContextExactGatherFault.PropagatedTag));
        });
    }

    private static IRepoContextAnnIndex UncountedBootstrappingPlane()
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RepoContextAnnSearchOutcome>(RepoContextAnnSearchOutcome.Bootstrapping));
        plane.KnownVectorCount(Arg.Any<string>()).Returns(0);
        return plane;
    }

    private static RepoContextExactScanBreaker Breaker(TimeProvider clock)
        => new(clock, ProbeDelay, TimeSpan.FromMinutes(15));

    private static AnnRepoContextSemanticIndex Create(
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        RepoContextExactScanBreaker breaker,
        CapturingLoggerProvider? logs = null)
        => new(
            plane,
            exact,
            RepoContextExactScanBudgets.Default(),
            breaker,
            new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero),
            logs is null
                ? Microsoft.Extensions.Logging.Abstractions.NullLogger<AnnRepoContextSemanticIndex>.Instance
                : new LoggerFactory([logs]).CreateLogger<AnnRepoContextSemanticIndex>());

    /// <summary>A clock the fixture advances by hand, so the timed exit is deterministic.</summary>
    private sealed class FaultClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    /// <summary>
    /// A gather whose fault is supplied per test. The factory shape matters: an
    /// exception instance reused across calls would carry one stack trace into
    /// several assertions, and one test needs to cancel a token at throw time.
    /// </summary>
    private sealed class FaultingGather : IRepoContextSemanticIndex
    {
        public Func<Exception>? Fault { get; set; }

        public int Searches { get; private set; }

        public string RetrievalPath => RepoContextRetrievalPath.SemanticExact;

        public Task<IReadOnlyList<RepoContextVectorMatch>> SearchAsync(
            string repoId,
            ReadOnlyMemory<float> query,
            EmbeddingSpaceTag querySpace,
            int k,
            CancellationToken cancellationToken)
        {
            Searches++;
            if (Fault is { } factory)
            {
                throw factory();
            }

            return Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                [new RepoContextVectorMatch("exact-0", "repo/acme/file/src/A.cs", 1d)]);
        }
    }

    /// <summary>
    /// Collects measurements on the gather-fault instrument, keyed by the fault
    /// tag, and records which arms were ever observed so a primed zero can be told
    /// apart from an absent series.
    /// </summary>
    private sealed class GatherFaultMeasurements : IDisposable
    {
        private readonly Dictionary<string, long> _byFault = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public GatherFaultMeasurements()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                    && instrument.Name == RepoContextRetrievalGuardReporter.ExactGatherFaultInstrumentName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key != RepoContextExactGatherFault.FaultTagKey
                        || tag.Value is not string fault)
                    {
                        continue;
                    }

                    lock (_byFault)
                    {
                        _byFault[fault] = _byFault.GetValueOrDefault(fault) + measurement;
                    }
                }
            });
            _listener.Start();
        }

        public long Count(string fault)
        {
            lock (_byFault)
            {
                return _byFault.GetValueOrDefault(fault);
            }
        }

        public bool Seen(string fault)
        {
            lock (_byFault)
            {
                return _byFault.ContainsKey(fault);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
