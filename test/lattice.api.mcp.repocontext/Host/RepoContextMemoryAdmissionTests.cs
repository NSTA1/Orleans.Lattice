using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the startup admission check added for issue #3255: the single seam that
/// decides whether this process accepts work at the managed-heap ceiling it has been
/// granted, before it accepts any.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two properties are load-bearing and everything here serves one of them.</b>
/// First, a refusal must rest only on a measurement this deployment recorded about
/// itself, never on a model - so the tests supply recorded observations and never a
/// predicted requirement, and there is no byte constant in the type under test to
/// assert against. Second, it must fail <i>open</i> at every other turn, because a
/// wrong refusal is an outage in a distroless container with no shell, while a wrong
/// admission costs exactly the crash-loop that exists today.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextMemoryAdmissionTests
{
    private const long NineGiB = 9_663_676_416;
    private const long ThirteenAndAHalfGiB = 14_495_514_624;
    private const string HistoryPath = "/data/heap-history.txt";

    private static readonly DateTimeOffset Observed = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private static RepoContextMemoryObservation Record(
        RepoContextMemoryOutcome outcome = RepoContextMemoryOutcome.Completed,
        long limit = NineGiB,
        long peak = 8_000_000_000,
        long events = 0,
        long? exhaustedAt = null) =>
        new(Observed, outcome, limit, peak, events, exhaustedAt, null);

    private static RepoContextMemoryAdmissionDecision Evaluate(
        long granted,
        RepoContextMemoryObservation? previous,
        string? overrideValue = null) =>
        RepoContextMemoryAdmission.Evaluate(granted, previous, overrideValue, HistoryPath);

    [Test]
    public void A_grant_no_larger_than_a_recorded_exhaustion_is_refused()
    {
        // The whole point of the change, stated as one comparison: this corpus on
        // this host has already been measured running out of managed heap at this
        // much, and it has not been given any more.
        var decision = Evaluate(NineGiB, Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB));

        Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Refuse));
    }

    [Test]
    public void A_grant_below_a_recorded_exhaustion_is_refused()
    {
        var decision = Evaluate(
            4_000_000_000,
            Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB));

        Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Refuse));
    }

    [Test]
    public void A_refusal_names_both_numbers_and_the_evidence_it_rests_on()
    {
        // The failure this replaces named neither number: it surfaced as a STORAGE
        // error reading grain state, which reads as flakiness rather than as
        // insufficiency. A refusal that only said "not enough memory" would be a
        // smaller version of the same defect.
        var decision = Evaluate(
            4_000_000_000,
            Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Message, Does.Contain("4000000000"), "the granted ceiling");
            Assert.That(decision.Message, Does.Contain(NineGiB.ToString()), "the recorded ceiling");
            Assert.That(decision.Message, Does.Contain(HistoryPath), "where the evidence is");
            Assert.That(
                decision.Message,
                Does.Contain(RepoContextMemoryAdmission.OverrideKey),
                "how to start anyway");
        });
    }

    [Test]
    public void A_refusal_tells_the_operator_the_exact_override_value_to_set()
    {
        // The image is distroless, so an operator cannot open a shell and read the
        // recorded ceiling out of the file. If the refusal does not carry the number,
        // the escape hatch is undiscoverable and the refusal is a brick.
        var decision = Evaluate(NineGiB, Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB));

        Assert.That(
            decision.Message,
            Does.Contain(RepoContextMemoryAdmission.OverrideKey + "=" + NineGiB.ToString()));
    }

    [Test]
    public void A_grant_larger_than_the_recorded_exhaustion_is_admitted()
    {
        var decision = Evaluate(
            ThirteenAndAHalfGiB,
            Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB, peak: NineGiB));

        Assert.That(decision.Verdict, Is.Not.EqualTo(RepoContextMemoryVerdict.Refuse));
    }

    [Test]
    public void An_exhausted_outcome_without_a_recorded_ceiling_cannot_refuse()
    {
        // The refusal keys on the carried high-water ceiling, never on the outcome
        // field, because only the ceiling says how much memory the exhaustion
        // happened at. An outcome alone is not a comparison.
        var decision = Evaluate(NineGiB, Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: null));

        Assert.That(decision.Verdict, Is.Not.EqualTo(RepoContextMemoryVerdict.Refuse));
    }

    [Test]
    public void An_override_matching_the_recorded_ceiling_exactly_suppresses_the_refusal()
    {
        var decision = Evaluate(
            NineGiB,
            Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB),
            NineGiB.ToString());

        Assert.Multiple(() =>
        {
            Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Warn));
            Assert.That(decision.HonouredOverrideBytes, Is.EqualTo(NineGiB));
            Assert.That(decision.Message, Does.Contain("OVERRIDDEN"));
        });
    }

    [TestCase("true")]
    [TestCase("1")]
    [TestCase("yes")]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("9663676415")]
    [TestCase("9663676417")]
    public void An_override_that_is_not_the_exact_recorded_ceiling_does_not_suppress_the_refusal(string value)
    {
        // Echoing the exact number is what stops this becoming a flag somebody sets
        // once and forgets, suppressing correct refusals for ever after. "true" and
        // "1" are the two values a hurried operator will try first, and both must
        // fail.
        var decision = Evaluate(
            NineGiB,
            Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB),
            value);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Refuse));
            Assert.That(decision.HonouredOverrideBytes, Is.Null);
        });
    }

    [Test]
    public void An_override_stops_matching_once_a_larger_exhaustion_is_recorded()
    {
        // Self-invalidation: the override covers the evidence the operator actually
        // read, not all future evidence. A deployment that goes on to exhaust at a
        // higher ceiling refuses again, with the stale override still set.
        const string StaleOverride = "9663676416";

        var decision = Evaluate(
            ThirteenAndAHalfGiB,
            Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: ThirteenAndAHalfGiB),
            StaleOverride);

        Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Refuse));
    }

    [Test]
    public void An_override_is_only_recorded_when_it_actually_suppressed_a_refusal()
    {
        // Otherwise the history would record the grant as disbelieved on every run
        // where the variable happened to be set, including runs that never needed it,
        // and a later reader could not tell which.
        var decision = Evaluate(ThirteenAndAHalfGiB, Record(), NineGiB.ToString());

        Assert.That(decision.HonouredOverrideBytes, Is.Null);
    }

    [Test]
    public void No_record_admits()
    {
        var decision = Evaluate(NineGiB, null);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Admit));
            Assert.That(decision.Message, Does.Contain(HistoryPath));
        });
    }

    [Test]
    public void A_runtime_reporting_no_usable_ceiling_admits()
    {
        // A check that cannot read its own input must never be the reason a container
        // fails to start. Both directions of "unusable" are pinned because a negative
        // reading would otherwise compare as smaller than every recorded ceiling and
        // refuse everything.
        Assert.Multiple(() =>
        {
            Assert.That(
                Evaluate(0, Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB)).Verdict,
                Is.EqualTo(RepoContextMemoryVerdict.Admit));
            Assert.That(
                Evaluate(-1, Record(RepoContextMemoryOutcome.Exhausted, exhaustedAt: NineGiB)).Verdict,
                Is.EqualTo(RepoContextMemoryVerdict.Admit));
        });
    }

    [Test]
    public void A_previous_peak_that_does_not_fit_the_new_ceiling_warns_but_admits()
    {
        // Deliberately a warning and not a refusal. Committed bytes is a measured
        // commitment, not a measured requirement: a heap collects more eagerly the
        // closer it sits to its ceiling, so a process given less may well commit
        // less. Refusing on it would refuse every grant decrease.
        var decision = Evaluate(NineGiB, Record(peak: ThirteenAndAHalfGiB, limit: ThirteenAndAHalfGiB));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Warn));
            Assert.That(decision.Message, Does.Contain("peak"));
        });
    }

    [Test]
    public void A_previous_peak_that_still_fits_the_new_ceiling_does_not_warn()
    {
        var decision = Evaluate(ThirteenAndAHalfGiB, Record(peak: 8_000_000_000, limit: NineGiB));

        Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Admit));
    }

    [Test]
    public void A_previous_run_that_never_stopped_cleanly_warns_and_names_the_kill_it_cannot_see()
    {
        // A surviving Admitted marker is the only residue a cgroup out-of-memory kill
        // leaves inside this container, because SIGKILL raises no exception and gives
        // nothing a chance to record one. It is ambiguous - a host reboot leaves the
        // same - so it can only warn. It is kept because that blind spot must be
        // visible somewhere rather than presenting as a clean record.
        var decision = Evaluate(ThirteenAndAHalfGiB, Record(RepoContextMemoryOutcome.Admitted));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Warn));
            Assert.That(decision.Message, Does.Contain("out-of-memory kill"));
        });
    }

    [Test]
    public void A_recorded_exhaustion_outranks_the_softer_warnings()
    {
        // Ordering matters: a record that would both refuse and warn must refuse. An
        // exhausted run has normally also peaked above any smaller new ceiling, so if
        // the warning were evaluated first the refusal would never fire.
        var decision = Evaluate(
            NineGiB,
            Record(
                RepoContextMemoryOutcome.Exhausted,
                limit: ThirteenAndAHalfGiB,
                peak: ThirteenAndAHalfGiB,
                exhaustedAt: ThirteenAndAHalfGiB));

        Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Refuse));
    }

    [Test]
    public void A_clean_previous_run_within_the_new_ceiling_admits_and_reports_what_it_measured()
    {
        var decision = Evaluate(ThirteenAndAHalfGiB, Record(peak: 8_000_000_000, limit: NineGiB));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Verdict, Is.EqualTo(RepoContextMemoryVerdict.Admit));
            Assert.That(decision.Message, Does.Contain("8000000000"));
            Assert.That(decision.Message, Does.Contain("82"), "the occupancy percentage");
        });
    }

    [Test]
    public void A_null_history_path_is_rejected_rather_than_producing_a_message_without_one()
    {
        Assert.That(
            () => RepoContextMemoryAdmission.Evaluate(NineGiB, null, null, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void The_override_key_is_specific_enough_that_it_cannot_be_set_by_accident()
    {
        // Pinned because the name is the contract an operator reads out of a refusal
        // message and types into a compose file; renaming it silently strands every
        // deployment that already carries it.
        Assert.That(
            RepoContextMemoryAdmission.OverrideKey,
            Is.EqualTo("LATTICE_REPOCONTEXT_HEAP_ADMISSION_OVERRIDE"));
    }
}
