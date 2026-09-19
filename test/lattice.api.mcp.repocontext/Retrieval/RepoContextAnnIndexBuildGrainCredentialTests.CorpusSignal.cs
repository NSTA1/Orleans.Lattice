using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The second half of the issue #2426 remedy: a corpus read that the access gate
/// refused must announce itself on a series, and must not be banked as a converged
/// index.
/// <para>
/// <b>Why a series and not a log line.</b> The credential half of #2426 was merged
/// and shipped, and the failure remained a live hypothesis through two full
/// deployment gates anyway, because a denial on this path produced no
/// distinguishable signal at all - it looked exactly like a slow or wedged build.
/// This bucket also has direct evidence of a warning line documenting a blindness
/// being ignored for three hours while a counter on the dashboard read success.
/// Prose that contradicts a metric loses whenever only one of the two is being
/// watched, so the remedy has to be measurable.
/// </para>
/// <para>
/// <b>Why the zero arm matters as much as the positive one.</b> An assertion that
/// can only fire positively cannot distinguish "the change landed" from "the check
/// is broken", so every positive here is paired with the reading the same series
/// gives on a correctly configured host.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
{
    /// <summary>
    /// Ticks to deliver when the build is expected never to converge. Comfortably
    /// past <see cref="RepoContextAnnIndexBuildGrain.TerminalDenialThreshold"/>
    /// once the backoff between attempts is accounted for.
    /// </summary>
    private const int DeniedTicks = 128;

    [Test]
    public async Task A_denied_corpus_is_counted_and_refused_convergence()
    {
        // THE DETECTOR. A default-deny host that has not admitted this build's
        // subject answers the corpus read with a reject-all key filter, so the
        // build reaches Ready holding nothing. The coordinator must classify that
        // emptiness before acting on it, count what it found, and decline to bank a
        // converged index on a read that never happened.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Denied);
        using var rig = new Rig(new NullRepoIndexRunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        await rig.PumpTicksAsync(DeniedTicks);
        var signal = rig.Reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Calls, Is.GreaterThan(0),
                "positive control: the coordinator must actually have probed, or this fixture is "
                + "asserting against a signal nothing produced");
            Assert.That(signal.Denied, Is.GreaterThan(0),
                "a refused corpus read must advance the denied arm - this is the series whose absence "
                + "left #2426 a live hypothesis through two deployment gates");
            Assert.That(rig.State.State.Converged, Is.False,
                "a read that did not happen leaves the index state UNKNOWN, not empty; banking "
                + "Converged on it is the fail-open-into-silence the issue exists to remove");
            Assert.That(signal.Unrestricted, Is.Zero,
                "a denial must not be misfiled as an honest empty repository");
            Assert.That(signal.NonEmpty, Is.Zero,
                "no build held vectors, so the non-empty arm must not have advanced");
        });
    }

    [Test]
    public async Task A_correctly_configured_host_reads_zero_on_the_denial_arm()
    {
        // THE PAIRED NEGATIVE, AND THE VERIFICATION THAT THE MERGED CREDENTIAL PATH
        // HOLDS UNDER DEFAULT DENY.
        //
        // Identical to the fixture above in every respect except the one that
        // matters: the run authority stamps the subject the gate has admitted, so
        // the corpus read succeeds. If the credential half of #2426 had regressed,
        // this build would read empty and the denied arm would advance - so this
        // test is a live check on the merged remedy, not a restatement of it.
        //
        // Asserting the denied arm is EXACTLY ZERO is the point. A test that only
        // ever asserts a count rising cannot tell a landed change from a broken
        // detector.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Denied);
        using var rig = new Rig(RunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var signal = rig.Reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "the index must converge");
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(64),
                "positive control: the credentialed build must actually have read the corpus the "
                + "store of record holds, or the zero below would be vacuous");
            Assert.That(signal.Denied, Is.Zero,
                "MEASURED ZERO: a correctly configured host must report the denial arm at exactly "
                + "zero. The probe is deliberately wired to answer 'denied' if it is ever asked, so "
                + "a non-zero reading here means the coordinator probed a build that held vectors");
            Assert.That(probe.Calls, Is.Zero,
                "the coverage probe is a cold path taken only when a build completes holding "
                + "nothing, so an ordinary build must never pay for it");
            Assert.That(signal.NonEmpty, Is.EqualTo(1),
                "the total must advance on an ordinary build, because that is what makes the zero "
                + "above a measured absence rather than silence");
        });
    }

    [Test]
    public async Task An_honestly_empty_repository_still_converges()
    {
        // The guard must not turn a legitimate empty state into a permanent
        // non-convergence. A fresh repository with nothing indexed yet reads empty
        // against an unrestricted prefix, and that is a correct, complete read.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Unrestricted);
        using var rig = new Rig(RunAuthority(), probe);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var signal = rig.Reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "an honestly empty repository must still converge, or a fresh deployment would never "
                + "settle and would retry forever");
            Assert.That(rig.State.State.Converged, Is.True);
            Assert.That(rig.State.State.VectorsIndexed, Is.Zero);
            Assert.That(signal.Unrestricted, Is.GreaterThan(0),
                "the emptiness is recorded as attributed rather than unexplained");
            Assert.That(signal.Denied, Is.Zero,
                "MEASURED ZERO: an honest empty store is not a denial");
        });
    }

    [Test]
    public async Task A_filtered_prefix_converges_on_the_subset_it_was_shown()
    {
        // Denied and Filtered are not the same event. Filtered means the authority
        // resolved correctly and the gate legitimately returned a subset, which is
        // a complete and correct read of what this caller may see - and therefore a
        // legitimate converged state. Refusing here would permanently wedge any
        // host that legitimately restricts content, which is a far larger harm than
        // the one being prevented. Converge on a known subset; never on an unknown.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Filtered);
        using var rig = new Rig(new NullRepoIndexRunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var signal = rig.Reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "a filtered prefix is a correct read and converges");
            Assert.That(rig.State.State.Converged, Is.True);
            Assert.That(signal.Filtered, Is.GreaterThan(0),
                "the restriction is still recorded, so an operator can see that the index covers a "
                + "subset rather than the whole repository");
            Assert.That(signal.Denied, Is.Zero,
                "MEASURED ZERO: a filtered read must not be counted as a denial");
        });
    }

    [Test]
    public async Task An_unanswerable_probe_is_classified_unknown_and_never_as_permitted()
    {
        // A probe that cannot answer is not permission granted. It is counted on
        // its own arm and withholds convergence, exactly as a denial does, up to
        // the terminal threshold - after which the build converges anyway, because
        // a diagnostic that can wedge the pipeline it observes inverts the blast
        // radius it exists to reduce. Bounded and loud beats unbounded and
        // safe-looking.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Unrestricted, throws: true);
        using var rig = new Rig(new NullRepoIndexRunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var signal = rig.Reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(signal.Unknown, Is.GreaterThan(0),
                "an unanswerable probe is counted, not swallowed");
            Assert.That(signal.Unrestricted, Is.Zero,
                "MEASURED ZERO: a probe that failed must never be filed as an unrestricted prefix - "
                + "that would be exactly the fail-open the guard exists to prevent");
            Assert.That(
                signal.Unknown,
                Is.GreaterThanOrEqualTo(RepoContextAnnIndexBuildGrain.TerminalDenialThreshold),
                "convergence is withheld while the question is open, so the coordinator retries "
                + "rather than banking an index it cannot vouch for");
            Assert.That(signal.TerminalDenials, Is.EqualTo(1),
                "the episode is escalated once, so an operator sees that the plane is not merely slow");
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "and then it converges: an unanswerable probe must bound the outage rather than "
                + "wedge the build forever");
        });
    }

    [Test]
    public async Task A_persistent_denial_backs_off_instead_of_retrying_every_tick()
    {
        // PM condition: refusing to converge must not become a hot loop. The gate
        // container this line is diagnosed against already runs at 340% CPU and
        // 94.6% of its memory limit, so a two-second retry is not a harmless
        // inefficiency there. A backed-off tick must take no build step and no
        // probe at all.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Denied);
        using var rig = new Rig(new NullRepoIndexRunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        await rig.PumpTicksAsync(DeniedTicks);
        var signal = rig.Reporter.Read();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Calls, Is.GreaterThan(0),
                "positive control: the coordinator must have attempted at least once, or 'it backed "
                + "off' would be indistinguishable from 'it never ran'");
            Assert.That(probe.Calls, Is.LessThan(DeniedTicks / 4),
                "a persistent denial must back off sharply rather than retry on the phase cadence");
            Assert.That(signal.Denied, Is.EqualTo(probe.Calls),
                "every attempt that was actually made is counted, and no skipped tick is");
            Assert.That(signal.TerminalDenials, Is.EqualTo(1),
                "the episode escalates exactly once, so a permanently refused deployment is loudly "
                + "parked rather than quietly spinning");
        });
    }

    [Test]
    public void The_backoff_doubles_and_is_capped()
    {
        // The schedule itself, asserted directly so the shape is pinned rather than
        // inferred from a tick count. At the two-second phase period the cap parks a
        // refused coordinator at roughly one attempt every five minutes.
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextAnnIndexBuildGrain.ComputeDenialSkipTicks(1), Is.EqualTo(1));
            Assert.That(RepoContextAnnIndexBuildGrain.ComputeDenialSkipTicks(2), Is.EqualTo(3));
            Assert.That(RepoContextAnnIndexBuildGrain.ComputeDenialSkipTicks(3), Is.EqualTo(7));
            Assert.That(RepoContextAnnIndexBuildGrain.ComputeDenialSkipTicks(4), Is.EqualTo(15));
            Assert.That(RepoContextAnnIndexBuildGrain.ComputeDenialSkipTicks(5), Is.EqualTo(31));
            Assert.That(
                RepoContextAnnIndexBuildGrain.ComputeDenialSkipTicks(64),
                Is.EqualTo(RepoContextAnnIndexBuildGrain.MaxDenialSkipTicks),
                "an unbounded run must settle at the cap rather than growing without limit");
            Assert.That(
                RepoContextAnnIndexBuildGrain.MaxDenialSkipTicks,
                Is.LessThan(int.MaxValue / 2),
                "the cap must remain a real bound, so a long episode cannot overflow into a "
                + "coordinator that never retries again");
        });
    }

    [Test]
    public async Task A_denial_that_clears_converges_without_a_restart()
    {
        // The reason the coordinator backs off rather than standing down. The
        // container seeds its access grant on ApplicationStarted with backoff
        // retry, while this coordinator's phase timer fires with dueTime zero, so a
        // build step can genuinely run before the grant lands. Standing down there
        // would make a transient startup race permanent until a restart.
        var probe = new RecoveringCorpusGateProbe();
        using var rig = new Rig(RunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        // Deny the corpus for the opening ticks, exactly as an unseeded grant would.
        rig.Backing.Gate(RepoId, Space).Denied = true;
        await rig.PumpTicksAsync(8);
        var duringOutage = rig.Reporter.Read();

        // The grant lands.
        rig.Backing.Gate(RepoId, Space).Denied = false;
        probe.Coverage = RepoContextAnnBuildCorpusCoverage.Unrestricted;
        var converged = false;
        for (var tick = 0; tick < MaxTicks && !converged; tick++)
        {
            await rig.Grain.ProcessNextPhaseAsync();
            converged = await rig.Grain.IsConvergedAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(duringOutage.Denied, Is.GreaterThan(0),
                "positive control: the outage must actually have been observed, or the recovery "
                + "below proves nothing");
            Assert.That(converged, Is.True,
                "a grant that seeds late must be picked up on the existing coordinator, with no "
                + "restart and no operator action");
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(64),
                "and the index it finally banks must hold the real corpus, not the empty one it "
                + "refused earlier");
        });
    }

    [Test]
    public void Every_series_is_minted_before_any_build_runs()
    {
        // An absent series and a series reading zero look identical on a dashboard
        // and are very different claims. Pre-minting makes 'denied = 0' a
        // falsifiable statement about a host rather than an artefact of nothing
        // having happened yet.
        using var reporter = new RepoContextAnnBuildCorpusReporter();
        var listener = new List<string>();
        using var meterListener = new System.Diagnostics.Metrics.MeterListener();
        meterListener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name is RepoContextAnnBuildCorpusReporter.CorpusInstrumentName
                or RepoContextAnnBuildCorpusReporter.TerminalDenialInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextAnnBuildCorpusReporter.CoverageTagKey)
                {
                    listener.Add($"{instrument.Name}|{tag.Value}");
                    return;
                }
            }

            listener.Add(instrument.Name);
        });
        meterListener.Start();

        // The instruments already exist, so re-minting is what a subscriber that
        // attached after start-up would observe. The production reporter mints on
        // construction; this asserts the same set is emitted with no build at all.
        using var observed = new RepoContextAnnBuildCorpusReporter();

        Assert.Multiple(() =>
        {
            Assert.That(listener, Is.Not.Empty,
                "positive control: the listener must have received measurements, or the assertions "
                + "below would pass on an empty collection");
            Assert.That(
                listener,
                Does.Contain(
                    $"{RepoContextAnnBuildCorpusReporter.CorpusInstrumentName}"
                    + $"|{RepoContextAnnBuildCorpusReporter.CoverageDeniedTag}"),
                "the denial arm must exist before any build runs, so a healthy host reports it at "
                + "zero rather than omitting it");
            Assert.That(
                listener,
                Does.Contain(RepoContextAnnBuildCorpusReporter.TerminalDenialInstrumentName),
                "and so must the terminal arm");
            Assert.That(observed.Read().Denied, Is.Zero,
                "minting must not fabricate a count");
        });
    }

    [Test]
    public void The_coverage_tag_set_is_closed()
    {
        // Cardinality and disclosure. The tag is a classification, never the
        // withheld keys, and an unrecognised value must fail closed onto 'unknown'
        // rather than onto a permissive arm.
        var described = Enum.GetValues<RepoContextAnnBuildCorpusCoverage>()
            .Select(RepoContextAnnBuildCorpusReporter.DescribeCoverage)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(described, Is.Unique, "each coverage class must have its own arm");
            Assert.That(
                RepoContextAnnBuildCorpusReporter.DescribeCoverage((RepoContextAnnBuildCorpusCoverage)9_999),
                Is.EqualTo(RepoContextAnnBuildCorpusReporter.CoverageUnknownTag),
                "an unrecognised class must fail closed onto 'unknown', never onto 'unrestricted'");
        });
    }

    [Test]
    public async Task A_host_with_no_gate_pays_nothing_for_the_probe()
    {
        // Specificity guard. The overwhelming majority of builds hold vectors, and
        // those must not acquire a grain call they did not have before. The probe is
        // cold-path only.
        var probe = new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Unrestricted);
        using var rig = new Rig(RunAuthority(), probe);
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        await rig.PumpAsync();

        Assert.That(probe.Calls, Is.Zero,
            "a build that held vectors needs no classification, so the streaming path pays nothing");
    }

    /// <summary>
    /// A probe whose answer can be changed mid-test, so a denial that clears can be
    /// exercised on one activation rather than by restarting the rig.
    /// </summary>
    private sealed class RecoveringCorpusGateProbe : IRepoContextCorpusGateProbe
    {
        /// <summary>The classification returned by the next probe.</summary>
        public RepoContextAnnBuildCorpusCoverage Coverage { get; set; } =
            RepoContextAnnBuildCorpusCoverage.Denied;

        /// <inheritdoc />
        public Task<RepoContextAnnBuildCorpusCoverage> ProbeAsync(
            string repoId, CancellationToken cancellationToken) => Task.FromResult(Coverage);
    }
}
