using System.Text.RegularExpressions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// Tests for the observability of a vectorising pass: that the symbol arm reports
/// its progress while it runs, that the heartbeat log cannot fall silent for an
/// unbounded stretch, and that the elapsed figure the heartbeat prints is relative
/// to the phase whose rate a reader will compute from it.
/// <para>
/// All three cover one family of defect - a measurand that is never exercised, so
/// its silence reads as good news. A job whose only progress writer is the file arm
/// reports <c>filesEmbedded: 0</c> and a frozen <c>updatedAt</c> while a different
/// arm embeds thousands of vectors, and those two readings together are exactly the
/// signature of a dead job.
/// </para>
/// </summary>
public sealed partial class RepoContextBootstrapServicePassTests
{
    /// <summary>
    /// Matches the elapsed field of a vectorising heartbeat line so a test can assert
    /// on the number rather than on the sentence.
    /// </summary>
    private static readonly Regex HeartbeatElapsed =
        new(@"embedded after (\d+) ms", RegexOptions.CultureInvariant);

    /// <summary>
    /// The symbol arm must report progress <b>while it is running</b>, not only in a
    /// tally once it finishes.
    /// <para>
    /// This is the #2616 defect at its seam. The symbol arm was handed no progress
    /// callback at all, so it embedded in silence; because
    /// <c>RepoIndexJobGrain.ReportProgressAsync</c> is the sole writer of
    /// <c>UpdatedAt</c>, the job clock froze for as long as that arm ran. On a
    /// repository whose file coverage is already complete the file arm legitimately
    /// embeds zero and finishes early, so the symbol arm is the only arm doing work -
    /// and the resulting reading (<c>filesEmbedded: 0</c>, frozen <c>updatedAt</c>) is
    /// indistinguishable from a stalled job.
    /// </para>
    /// <para>
    /// The assertion is deliberately on reports observed <b>inside</b> the arm. A
    /// final tally reported after the arm returns would satisfy a naive "the counter
    /// appears" assertion while leaving the silence - and therefore the defect -
    /// completely intact.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_running_symbol_arm_reports_progress_before_it_returns()
    {
        _harness.WriteFile("a.cs", "class A { }");

        // The file arm embeds nothing, which is the honest steady-state case: file
        // coverage is complete, so there is nothing for it to do. It is also what
        // makes this test reproduce the production reading rather than a contrived
        // one.
        var callbackSupplied = false;
        var reportsAttempted = 0;
        var reportsObservedInsideTheArm = -1;

        _harness.VectorIngestor.IngestSymbolsAsync(
            Arg.Any<string>(),
            Arg.Any<IReadOnlyCollection<string>>(),
            Arg.Any<IReadOnlyCollection<string>>(),
            Arg.Any<CancellationToken>(),
            Arg.Any<Func<int, CancellationToken, ValueTask>?>())
            .Returns(call => EmbedSymbolsAsync(
                call.ArgAt<Func<int, CancellationToken, ValueTask>?>(4),
                call.ArgAt<CancellationToken>(3)));

        var result = await _harness.Service.RunAsync(_harness.Request(), _harness.Progress);

        var updates = _harness.ProgressUpdates;

        Assert.Multiple(() =>
        {
            // Non-vacuity first: if the fixture never got a callback, or never used
            // it, the count below would be a measured zero for an uninteresting
            // reason and the test must say which.
            Assert.That(
                callbackSupplied, Is.True,
                "The symbol arm was invoked without a progress callback, so it cannot report "
                + "liveness however long it runs.");
            Assert.That(
                reportsAttempted, Is.EqualTo(5),
                "The fixture did not drive the arm, so this test proves nothing about reporting.");

            // The discriminating assertion.
            Assert.That(
                reportsObservedInsideTheArm, Is.EqualTo(5),
                "The symbol arm's progress did not reach the sink until after the arm returned, "
                + "so the job clock stays frozen for the whole time the arm runs.");

            Assert.That(
                updates.Any(u => u.SymbolsEmbedded == 250), Is.True,
                "The arm's final symbol tally was never reported.");

            // The production reading this defect presented as: zero files embedded,
            // which is correct and must stay correct. The fix adds a term; it does
            // not relabel an existing one.
            Assert.That(
                updates.Where(u => u.FilesEmbedded is not null).All(u => u.FilesEmbedded == 0), Is.True,
                "The file arm embedded something, so this pass is not the steady-state case "
                + "the defect appears in.");

            Assert.That(result.FilesScanned, Is.EqualTo(1));
        });

        async Task<int> EmbedSymbolsAsync(Func<int, CancellationToken, ValueTask>? onProgress, CancellationToken ct)
        {
            if (onProgress is not null)
            {
                callbackSupplied = true;
                for (var embedded = 50; embedded <= 250; embedded += 50)
                {
                    reportsAttempted++;
                    await onProgress(embedded, ct);
                }
            }

            // Snapshot before returning: this is the whole point of the test. What
            // reaches the sink after the arm returns is not liveness.
            reportsObservedInsideTheArm = _harness.ProgressUpdates.Count(u => u.SymbolsEmbedded is not null);
            return 250;
        }
    }

    /// <summary>
    /// The vectorising heartbeat must not be able to fall silent indefinitely.
    /// <para>
    /// The heartbeat was throttled by <b>count</b> alone - one line per hundred newly
    /// embedded files - so the wall-clock period between lines is a function of
    /// throughput, and varies inversely with it. Measured on a real deployment
    /// embedding at 7.58 files/minute, that is one line every thirteen minutes. The
    /// channel therefore goes quietest exactly when the system is slowest, which is
    /// when an operator most needs to tell "slow" from "hung", and no duration of
    /// silence can be treated as evidence of anything.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_slow_vectorising_pass_still_beats_when_too_few_files_embed_to_trip_the_count_threshold()
    {
        var clock = new AdvanceableTimeProvider();
        using var harness = new BootstrapHarness(clock);
        harness.WriteFile("a.cs", "class A { }");

        // Three batches of one file each. The count threshold is 100, so it is never
        // crossed: every line this pass emits is attributable to the time floor.
        harness.OnIngest = async (report, ct) =>
        {
            await report(1, ct);
            clock.Advance(TimeSpan.FromMinutes(3));
            await report(2, ct);
            clock.Advance(TimeSpan.FromMinutes(3));
            await report(3, ct);
            return 3;
        };

        await harness.Service.RunAsync(harness.Request(), harness.Progress);

        var heartbeats = harness.LogEntries
            .Where(e => e.Message.Contains("vectorising progress", StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.ProgressUpdates.Any(u => u.FilesEmbedded == 3), Is.True,
                "The fixture did not drive the file arm, so this test proves nothing about the heartbeat.");
            Assert.That(
                heartbeats, Has.Length.GreaterThanOrEqualTo(2),
                "A pass that embedded far too few files to trip the count threshold emitted no "
                + "heartbeat, so its silence carries no information about whether it is alive.");
        });
    }

    /// <summary>
    /// The elapsed figure in the heartbeat must be relative to the vectorising phase,
    /// because that is the phase whose rate a reader divides it to obtain.
    /// <para>
    /// It previously read a stopwatch started at the beginning of the whole run, so
    /// the scan and apply phases sat silently in the denominator. On a real
    /// deployment the line read "203 file(s) embedded after 2758624 ms", giving 4.42
    /// files/min against a true embedding rate of 7.58 files/min - a 42 percent
    /// understatement, inside the single line whose purpose is to report that rate.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_vectorising_heartbeat_reports_time_spent_vectorising_not_time_since_the_run_began()
    {
        var clock = new AdvanceableTimeProvider();
        using var harness = new BootstrapHarness(clock);
        harness.WriteFile("a.cs", "class A { }");

        // Burn ten hours before vectorising starts, so a job-relative figure and a
        // phase-relative one cannot be confused for each other by any margin of
        // measurement error.
        var preVectorisingWorkHappened = false;
        harness.SymbolExtractor.Supports(Arg.Any<string>()).Returns(call =>
        {
            if (!preVectorisingWorkHappened)
            {
                preVectorisingWorkHappened = true;
                clock.Advance(TimeSpan.FromHours(10));
            }

            return string.Equals(call.ArgAt<string>(0), "csharp", StringComparison.Ordinal);
        });

        harness.OnIngest = async (report, ct) =>
        {
            clock.Advance(TimeSpan.FromHours(1));

            // Report past the count threshold, so a heartbeat fires under the old
            // count-only rule too. This test must isolate the elapsed figure: if the
            // line were absent under one arm of the comparison, the test would be
            // measuring whether a heartbeat happens rather than what it says.
            await report(150, ct);
            return 150;
        };

        await harness.Service.RunAsync(harness.Request(), harness.Progress);

        var elapsed = harness.LogEntries
            .Select(e => HeartbeatElapsed.Match(e.Message))
            .Where(m => m.Success)
            .Select(m => long.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                preVectorisingWorkHappened, Is.True,
                "No time was burned before the vectorising phase, so a job-relative figure and a "
                + "phase-relative one would be identical and this test could not tell them apart.");
            Assert.That(
                elapsed, Is.Not.Empty,
                "The pass emitted no heartbeat at all, so there is no elapsed figure to check.");

            var reported = elapsed[0];

            // Lower bound: the pre-fix figure came from a real Stopwatch, which in a
            // unit pass reads tens of milliseconds. Anything at hour scale proves the
            // reported figure tracks the phase timer.
            Assert.That(
                reported, Is.GreaterThanOrEqualTo((long)TimeSpan.FromHours(1).TotalMilliseconds),
                "The heartbeat's elapsed figure did not track the vectorising phase, so dividing "
                + "the line's own two numbers does not give the embedding rate.");

            // Upper bound: it must not include the ten hours spent before the phase
            // began, which is the denominator defect itself.
            Assert.That(
                reported, Is.LessThan((long)TimeSpan.FromHours(2).TotalMilliseconds),
                "The heartbeat's elapsed figure includes time spent before vectorising started, "
                + "so it understates the embedding rate by the length of the scan and apply phases.");
        });
    }
}
