using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the cadence the sweep loop runs at, and for the provenance its entry
/// line gives that cadence (issue #2459).
/// <para>
/// <b>The defect these pin.</b> The sweep took its interval from
/// <c>LATTICE_RECONCILE_INTERVAL_SECONDS</c>, a variable about the content reconcile.
/// The number was never hidden - the entry line already printed it - but a printed
/// value with no provenance is not diagnosable: an operator reading a day-long sweep
/// cadence had no way to learn it came from a variable they had set for an unrelated
/// reason, and no reason to suspect one. So there are two properties here, and they
/// fail independently: the cadence must not follow the reconcile knob, and the line
/// must name the knob it does follow.
/// </para>
/// <para>
/// <b>Why the entry line is assertable at all.</b> It is emitted before the
/// <c>CanSchedule</c> branch, so a service whose scheduling is off writes it and then
/// returns immediately. That is the cheap seam these tests drive: no sweep runs, no
/// interval elapses, and the line is still produced - which is the same property that
/// makes the line worth having on a host where nothing else happens.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexSweepServiceTests
{
    /// <summary>The distinctive fragment of the unconditional entry announcement.</summary>
    private const string EntryFragment = "build sweep entered";

    /// <summary>The entry announcement, if the service has written it yet.</summary>
    private static CapturedLogEntry? EntryLine(CapturingLoggerProvider provider)
        => provider.Entries
            .Cast<CapturedLogEntry?>()
            .FirstOrDefault(entry => entry!.Value.Level == LogLevel.Information
                && entry.Value.Message.Contains(EntryFragment, StringComparison.Ordinal));

    /// <summary>
    /// Runs a service to its entry line with scheduling switched off, so
    /// <c>ExecuteAsync</c> writes the line and returns without sweeping.
    /// </summary>
    private async Task<string> EntryLineForAsync(RepoContextIndexingOptions options)
    {
        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(provider);
        });

        var grainFactory = GrainFactoryListing();

        // Scheduling off is what makes this cheap: the entry line precedes the
        // CanSchedule branch, so the service announces and then returns. Withholding
        // the embedder is the cheapest of the three blocking disjuncts to arrange and
        // leaves the caller's options untouched, which matters here because the
        // options are the subject under test.
        var blocked = new RepoContextAnnIndexScheduler(
            grainFactory,
            options,
            NullLogger<RepoContextAnnIndexScheduler>.Instance,
            embedder: null);

        var sweep = Sweep(
            Store(grainFactory),
            blocked,
            options: options,
            logger: factory.CreateLogger<RepoContextAnnIndexSweepService>());

        await sweep.StartAsync(Ct);
        try
        {
            var announced = await WaitForAsync(() => EntryLine(provider) is not null, Ct);
            Assert.That(announced, Is.True, "the sweep must announce its entry unconditionally");
            return EntryLine(provider)!.Value.Message;
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task The_entry_line_names_the_variable_that_set_the_sweep_cadence()
    {
        var line = await EntryLineForAsync(new RepoContextIndexingOptions
        {
            AnnSweepInterval = TimeSpan.FromMinutes(5),
        });

        Assert.Multiple(() =>
        {
            Assert.That(line, Does.Contain(RepoContextIndexingOptions.AnnSweepIntervalSecondsKey),
                "A cadence with no provenance is not actionable: the old line printed the number "
                + "and left the operator to guess which knob produced it.");
            Assert.That(line, Does.Contain("00:05:00"),
                "The configured cadence must still be visible, not merely attributed.");
            Assert.That(line, Does.Contain("as configured"),
                "An unfloored value must say so, or 'floored' would be the only readable state.");
        });
    }

    [Test]
    public async Task The_entry_line_disclaims_the_reconcile_interval_it_used_to_follow()
    {
        var line = await EntryLineForAsync(new RepoContextIndexingOptions());

        Assert.Multiple(() =>
        {
            Assert.That(line, Does.Contain(RepoContextIndexingOptions.ReconcileIntervalSecondsKey),
                "The variable is named precisely because it is the one an operator would otherwise "
                + "still suspect: the coupling existed for long enough to be believed in, and a "
                + "silent removal leaves that belief in place.");
            Assert.That(line, Does.Contain("independent"),
                "Naming it without stating the relationship would read as though it still applied.");
        });
    }

    [Test]
    public async Task The_entry_line_says_when_the_floor_overrode_a_shorter_configured_cadence()
    {
        var line = await EntryLineForAsync(new RepoContextIndexingOptions
        {
            AnnSweepInterval = TimeSpan.FromSeconds(10),
        });

        Assert.Multiple(() =>
        {
            Assert.That(line, Does.Contain("floor"),
                "An operator who configures 10s and observes 60s must be told the floor raised it, "
                + "rather than left to conclude the setting was ignored.");
            Assert.That(line, Does.Contain("00:00:10"),
                "The configured value is what the operator will search their own deployment for, so "
                + "reporting only the effective one hides the half they can act on.");
            Assert.That(line, Does.Contain("00:01:00"), "The effective cadence is the floor.");
        });
    }

    [Test]
    public async Task The_sweep_cadence_reported_at_entry_ignores_the_reconcile_interval()
    {
        // The regression assertion, at the observable surface rather than on the
        // option: a day-long reconcile interval is what the affected deployment had
        // set, and it used to be echoed here verbatim as the sweep's own cadence.
        var line = await EntryLineForAsync(new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.FromHours(24),
        });

        Assert.Multiple(() =>
        {
            Assert.That(line, Does.Contain("00:15:00"),
                "The sweep cadence is its own default and must not track the reconcile interval.");
            Assert.That(line, Does.Not.Contain("1.00:00:00"),
                "Echoing the reconcile interval as the sweep cadence is the reported defect.");
        });
    }
}
