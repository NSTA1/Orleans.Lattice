using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Archive;

/// <summary>
/// Tests for <see cref="RepoContextMemoryArchiveService"/>, the hosted service that
/// drives the durable-memory archive: one restore attempt at startup, a periodic
/// export while the host runs, and one bounded export on graceful shutdown.
/// <para>
/// <b>Why the service needs its own tests even though the archive has them.</b>
/// <see cref="RepoContextMemoryArchive"/> is tested for what an export and a restore
/// do to a directory and a tree. This type is tested for something the archive cannot
/// observe about itself: that the three triggers actually fire, that the run
/// credential is stamped onto every archive turn, and that each outcome the archive
/// can return is reported rather than passed over. The last is the one that matters
/// most - a partial restore presents as a normally populated store, so an outcome
/// that reached no reporter and no log would leave an operator believing the boot was
/// clean.
/// </para>
/// <para>
/// The clock is <see cref="ManualTimeProvider"/> throughout, so the five-second
/// initial delay and the five-minute cadence cost no wall-clock and the fixture
/// asserts on the trigger rather than on a sleep.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextMemoryArchiveServiceTests
{
    private const string RepoId = "acme";

    private string scratch = string.Empty;

    private static CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    [SetUp]
    public void CreateScratch()
    {
        scratch = Path.Combine(
            Path.GetTempPath(), "lattice-archive-service-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(scratch);
    }

    [TearDown]
    public void RemoveScratch()
    {
        try
        {
            if (Directory.Exists(scratch))
            {
                Directory.Delete(scratch, recursive: true);
            }
        }
        catch (IOException)
        {
        }
    }

    private string SnapshotPath => Path.Combine(scratch, RepoContextMemoryArchive.SnapshotFileName);

    private RepoContextMemoryArchiveOptions Options(
        RepoContextMemoryArchiveRestoreMode restoreMode = RepoContextMemoryArchiveRestoreMode.Auto,
        string? directory = null,
        bool useScratch = true)
        => new()
        {
            Directory = directory ?? (useScratch ? scratch : null),
            RestoreMode = restoreMode,
        };

    /// <summary>
    /// Everything a test needs to drive one service and assert on what it did.
    /// </summary>
    private sealed record Rig(
        RepoContextMemoryArchiveService Service,
        ManualTimeProvider Clock,
        CapturingLoggerProvider Logs,
        IRepoIndexRunAuthority Authority,
        List<string> RestoreOutcomes,
        MeterListener Listener,
        ILoggerFactory LoggerFactory,
        ILattice MemoryTree,
        Serializer Serializer) : IDisposable
    {
        public void Dispose()
        {
            Listener.Dispose();
            Service.Dispose();
            LoggerFactory.Dispose();
        }

        public bool Logged(LogLevel level, string fragment)
            => Logs.Entries.Any(e =>
                e.Level == level
                && e.Message.Contains(fragment, StringComparison.OrdinalIgnoreCase));

        public int CountLogged(LogLevel level, string fragment)
            => Logs.Entries.Count(e =>
                e.Level == level
                && e.Message.Contains(fragment, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Builds a service over the harness's real memory tree and a real archive, with a
    /// listener already attached to the restore instrument. The listener is started
    /// before the reporter is constructed because the reporter mints its zero-valued
    /// series in its constructor, and a listener attached afterwards never sees them.
    /// </summary>
    private Rig CreateRig(
        RepoContextMcpHarness harness,
        RepoContextMemoryArchiveOptions? options = null,
        LatticeCredential? credential = null,
        Exception? authorityFault = null)
    {
        options ??= Options();

        var outcomes = new List<string>();
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name,
                        RepoContextUsageRecorder.MeterName,
                        StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextMemoryRestoreReporter.InstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (value == 0)
            {
                // The constructor's pre-minted arms. They prove the series exist; only
                // a recorded attempt tells us what the service actually did.
                return;
            }

            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextMemoryRestoreReporter.OutcomeTagKey
                    && tag.Value is string outcome)
                {
                    lock (outcomes)
                    {
                        outcomes.Add(outcome);
                    }
                }
            }
        });
        listener.Start();

        var authority = Substitute.For<IRepoIndexRunAuthority>();
        if (authorityFault is not null)
        {
            authority.Resolve().Throws(authorityFault);
        }
        else
        {
            authority.Resolve().Returns(credential);
        }

        var logs = new CapturingLoggerProvider();

        // Deliberately NOT disposed here: the factory owns the logger the service
        // keeps for its whole lifetime, so disposing it at the end of this method
        // would silently retire every log line the fixture asserts on. The rig
        // disposes it instead.
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(
            b => b.AddProvider(logs));
        var clock = new ManualTimeProvider();
        var serializer = harness.Services.GetRequiredService<Serializer>();

        var service = new RepoContextMemoryArchiveService(
            harness.GrainFactory,
            serializer,
            options,
            new RepoContextMemoryArchive(options),
            new RepoContextMemoryRestoreReporter(),
            authority,
            clock,
            loggerFactory.CreateLogger<RepoContextMemoryArchiveService>());

        return new Rig(
            service,
            clock,
            logs,
            authority,
            outcomes,
            listener,
            loggerFactory,
            harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory),
            serializer);
    }

    /// <summary>
    /// Seeds memory through the capture path production writes through, so the export
    /// sees the envelope shape a real store holds rather than a raw value.
    /// </summary>
    private static async Task SeedAsync(
        ILattice tree, Serializer serializer, params string[] ids)
    {
        foreach (var id in ids)
        {
            var record = new MemoryRecord
            {
                RepoId = RepoId,
                Topic = "gotchas",
                Id = id,
                Kind = MemoryKind.Note,
                Title = RepoContextValues.Lww($"title-{id}", Clock(1_000)),
                Body = RepoContextValues.Lww($"body-{id}", Clock(1_000)),
            };

            await RepoContextMemoryCodec
                .Accessor(tree, RepoContextKeys.Memory(RepoId, "gotchas", id))
                .SetAsync("local", serializer.SerializeToArray(record));
        }
    }

    private static Task<long> CountMemoryAsync(ILattice tree)
        => RepoContextMemoryArchive.CountMemoryAsync(tree, Ct);

    /// <summary>
    /// Starts the service and carries the clock past the initial delay, so the startup
    /// restore has been triggered by the time this returns. The barrier waits on the
    /// restore reporter, which is the service's own record that the attempt completed.
    /// </summary>
    /// <summary>
    /// Carries the clock past a wait the subject has actually armed.
    /// <para>
    /// Waiting for the timer first is load-bearing. A <c>BackgroundService</c> on
    /// .NET 10 does not reach its first <c>await</c> before <c>StartAsync</c>
    /// returns, so advancing straight after it moves the clock past a deadline
    /// nobody is waiting on - and because <see cref="ManualTimeProvider"/> only
    /// fires the timers that exist when it is advanced, the service then waits
    /// forever against a clock that has already gone by.
    /// </para>
    /// </summary>
    private static async Task AdvanceArmedAsync(Rig rig, TimeSpan delta, string because)
    {
        await TestPoll.UntilAsync(
            () => rig.Clock.PendingTimerCount > 0,
            $"the service to arm {because}");
        rig.Clock.Advance(delta);
    }

    /// <summary>
    /// Starts the service and carries the clock past the initial delay, so the startup
    /// restore has been triggered by the time this returns. The barrier waits on the
    /// restore reporter, which is the service's own record that the attempt completed.
    /// </summary>
    private static async Task StartAndRestoreAsync(Rig rig)
    {
        await rig.Service.StartAsync(Ct);
        await AdvanceArmedAsync(rig, TimeSpan.FromSeconds(5), "its startup delay");

        var recorded = await TestPoll.TryUntilAsync(() =>
        {
            lock (rig.RestoreOutcomes)
            {
                return rig.RestoreOutcomes.Count > 0;
            }
        });

        if (!recorded)
        {
            var seen = string.Join(
                Environment.NewLine,
                rig.Logs.Entries.Select(e => $"  [{e.Level}] {e.Message}"));
            Assert.Fail(
                "the startup restore attempt was never recorded on the restore instrument."
                + Environment.NewLine
                + (seen.Length == 0 ? "  (no log entries were captured)" : seen));
        }
    }

    private static string SingleOutcome(Rig rig)
    {
        lock (rig.RestoreOutcomes)
        {
            Assert.That(rig.RestoreOutcomes, Has.Count.EqualTo(1));
            return rig.RestoreOutcomes[0];
        }
    }

    [Test]
    public async Task A_store_that_came_up_empty_is_restored_from_the_archive_at_startup()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness);

        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1", "g2", "g3");
        var archive = new RepoContextMemoryArchive(Options());
        var export = await archive.ExportAsync(rig.MemoryTree, rig.Serializer, Ct);
        Assert.That(export.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written));

        // The state a destroyed data volume leaves behind.
        var prefix = RepoContextKeys.AllReposPrefix();
        await rig.MemoryTree.DeleteRangeAsync(
            prefix, RepoContextPortability.PrefixUpperBound(prefix), Ct);
        Assert.That(await CountMemoryAsync(rig.MemoryTree), Is.Zero);

        await StartAndRestoreAsync(rig);

        var held = await CountMemoryAsync(rig.MemoryTree);
        Assert.Multiple(() =>
        {
            Assert.That(SingleOutcome(rig), Is.EqualTo("restored"));
            Assert.That(held, Is.EqualTo(3));
            Assert.That(
                rig.Logged(LogLevel.Warning, "Durable memory was restored from the archive"),
                Is.True,
                "a restore means the volume was replaced or wiped, which is warning-worthy");
        });

        await rig.Service.StopAsync(Ct);
    }

    [Test]
    public async Task A_store_that_already_holds_memory_is_left_alone_and_the_decision_is_reported()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness);

        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1", "g2");
        var archive = new RepoContextMemoryArchive(Options());
        await archive.ExportAsync(rig.MemoryTree, rig.Serializer, Ct);

        await StartAndRestoreAsync(rig);

        var held = await CountMemoryAsync(rig.MemoryTree);
        Assert.Multiple(() =>
        {
            Assert.That(SingleOutcome(rig), Is.EqualTo("nothingtorestore"));
            Assert.That(held, Is.EqualTo(2));
            Assert.That(
                rig.Logged(LogLevel.Information, "was not restored from the archive"),
                Is.True);
        });

        await rig.Service.StopAsync(Ct);
    }

    [Test]
    public async Task Restore_off_records_a_not_attempted_outcome_without_reading_the_archive()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(
            harness, Options(RepoContextMemoryArchiveRestoreMode.Off));

        await StartAndRestoreAsync(rig);

        Assert.That(SingleOutcome(rig), Is.EqualTo("notattempted"));

        await rig.Service.StopAsync(Ct);
    }

    /// <summary>
    /// A snapshot truncated past its header still looks like a snapshot and still
    /// carries complete records, so the import writes some and then fails. That is the
    /// <c>partial</c> arm - the one state the whole restore-state marker exists for,
    /// and the one an operator has to be told about without knowing to look.
    /// </summary>
    [Test]
    public async Task A_partial_restore_is_reported_at_error_level_because_nothing_else_will_report_it()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);

        var intact = Path.Combine(scratch, "intact");
        Directory.CreateDirectory(intact);
        var seedRig = CreateRig(harness, Options(directory: intact));
        using (seedRig)
        {
            await SeedAsync(
                seedRig.MemoryTree,
                seedRig.Serializer,
                [.. Enumerable.Range(0, 40).Select(i => $"g{i:D3}")]);
            var archive = new RepoContextMemoryArchive(Options(directory: intact));
            var export = await archive.ExportAsync(seedRig.MemoryTree, seedRig.Serializer, Ct);
            Assert.That(export.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written));
        }

        var full = await File.ReadAllBytesAsync(
            Path.Combine(intact, RepoContextMemoryArchive.SnapshotFileName), Ct);
        await File.WriteAllBytesAsync(SnapshotPath, full[..(int)(full.Length * 0.6)], Ct);

        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);
        var prefix = RepoContextKeys.AllReposPrefix();
        await tree.DeleteRangeAsync(prefix, RepoContextPortability.PrefixUpperBound(prefix), Ct);

        using var rig = CreateRig(harness);
        await StartAndRestoreAsync(rig);

        Assert.Multiple(() =>
        {
            Assert.That(SingleOutcome(rig), Is.EqualTo("partial"));
            Assert.That(
                rig.Logged(LogLevel.Error, "only PARTIALLY restored"),
                Is.True,
                "a partial import presents as a populated store, so it must be loud");
        });

        await rig.Service.StopAsync(Ct);
    }

    /// <summary>
    /// A snapshot whose header is destroyed is refused outright, so nothing is written
    /// and the tree is exactly as it was. That is a different operator response from a
    /// partial, which is why the two are different arms.
    /// </summary>
    [Test]
    public async Task An_unreadable_archive_is_reported_as_failed_and_the_tree_is_untouched()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        await File.WriteAllBytesAsync(SnapshotPath, [0x00, 0x01, 0x02, 0x03, 0x04], Ct);

        using var rig = CreateRig(harness);
        var tree = rig.MemoryTree;
        var prefix = RepoContextKeys.AllReposPrefix();
        await tree.DeleteRangeAsync(prefix, RepoContextPortability.PrefixUpperBound(prefix), Ct);

        await StartAndRestoreAsync(rig);

        var held = await CountMemoryAsync(tree);
        Assert.Multiple(() =>
        {
            Assert.That(SingleOutcome(rig), Is.EqualTo("failed"));
            Assert.That(held, Is.Zero);
            Assert.That(
                rig.Logged(LogLevel.Warning, "could not be restored from the archive"),
                Is.True);
        });

        await rig.Service.StopAsync(Ct);
    }

    [Test]
    public async Task Memory_is_exported_on_the_configured_cadence_while_the_host_runs()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness);

        await StartAndRestoreAsync(rig);
        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1", "g2");
        Assert.That(File.Exists(SnapshotPath), Is.False, "no export has been triggered yet");

        await AdvanceArmedAsync(rig, TimeSpan.FromMinutes(5), "its export cadence");

        await TestPoll.UntilAsync(
            () => File.Exists(SnapshotPath),
            "the periodic export to land a snapshot");
        Assert.That(rig.Logged(LogLevel.Information, "Durable memory archived (periodic)"), Is.True);

        // The loop must come back round rather than exiting after one export. The
        // snapshot file appears part-way through the export, so waiting only for it
        // would let this pass against a service that exported once and stopped. A
        // SECOND periodic pass is the observable that the loop iterated: waiting on
        // the cadence timer being re-armed would not be, because the elapsed timer's
        // disposal is asynchronous and the count can still read one from the pass
        // that just finished.
        await TestPoll.UntilAsync(
            () =>
            {
                rig.Clock.Advance(TimeSpan.FromMinutes(5));
                return rig.CountLogged(
                    LogLevel.Information, "Durable memory archived (periodic)") >= 2;
            },
            "the export loop to complete a second pass");

        await rig.Service.StopAsync(Ct);
    }

    [Test]
    public async Task A_final_export_runs_on_graceful_shutdown()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness);

        await StartAndRestoreAsync(rig);
        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1");
        Assert.That(File.Exists(SnapshotPath), Is.False);

        await rig.Service.StopAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                File.Exists(SnapshotPath),
                Is.True,
                "memory authored since the last interval must be captured by the stop-time export");
            Assert.That(
                rig.Logged(LogLevel.Information, "Durable memory archived (shutdown)"),
                Is.True);
        });
    }

    /// <summary>
    /// An empty store must never overwrite a good archive: that failure would use the
    /// recovery mechanism to destroy the copy it exists to protect.
    /// </summary>
    [Test]
    public async Task An_empty_store_is_refused_rather_than_allowed_to_empty_the_archive()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness, Options(RepoContextMemoryArchiveRestoreMode.Off));

        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1", "g2");
        var archive = new RepoContextMemoryArchive(Options());
        await archive.ExportAsync(rig.MemoryTree, rig.Serializer, Ct);
        var good = await File.ReadAllBytesAsync(SnapshotPath, Ct);

        var prefix = RepoContextKeys.AllReposPrefix();
        await rig.MemoryTree.DeleteRangeAsync(
            prefix, RepoContextPortability.PrefixUpperBound(prefix), Ct);

        await StartAndRestoreAsync(rig);
        await rig.Service.StopAsync(Ct);

        var after = await File.ReadAllBytesAsync(SnapshotPath, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(
                after,
                Is.EqualTo(good),
                "the archive must be byte-identical: an empty export was refused, not written");
            Assert.That(rig.Logged(LogLevel.Warning, "was refused"), Is.True);
        });
    }

    /// <summary>
    /// An export that throws must still be reported, and reported with its
    /// consequence: the archive is intact but is now older than the store.
    /// </summary>
    [Test]
    public async Task An_export_that_throws_is_reported_as_not_archived_rather_than_failing_silently()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);

        // No directory configured, so the archive throws rather than writing. This is
        // the shape of any I/O fault on the archive path.
        using var rig = CreateRig(
            harness, Options(RepoContextMemoryArchiveRestoreMode.Off, useScratch: false));

        await StartAndRestoreAsync(rig);
        await rig.Service.StopAsync(Ct);

        Assert.That(
            rig.Logged(LogLevel.Warning, "Durable memory was NOT archived (shutdown)"),
            Is.True);
    }

    /// <summary>
    /// A restore that throws must not take the service down with it: the store runs on
    /// with whatever memory it already held, and the fault is reported at error level.
    /// </summary>
    [Test]
    public async Task A_restore_that_throws_is_reported_and_the_service_keeps_running()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness, Options(useScratch: false));

        await rig.Service.StartAsync(Ct);
        await AdvanceArmedAsync(rig, TimeSpan.FromSeconds(5), "its startup delay");

        await TestPoll.UntilAsync(
            () => rig.Logged(LogLevel.Error, "Restoring durable memory from the archive"),
            "the restore fault to be reported at error level");

        // Still looping: the cadence export runs, and reports its own failure rather
        // than the loop having ended.
        await AdvanceArmedAsync(rig, TimeSpan.FromMinutes(5), "its export cadence");
        await TestPoll.UntilAsync(
            () => rig.Logged(LogLevel.Warning, "Durable memory was NOT archived (periodic)"),
            "the periodic export to keep running after a failed restore");

        await rig.Service.StopAsync(Ct);
    }

    /// <summary>
    /// The run credential is what stops a background turn reading as anonymous. Under
    /// a default-deny gate an uncredentialed range read does not throw - it returns
    /// empty - so an export made without one would archive nothing and report it as
    /// healthy.
    /// </summary>
    [Test]
    public async Task Every_archive_turn_resolves_the_run_authoritys_credential()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(
            harness,
            credential: new LatticeCredential("archive-token") { PrincipalId = "repo-index-run" });

        await StartAndRestoreAsync(rig);
        var afterRestore = rig.Authority.ReceivedCalls().Count(c => c.GetMethodInfo().Name == "Resolve");

        await rig.Service.StopAsync(Ct);
        var afterExport = rig.Authority.ReceivedCalls().Count(c => c.GetMethodInfo().Name == "Resolve");

        Assert.Multiple(() =>
        {
            Assert.That(afterRestore, Is.EqualTo(1), "the startup restore opens a credential scope");
            Assert.That(
                afterExport,
                Is.EqualTo(2),
                "the shutdown export opens its own scope rather than inheriting one");
        });
    }

    /// <summary>
    /// An authority that throws is reported and the pass proceeds uncredentialed: a
    /// host with no access gate resolves null here anyway, and the archive's own
    /// empty-over-non-empty refusal makes a degraded pass safe.
    /// </summary>
    [Test]
    public async Task An_authority_that_throws_is_reported_and_the_pass_still_proceeds()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(
            harness,
            Options(RepoContextMemoryArchiveRestoreMode.Off),
            authorityFault: new InvalidOperationException("no authority is configured"));

        await StartAndRestoreAsync(rig);
        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1");
        await rig.Service.StopAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                rig.Logged(LogLevel.Warning, "could not resolve a run credential"),
                Is.True);
            Assert.That(
                File.Exists(SnapshotPath),
                Is.True,
                "the export still runs: a degraded pass is safe, a skipped one is not");
        });
    }

    /// <summary>
    /// The stop-time export is bounded, and a caller whose token is already cancelled
    /// gets the bounded path rather than a hang. The export does not land, and that is
    /// reported rather than passed over in silence.
    /// </summary>
    [Test]
    public async Task A_cancelled_shutdown_reports_the_export_it_could_not_complete()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(cancellationToken: Ct);
        using var rig = CreateRig(harness, Options(RepoContextMemoryArchiveRestoreMode.Off));

        await StartAndRestoreAsync(rig);
        await SeedAsync(rig.MemoryTree, rig.Serializer, "g1");

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await rig.Service.StopAsync(cancelled.Token);

        Assert.Multiple(() =>
        {
            Assert.That(
                rig.Logged(LogLevel.Warning, "Durable memory was NOT archived (shutdown)"),
                Is.True);
            Assert.That(
                File.Exists(SnapshotPath),
                Is.False,
                "a cancelled export leaves the existing archive untouched rather than half-written");
        });
    }

    /// <summary>
    /// A restore interrupted by the host stopping is rethrown, not swallowed into the
    /// generic fault arm. The distinction is load-bearing: a cancelled restore is the
    /// host shutting down and must end the loop, whereas a faulted one is a real
    /// failure the service must report and then keep running through. Reporting a
    /// shutdown as an archive fault would put a false alarm in every clean stop.
    /// <para>
    /// Driven through a substituted tree rather than the harness, because "the tree
    /// read observed cancellation" is precisely the state to reproduce and racing a
    /// real stop against a real restore would reproduce it only sometimes.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_restore_cancelled_by_shutdown_is_rethrown_rather_than_reported_as_a_fault()
    {
        // A snapshot must exist or the restore short-circuits before reading the tree.
        await File.WriteAllBytesAsync(SnapshotPath, [1, 2, 3, 4, 5, 6, 7, 8], Ct);

        var tree = Substitute.For<ILattice>();
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Memory, null).Returns(tree);

        var options = Options();
        var logs = new CapturingLoggerProvider();
        using var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(
            b => b.AddProvider(logs));
        var clock = new ManualTimeProvider();
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();

        using var service = new RepoContextMemoryArchiveService(
            grainFactory,
            services.GetRequiredService<Serializer>(),
            options,
            new RepoContextMemoryArchive(options),
            new RepoContextMemoryRestoreReporter(),
            Substitute.For<IRepoIndexRunAuthority>(),
            clock,
            loggerFactory.CreateLogger<RepoContextMemoryArchiveService>());

        await service.StartAsync(Ct);
        await TestPoll.UntilAsync(
            () => clock.PendingTimerCount > 0, "the service to arm its startup delay");
        clock.Advance(TimeSpan.FromSeconds(5));

        // The cancellation ends the loop, so no further wait is ever armed. That is
        // the observable difference from the fault path, which keeps looping.
        var settled = await TestPoll.TryUntilAsync(() => clock.PendingTimerCount > 0);

        Assert.Multiple(() =>
        {
            Assert.That(
                settled,
                Is.False,
                "a cancelled restore must end the loop rather than re-arm the cadence");
            Assert.That(
                logs.Entries.Any(e => e.Level == LogLevel.Error),
                Is.False,
                "a shutdown is not an archive failure and must not be reported as one");
        });
    }
}
