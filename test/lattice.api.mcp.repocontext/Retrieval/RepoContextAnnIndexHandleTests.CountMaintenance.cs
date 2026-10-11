using System.Reflection;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

public sealed partial class RepoContextAnnIndexHandleTests
{
    [Test]
    public async Task Advance_restored_build_deferred_replay_preserves_verification_until_completed_count()
    {
        var clock = new AdvanceableTimeProvider();
        using var rig = new Rig(Options(clock: clock));
        rig.SeedRing(64);
        VectorIndexBuildProgress progress;
        do
        {
            progress = await rig.Handle.AdvanceAsync(Ct);
            Assert.That(progress.Phase, Is.Not.EqualTo(VectorIndexBuildPhase.Ready));
        } while (progress.VectorsIndexed == 0);
        rig.Restart();
        progress = await rig.Handle.AdvanceAsync(Ct);
        Assert.That(progress.Phase, Is.Not.EqualTo(VectorIndexBuildPhase.Ready),
            "the restored build must still be streaming when the real write arrives");
        const string Revised = "vec-000000";
        var distinctive = Rig.Unit(3);
        rig.Source.Set(Revised, RepoContextKeys.File(RepoId, "src/File0.cs"), distinctive);
        await rig.Handle.ApplyWriteAsync(
            [new RepoContextAnnVectorUpdate(Revised, RepoContextKeys.File(RepoId, "src/File0.cs"), distinctive)],
            [], Ct);
        var counts = rig.Source.CountCalls;
        await rig.Handle.EnsureBuiltAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts),
                "deferred replay deliberately bypasses the count");
            Assert.That(rig.Handle.SourceCountPending, Is.True,
                "replaying known writes cannot complete restored source-count verification");
        });
        var outcome = await rig.Handle.SearchAsync(distinctive, 1, Ct);
        Assert.That(outcome.Matches[0].Score, Is.GreaterThan(0.9d));
        rig.Source.FailNextCounts(1, static () =>
            new RepoContextCountBudgetExceededException(RepoId, 12, TimeSpan.FromSeconds(10)));
        await rig.Handle.AdvanceAsync(Ct);
        await rig.Handle.ApplyWriteAsync(
            [new RepoContextAnnVectorUpdate(Revised, RepoContextKeys.File(RepoId, "src/File0.cs"), distinctive)],
            [], Ct);
        await rig.Handle.AdvanceAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts + 1));
            Assert.That(rig.Handle.SourceCountPending, Is.True);
        });
        clock.Advance(TimeSpan.FromMinutes(1));
        await rig.Handle.AdvanceAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts + 2));
            Assert.That(rig.Handle.SourceCountPending, Is.False);
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Advance_unknown_count_does_not_repair_and_retries_only_after_backoff(bool budgetExpired)
    {
        var clock = new AdvanceableTimeProvider();
        var log = new CapturingLoggerProvider();
        using var rig = new Rig(Options(clock: clock), log.CreateLogger("handle"));
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        rig.Restart(log.CreateLogger("handle"));
        rig.Source.Set("vec-999999", RepoContextKeys.File(RepoId, "src/Late.cs"), Rig.Unit(1));
        rig.Source.FailNextCounts(1, () => budgetExpired
            ? new RepoContextCountBudgetExceededException(RepoId, 12, TimeSpan.FromSeconds(10))
            : new EnumerationAbortedException("reconnect budget exhausted"));
        var enumerations = rig.Source.FullEnumerations;
        var writes = rig.Store.RecordsWritten;
        var counts = rig.Source.CountCalls;

        await rig.Handle.AdvanceAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts + 1));
            Assert.That(rig.Source.FullEnumerations, Is.EqualTo(enumerations),
                "Unknown is not evidence of lag; the production caller must not stream a repair.");
            Assert.That(rig.Store.RecordsWritten, Is.EqualTo(writes));
            Assert.That(log.Entries.Any(e => e.Message.Contains("unknown", StringComparison.Ordinal)), Is.True);
        });
        for (var i = 0; i < 10; i++)
            await rig.Handle.AdvanceAsync(Ct);
        Assert.That(rig.Source.CountCalls, Is.EqualTo(counts + 1));

        clock.Advance(TimeSpan.FromMinutes(1));
        await rig.Handle.AdvanceAsync(Ct);
        var outcome = await rig.Handle.SearchAsync(Rig.Unit(1), 5, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts + 2));
            Assert.That(rig.Source.FullEnumerations, Is.EqualTo(enumerations + 1));
            Assert.That(outcome.Matches.Select(static m => m.VectorId), Does.Contain("vec-999999"),
                "a completed count proving shortfall must still repair the persisted index");
        });
    }

    [Test]
    public async Task Advance_repeated_unknowns_cap_backoff_and_completed_count_resets_it()
    {
        var clock = new AdvanceableTimeProvider();
        using var rig = new Rig(Options(clock: clock));
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        rig.Restart();
        rig.Source.FailNextCounts(7, static () =>
            new RepoContextCountBudgetExceededException(RepoId, 12, TimeSpan.FromSeconds(10)));
        var counts = rig.Source.CountCalls;
        var enumerations = rig.Source.FullEnumerations;
        foreach (var minutes in new[] { 1, 2, 4, 8, 10, 10, 10 })
        {
            await rig.Handle.AdvanceAsync(Ct);
            Assert.That(rig.Source.CountCalls, Is.EqualTo(++counts));
            clock.Advance(TimeSpan.FromMinutes(minutes) - TimeSpan.FromSeconds(1));
            await rig.Handle.AdvanceAsync(Ct);
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts));
            clock.Advance(TimeSpan.FromSeconds(1));
        }
        await rig.Handle.AdvanceAsync(Ct);
        Assert.That(rig.Source.CountCalls, Is.EqualTo(++counts));
        rig.Source.FailNextCounts(1, static () =>
            new RepoContextCountBudgetExceededException(RepoId, 12, TimeSpan.FromSeconds(10)));
        await rig.Handle.AdvanceAsync(Ct);
        Assert.That(rig.Source.CountCalls, Is.EqualTo(++counts));
        clock.Advance(TimeSpan.FromMinutes(1));
        await rig.Handle.AdvanceAsync(Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts + 1));
            Assert.That(rig.Source.FullEnumerations, Is.EqualTo(enumerations));
        });
    }

    [Test]
    public async Task Advance_retrain_due_suppresses_count_and_reports_submitted_write_cost()
    {
        var log = new CapturingLoggerProvider();
        using var rig = new Rig(Options(retrainFraction: 0.01d), log.CreateLogger("handle"));
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        // Arrange pending updates without taking ApplyWriteAsync's maintenance turn;
        // AdvanceAsync remains the production caller under test.
        var field = typeof(RepoContextAnnIndexHandle).GetField("_index", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(field, Is.Not.Null, "The index field moved; update the pending-update arrangement.");
        var index = (DurableVectorIndex)field!.GetValue(rig.Handle)!;
        await index.UpsertAsync("vec-000000", Rig.Unit(2), Ct);
        var counts = rig.Source.CountCalls;
        var writes = rig.Store.RecordsWritten;
        var bytes = rig.Store.PayloadBytesWritten;

        await rig.Handle.AdvanceAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(rig.Source.CountCalls, Is.EqualTo(counts),
                "a retraining turn must not add a source-count walk to its load");
            Assert.That(rig.Store.RecordsWritten, Is.GreaterThan(writes));
            Assert.That(log.Entries.Any(e => e.Message.Contains(
                $"WAL input submitted {rig.Store.RecordsWritten - writes} records, "
                + $"{rig.Store.PayloadBytesWritten - bytes} key/value payload bytes", StringComparison.Ordinal)), Is.True,
                "the production retrain caller must report write cost, not just its update trigger");
        });
    }

    [Test]
    public async Task Advance_unexpected_count_failure_propagates_without_repair()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        rig.Restart();
        rig.Source.FailNextCounts(1, static () => new InvalidOperationException("not a count budget"));
        var enumerations = rig.Source.FullEnumerations;
        Assert.That(async () => await rig.Handle.AdvanceAsync(Ct),
            Throws.InvalidOperationException.With.Message.EqualTo("not a count budget"));
        Assert.That(rig.Source.FullEnumerations, Is.EqualTo(enumerations));
    }

    [Test]
    public async Task ApplyWrite_failed_retrain_reports_attempted_wal_input_and_preserves_error()
    {
        var log = new CapturingLoggerProvider();
        using var rig = new Rig(Options(retrainFraction: 0.01d), log.CreateLogger("handle"));
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        var bytes = rig.Store.PayloadBytesWritten;
        rig.Store.WriteFault = new InvalidOperationException("injected persistence failure");
        Assert.That(async () => await rig.Handle.ApplyWriteAsync(
            [new RepoContextAnnVectorUpdate("vec-000000", RepoContextKeys.File(RepoId, "src/File0.cs"), Rig.Unit(2))],
            [], Ct), Throws.InvalidOperationException.With.Message.EqualTo("injected persistence failure"));
        var report = log.Entries.Single(e => e.Message.Contains("WAL input", StringComparison.Ordinal)).Message;
        Assert.Multiple(() =>
        {
            Assert.That(report, Does.Contain("retrain failed"));
            Assert.That(report, Does.Not.Contain("WAL input submitted 0 records"));
            Assert.That(report, Does.Contain("does not measure retained WAL bytes"));
            Assert.That(rig.Store.PayloadBytesWritten, Is.EqualTo(bytes),
                "attempted WAL input is deliberately not presented as committed or retained WAL");
        });
    }
}
