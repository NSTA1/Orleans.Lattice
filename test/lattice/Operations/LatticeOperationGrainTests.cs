using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Unit tests for <see cref="LatticeOperationGrain"/>, the durable record of one
/// coordinated long-running operation: idempotent begin, progress (phase index,
/// clamped totals, no regression), cancellation, terminal completion, failure of
/// a lost runner by silo death or heartbeat lease, and bounded retention. Driven
/// with a manual clock, so nothing depends on wall time.
/// </summary>
[TestFixture]
public sealed partial class LatticeOperationGrainTests
{
    private const string Tenant = "default";
    private const string OperationId = "op-1";

    private static readonly SiloAddress RunnerSilo = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 11111), 1);

    private ManualTimeProvider _clock = null!;
    private FakePersistentState<LatticeOperationGrainState> _state = null!;
    private ILatticeOperationIndexGrain _index = null!;
    private ILatticeOperationSiloLiveness _liveness = null!;
    private LatticeOperationOptions _options = null!;

    [SetUp]
    public void SetUp()
    {
        _clock = new ManualTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
        _state = new FakePersistentState<LatticeOperationGrainState>();
        _index = Substitute.For<ILatticeOperationIndexGrain>();
        _liveness = Substitute.For<ILatticeOperationSiloLiveness>();
        _options = new LatticeOperationOptions
        {
            Retention = TimeSpan.FromHours(1),
            HeartbeatLease = TimeSpan.FromMinutes(2),
        };
    }

    private LatticeOperationGrain CreateGrain(string operationId = OperationId, IReminderRegistry? reminders = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("latticeoperation", LatticeOperationKey.For(Tenant, operationId)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeOperationIndexGrain>(LatticeOperationKey.ForIndex(Tenant), null).Returns(_index);

        return new LatticeOperationGrain(
            context,
            _state,
            factory,
            _liveness,
            Options.Create(_options),
            NullLogger<LatticeOperationGrain>.Instance,
            reminders ?? Substitute.For<IReminderRegistry>())
        {
            Clock = _clock,
            TimerFactory = _ => Substitute.For<IGrainTimer>(),
        };
    }

    private static LatticeOperationBeginRequest Begin(string kind = "test.kind", params string[] phases) =>
        new() { Kind = kind, TreeIds = ["tree-a"], Phases = phases, RunnerSilo = RunnerSilo };

    [Test]
    public async Task Begin_and_complete_keep_their_own_copy_of_the_senders_collections()
    {
        var grain = CreateGrain();
        var trees = new List<string> { "tree-a" };
        var phases = new List<string> { "A", "B" };
        var attributes = new Dictionary<string, string> { ["scope"] = "whole" };
        var result = new Dictionary<string, string> { ["backupId"] = "b1" };

        await grain.BeginAsync(new LatticeOperationBeginRequest { Kind = "test.kind", TreeIds = trees, Phases = phases, Attributes = attributes, RunnerSilo = RunnerSilo });
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded("b1", result));
        trees[0] = "tree-z";
        phases.Add("C");
        attributes["scope"] = "prefix";
        result["backupId"] = "b2";

        var record = _state.State.Record!;
        Assert.Multiple(() =>
        {
            Assert.That(record.TreeIds, Is.EqualTo(new[] { "tree-a" }));
            Assert.That(record.Phases, Is.EqualTo(new[] { "A", "B" }));
            Assert.That(record.Attributes["scope"], Is.EqualTo("whole"));
            Assert.That(record.Result["backupId"], Is.EqualTo("b1"));
        });
    }

    [Test]
    public void GrainContext_returns_the_injected_context()
    {
        var grain = CreateGrain();

        Assert.That(grain.GrainContext, Is.Not.Null);
    }

    [Test]
    public async Task Begin_records_a_queued_operation_and_indexes_it()
    {
        var grain = CreateGrain();

        var result = await grain.BeginAsync(Begin("test.kind", "A", "B"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Created, Is.True);
            Assert.That(result.Record.OperationId, Is.EqualTo(OperationId));
            Assert.That(result.Record.TenantId, Is.EqualTo(Tenant));
            Assert.That(result.Record.State, Is.EqualTo(LatticeOperationState.Queued));
            Assert.That(result.Record.Phase, Is.EqualTo(LatticeOperationPhaseNames.Queued));
            Assert.That(result.Record.PhaseCount, Is.EqualTo(2));
            Assert.That(result.Record.TreeIds, Is.EqualTo(new[] { "tree-a" }));
            Assert.That(result.Record.StartedAtUtc, Is.EqualTo(_clock.GetUtcNow()));
            Assert.That(_state.WriteCount, Is.EqualTo(2), "The record/outbox and index acknowledgement are persisted separately.");
        });
        await _index.Received(1).ReconcileAsync(
            Arg.Is<LatticeOperationRecord>(r => r.OperationId == OperationId && r.State == LatticeOperationState.Queued), false);
    }

    [Test]
    public async Task Begin_again_with_the_same_kind_is_idempotent()
    {
        var grain = CreateGrain();
        var first = await grain.BeginAsync(Begin());
        _clock.Advance(TimeSpan.FromSeconds(5));

        var second = await grain.BeginAsync(Begin());

        Assert.Multiple(() =>
        {
            Assert.That(second.Created, Is.False);
            Assert.That(second.Record.StartedAtUtc, Is.EqualTo(first.Record.StartedAtUtc));
        });
    }

    [Test]
    public async Task Begin_again_with_a_different_kind_is_refused()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind"));

        Assert.That(
            async () => await grain.BeginAsync(Begin("other.kind")),
            Throws.InvalidOperationException.With.Message.Contains("other.kind"));
    }

    [Test]
    public async Task Report_moves_to_running_and_derives_the_phase_index()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind", "A", "B"));

        var stop = await grain.ReportAsync(new LatticeOperationProgressReport("B", 3, 10, "entries"));
        var record = await grain.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(stop, Is.False);
            Assert.That(record!.State, Is.EqualTo(LatticeOperationState.Running));
            Assert.That(record.Phase, Is.EqualTo("B"));
            Assert.That(record.PhaseIndex, Is.EqualTo(1));
            Assert.That(record.CompletedUnits, Is.EqualTo(3));
            Assert.That(record.TotalUnits, Is.EqualTo(10));
            Assert.That(record.UnitName, Is.EqualTo("entries"));
        });
    }

    [Test]
    public async Task Report_of_an_undeclared_phase_has_no_index()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind", "A"));

        await grain.ReportAsync(new LatticeOperationProgressReport("Elsewhere", 0, null, null));

        Assert.That((await grain.GetAsync())!.PhaseIndex, Is.Null);
    }

    [Test]
    public async Task Report_never_lets_the_total_fall_below_the_completed_units()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());

        await grain.ReportAsync(new LatticeOperationProgressReport("A", 12, 10, "entries"));

        Assert.That((await grain.GetAsync())!.TotalUnits, Is.EqualTo(12));
    }

    [Test]
    public async Task Report_never_regresses_progress_within_a_phase()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        await grain.ReportAsync(new LatticeOperationProgressReport("A", 7, 10, "entries"));

        await grain.ReportAsync(new LatticeOperationProgressReport("A", 4, 10, "entries"));

        Assert.That((await grain.GetAsync())!.CompletedUnits, Is.EqualTo(7));
    }

    [Test]
    public async Task Report_into_a_new_phase_starts_its_count_afresh()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        await grain.ReportAsync(new LatticeOperationProgressReport("A", 7, 10, "entries"));

        await grain.ReportAsync(new LatticeOperationProgressReport("B", 1, 4, "shards"));

        Assert.That((await grain.GetAsync())!.CompletedUnits, Is.EqualTo(1));
    }

    [Test]
    public async Task Report_and_heartbeat_tell_the_runner_to_stop_once_cancellation_is_requested()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());

        var cancelled = await grain.RequestCancelAsync();
        var reportStop = await grain.ReportAsync(new LatticeOperationProgressReport("A", 1, null, null));
        var heartbeatStop = await grain.HeartbeatAsync();

        Assert.Multiple(() =>
        {
            Assert.That(cancelled!.CancelRequested, Is.True);
            Assert.That(cancelled.State, Is.Not.EqualTo(LatticeOperationState.Cancelled),
                "A cancel request leaves the operation running until the work observes it.");
            Assert.That(reportStop, Is.True);
            Assert.That(heartbeatStop, Is.True);
        });
    }

    [Test]
    public async Task Reports_on_a_missing_or_finished_operation_tell_the_runner_to_stop()
    {
        var grain = CreateGrain();

        Assert.That(await grain.ReportAsync(new LatticeOperationProgressReport("A", 0, null, null)), Is.True);
        Assert.That(await grain.HeartbeatAsync(), Is.True);

        await grain.BeginAsync(Begin());
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());

        Assert.That(await grain.HeartbeatAsync(), Is.True);
    }

    [Test]
    public async Task Complete_records_the_outcome_once_and_marks_the_index()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind", "A"));
        await grain.ReportAsync(new LatticeOperationProgressReport("A", 5, 5, "entries"));
        _clock.Advance(TimeSpan.FromSeconds(30));
        var finishedAt = _clock.GetUtcNow();

        var done = await grain.CompleteAsync(LatticeOperationCompletion.Succeeded(
            "bk-1", new Dictionary<string, string> { ["backupId"] = "bk-1" }));
        _clock.Advance(TimeSpan.FromSeconds(1));
        var again = await grain.CompleteAsync(LatticeOperationCompletion.Failed("late"));

        Assert.Multiple(() =>
        {
            Assert.That(done!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(done.Phase, Is.EqualTo(LatticeOperationPhaseNames.Completed));
            Assert.That(done.PhaseIndex, Is.Null);
            Assert.That(done.FinishedAtUtc, Is.EqualTo(finishedAt));
            Assert.That(done.ResultReference, Is.EqualTo("bk-1"));
            Assert.That(done.Result["backupId"], Is.EqualTo("bk-1"));
            Assert.That(again!.State, Is.EqualTo(LatticeOperationState.Succeeded), "A terminal outcome is final.");
        });
        await _index.Received(1).ReconcileAsync(
            Arg.Is<LatticeOperationRecord>(r => r.FinishedAtUtc == finishedAt), false);
    }

    [Test]
    public async Task Complete_keeps_the_phase_of_a_failure_for_diagnosis()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind", "A", "B"));
        await grain.ReportAsync(new LatticeOperationProgressReport("B", 2, 9, "shards"));

        var failed = await grain.CompleteAsync(LatticeOperationCompletion.Failed("boom"));

        Assert.Multiple(() =>
        {
            Assert.That(failed!.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(failed.FailureReason, Is.EqualTo("boom"));
            Assert.That(failed.Phase, Is.EqualTo("B"));
            Assert.That(failed.PhaseIndex, Is.EqualTo(1));
            Assert.That(failed.CompletedUnits, Is.EqualTo(2));
        });
    }

    [Test]
    public void Complete_with_a_non_terminal_state_is_rejected()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.CompleteAsync(new LatticeOperationCompletion { State = LatticeOperationState.Running }),
            Throws.ArgumentException);
    }

    [Test]
    public async Task Complete_on_a_missing_operation_returns_null()
    {
        Assert.That(await CreateGrain().CompleteAsync(LatticeOperationCompletion.Succeeded()), Is.Null);
    }

    [Test]
    public async Task A_running_operation_whose_runner_silo_is_dead_reads_as_failed()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        await grain.ReportAsync(new LatticeOperationProgressReport("A", 1, 4, "shards"));
        _liveness.IsDead(RunnerSilo).Returns(true);

        var record = await grain.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(record!.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(record.FailureReason, Does.Contain("was lost").And.Contain("not supported"));
            Assert.That(record.CompletedUnits, Is.EqualTo(1), "Progress made before the loss is kept.");
            Assert.That(_state.State.Record!.State, Is.EqualTo(LatticeOperationState.Failed),
                "The failure is persisted, not only reported.");
        });
    }

    [Test]
    public async Task A_running_operation_past_its_heartbeat_lease_reads_as_failed()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        _clock.Advance(_options.HeartbeatLease);

        Assert.That((await grain.GetAsync())!.State, Is.Not.EqualTo(LatticeOperationState.Failed),
            "Exactly at the lease the operation is still live.");

        _clock.Advance(TimeSpan.FromTicks(1));
        var record = await grain.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(record!.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(record.FailureReason, Does.Contain("stopped reporting"));
        });
    }

    [Test]
    public async Task A_heartbeat_renews_the_lease()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        _clock.Advance(TimeSpan.FromMinutes(1.5));
        await grain.HeartbeatAsync();
        _clock.Advance(TimeSpan.FromMinutes(1.5));

        Assert.That((await grain.GetAsync())!.State, Is.EqualTo(LatticeOperationState.Queued));
    }

    [Test]
    public async Task A_finished_operation_is_pruned_once_its_retention_elapses()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin());
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        _clock.Advance(_options.Retention - TimeSpan.FromTicks(1));

        Assert.That(await grain.GetAsync(), Is.Not.Null);

        _clock.Advance(TimeSpan.FromTicks(1));

        Assert.That(await grain.GetAsync(), Is.Null);
        await _index.Received(1).ReconcileAsync(Arg.Any<LatticeOperationRecord>(), true);
    }

    [Test]
    public async Task A_pruned_id_can_be_begun_again()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind"));
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        _clock.Advance(_options.Retention);

        var again = await grain.BeginAsync(Begin("other.kind"));

        Assert.That(again.Created, Is.True);
    }

    [Test]
    public async Task Cancel_of_a_missing_operation_returns_null_and_of_a_finished_one_is_unchanged()
    {
        var grain = CreateGrain();
        Assert.That(await grain.RequestCancelAsync(), Is.Null);

        await grain.BeginAsync(Begin());
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded());
        var cancelled = await grain.RequestCancelAsync();

        Assert.Multiple(() =>
        {
            Assert.That(cancelled!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(cancelled.CancelRequested, Is.False);
        });
    }
}
