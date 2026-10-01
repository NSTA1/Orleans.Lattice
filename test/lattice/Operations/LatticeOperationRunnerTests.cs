using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Unit tests for <see cref="LatticeOperationRunner"/>, the reusable engine-side
/// coordinator: it starts work in-process and independent of the caller,
/// propagates the engine's own result and exception, records the terminal state,
/// banks progress on a fault, honours cancellation from the grain or a local
/// request, starts nothing for an existing id, and exposes the ambient progress
/// sink. Synchronised with task completion sources and a manual clock only.
/// </summary>
[TestFixture]
public sealed class LatticeOperationRunnerTests
{
    private static readonly SiloAddress LocalSilo = SiloAddress.New(new IPEndPoint(IPAddress.Loopback, 22222), 7);

    private Dictionary<string, FakeOperationGrain> _grains = null!;
    private ILatticeOperationIndexGrain _index = null!;
    private ManualTimeProvider _clock = null!;
    private LatticeOperationRunner _runner = null!;

    [SetUp]
    public void SetUp()
    {
        _grains = new Dictionary<string, FakeOperationGrain>(StringComparer.Ordinal);
        _index = Substitute.For<ILatticeOperationIndexGrain>();
        _clock = new ManualTimeProvider();

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeOperationGrain>(Arg.Any<string>(), null)
            .Returns(call => Grain(call.ArgAt<string>(0)));
        factory.GetGrain<ILatticeOperationIndexGrain>(Arg.Any<string>(), null).Returns(_index);

        var silo = Substitute.For<ILocalSiloDetails>();
        silo.SiloAddress.Returns(LocalSilo);

        _runner = new LatticeOperationRunner(
            factory,
            silo,
            Options.Create(new LatticeOperationOptions { HeartbeatInterval = TimeSpan.FromSeconds(10) }),
            NullLogger<LatticeOperationRunner>.Instance)
        {
            Clock = _clock,
        };
    }

    private FakeOperationGrain Grain(string key)
    {
        if (!_grains.TryGetValue(key, out var grain))
        {
            grain = new FakeOperationGrain(key);
            _grains[key] = grain;
        }

        return grain;
    }

    private FakeOperationGrain GrainFor(string operationId) => Grain(LatticeOperationKey.For("t1", operationId));

    private static LatticeOperationStart Start(string id = "op-1", params string[] phases) =>
        new() { TenantId = "t1", OperationId = id, Kind = "test.kind", TreeIds = ["tree"], Phases = phases };

    [Test]
    public async Task Start_runs_the_work_and_records_its_success()
    {
        var launch = await _runner.StartAsync(
            Start("op-1", "Working"),
            static (_, _) => Task.FromResult(42),
            static result => LatticeOperationCompletion.Succeeded(result.ToString()));

        var result = await launch.Completion!;

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(42));
            Assert.That(launch.Record.State, Is.EqualTo(LatticeOperationState.Queued));
            Assert.That(GrainFor("op-1").BeginRequest!.RunnerSilo, Is.EqualTo(LocalSilo));
            Assert.That(GrainFor("op-1").Reports[0].Phase, Is.EqualTo("Working"), "The first declared phase starts the run.");
            Assert.That(GrainFor("op-1").Completion!.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(GrainFor("op-1").Completion!.ResultReference, Is.EqualTo("42"));
            Assert.That(_runner.RunningCount, Is.Zero);
        });
    }

    [Test]
    public async Task Without_declared_phases_the_kind_names_the_first_phase()
    {
        var launch = await _runner.StartAsync(Start(), static (_, _) => Task.FromResult(0), static _ => LatticeOperationCompletion.Succeeded());
        await launch.Completion!;

        Assert.That(GrainFor("op-1").Reports[0].Phase, Is.EqualTo("test.kind"));
    }

    [Test]
    public async Task Starting_an_existing_id_starts_nothing()
    {
        GrainFor("op-1").Existing = true;
        var invoked = false;

        var launch = await _runner.StartAsync(
            Start(),
            (_, _) => { invoked = true; return Task.FromResult(0); },
            static _ => LatticeOperationCompletion.Succeeded());

        Assert.Multiple(() =>
        {
            Assert.That(launch.Completion, Is.Null);
            Assert.That(invoked, Is.False);
        });
    }

    [Test]
    public async Task A_failing_work_item_records_failure_banks_progress_and_rethrows_its_own_exception()
    {
        var fault = new InvalidDataException("bad artifact");

        var launch = await _runner.StartAsync<int>(
            Start("op-1", "A"),
            async (progress, _) =>
            {
                await progress.ReportAsync("A", 0, 1000, "entries");
                await progress.ReportAsync("A", 7, 1000, "entries");
                throw fault;
            },
            static _ => LatticeOperationCompletion.Succeeded());

        var thrown = Assert.ThrowsAsync<InvalidDataException>(async () => await launch.Completion!);
        var grain = GrainFor("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(fault), "The caller sees the engine's own exception, unchanged.");
            Assert.That(grain.Completion!.State, Is.EqualTo(LatticeOperationState.Failed));
            Assert.That(grain.Completion.FailureReason, Is.EqualTo("InvalidDataException: bad artifact"));
            Assert.That(grain.Reports[^1].CompletedUnits, Is.EqualTo(7),
                "The coalesced report is banked before the failure is recorded.");
        });
    }

    [Test]
    public async Task A_local_cancel_request_cancels_the_work_and_records_cancelled()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var launch = await _runner.StartAsync(
            Start(),
            async (_, ct) =>
            {
                started.SetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return 0;
            },
            static _ => LatticeOperationCompletion.Succeeded());
        await started.Task;

        var record = await _runner.RequestCancelAsync("t1", "op-1");

        Assert.ThrowsAsync<TaskCanceledException>(async () => await launch.Completion!);
        Assert.Multiple(() =>
        {
            Assert.That(record!.CancelRequested, Is.True);
            Assert.That(GrainFor("op-1").Completion!.State, Is.EqualTo(LatticeOperationState.Cancelled));
        });
    }

    [Test]
    public async Task A_stop_signal_on_the_heartbeat_cancels_the_work()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var launch = await _runner.StartAsync(
            Start(),
            async (_, ct) =>
            {
                started.SetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return 0;
            },
            static _ => LatticeOperationCompletion.Succeeded());
        await started.Task;
        GrainFor("op-1").StopOnHeartbeat = true;

        await TestPoll.UntilAsync(() => _clock.PendingTimerCount > 0, "the heartbeat delay to be armed");
        _clock.Advance(TimeSpan.FromSeconds(10));

        Assert.ThrowsAsync<TaskCanceledException>(async () => await launch.Completion!);
        Assert.That(GrainFor("op-1").Completion!.State, Is.EqualTo(LatticeOperationState.Cancelled));
    }

    [Test]
    public async Task The_work_sees_the_sink_ambiently_and_the_callers_execution_context()
    {
        var callerValue = new AsyncLocal<string?> { Value = "caller-credential" };
        ILatticeOperationProgress? ambient = null;
        string? seen = null;

        var launch = await _runner.StartAsync(
            Start(),
            (progress, _) =>
            {
                ambient = LatticeOperationProgress.Current;
                seen = callerValue.Value;
                return Task.FromResult(ReferenceEquals(ambient, progress));
            },
            static _ => LatticeOperationCompletion.Succeeded());

        var same = await launch.Completion!;

        Assert.Multiple(() =>
        {
            Assert.That(same, Is.True, "The ambient sink is the one handed to the work.");
            Assert.That(seen, Is.EqualTo("caller-credential"));
            Assert.That(LatticeOperationProgress.Current, Is.Null, "The sink is never ambient outside the work.");
        });
    }

    [Test]
    public void Start_validates_its_arguments()
    {
        Func<ILatticeOperationProgress, CancellationToken, Task<int>> work = static (_, _) => Task.FromResult(0);
        Func<int, LatticeOperationCompletion> done = static _ => LatticeOperationCompletion.Succeeded();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await _runner.StartAsync(null!, work, done), Throws.ArgumentNullException);
            Assert.That(async () => await _runner.StartAsync(Start(), null!, done), Throws.ArgumentNullException);
            Assert.That(async () => await _runner.StartAsync(Start(), work, null!), Throws.ArgumentNullException);
            Assert.That(async () => await _runner.StartAsync(Start("bad/id"), work, done), Throws.ArgumentException);
        });
    }

    [Test]
    public async Task Reads_of_a_malformed_id_report_not_found_without_reaching_a_grain()
    {
        Assert.Multiple(async () =>
        {
            Assert.That(await _runner.GetAsync("t1", "bad/id"), Is.Null);
            Assert.That(await _runner.RequestCancelAsync("t1", "bad/id"), Is.Null);
        });
        Assert.That(_grains, Is.Empty);
    }

    [Test]
    public async Task List_hydrates_the_index_page_and_skips_pruned_operations()
    {
        GrainFor("live").Record = Record("live");
        _index.ListAsync("test.", "token", 5).Returns(new LatticeOperationIndexPage(["live", "pruned"], "next"));

        var (records, next) = await _runner.ListAsync("t1", "test.", "token", 5);

        Assert.Multiple(() =>
        {
            Assert.That(records.Select(r => r.OperationId), Is.EqualTo(new[] { "live" }));
            Assert.That(next, Is.EqualTo("next"));
        });
    }

    private static LatticeOperationRecord Record(string id) => new()
    {
        OperationId = id,
        Kind = "test.kind",
        TenantId = "t1",
        State = LatticeOperationState.Running,
        Phase = "A",
    };

    /// <summary>An in-memory stand-in for one operation grain.</summary>
    private sealed class FakeOperationGrain(string key) : ILatticeOperationGrain
    {
        public bool Existing { get; set; }

        public bool StopOnHeartbeat { get; set; }

        public LatticeOperationBeginRequest? BeginRequest { get; private set; }

        public List<LatticeOperationProgressReport> Reports { get; } = [];

        public LatticeOperationCompletion? Completion { get; private set; }

        public LatticeOperationRecord? Record { get; set; }

        public Task<LatticeOperationBeginResult> BeginAsync(LatticeOperationBeginRequest request)
        {
            BeginRequest = request;
            var (tenant, id) = LatticeOperationKey.Parse(key);
            Record ??= new LatticeOperationRecord
            {
                OperationId = id,
                Kind = request.Kind,
                TenantId = tenant,
                State = LatticeOperationState.Queued,
                Phase = LatticeOperationPhaseNames.Queued,
            };
            return Task.FromResult(new LatticeOperationBeginResult(!Existing, Record));
        }

        public Task<bool> ReportAsync(LatticeOperationProgressReport report)
        {
            lock (Reports)
            {
                Reports.Add(report);
            }

            return Task.FromResult(Record?.CancelRequested ?? false);
        }

        public Task<bool> HeartbeatAsync() => Task.FromResult(StopOnHeartbeat);

        public Task<LatticeOperationRecord?> CompleteAsync(LatticeOperationCompletion completion)
        {
            Completion = completion;
            return Task.FromResult(Record);
        }

        public Task<LatticeOperationRecord?> GetAsync() => Task.FromResult(Record);

        public Task<LatticeOperationRecord?> RequestCancelAsync()
        {
            if (Record is not null)
            {
                Record = Record with { CancelRequested = true };
            }

            return Task.FromResult(Record);
        }
    }
}
