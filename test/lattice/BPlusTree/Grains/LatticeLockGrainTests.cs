using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Fast, runtime-free unit tests for <see cref="LatticeLockGrain"/> (issue #1608).
/// The grain is constructed directly with substitute Orleans seams and driven
/// through a controllable <see cref="TimeProvider"/> and a capturing one-shot
/// timer factory, so lease expiry and wait-timeout - which in production fire off
/// grain timers - are simulated deterministically without a cluster.
/// </summary>
[TestFixture]
public sealed class LatticeLockGrainTests
{
    private const string LockName = "orders/42";
    private static readonly DateTimeOffset T0 = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed class MutableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    private sealed class CapturedTimer(string purpose, Func<CancellationToken, Task> callback) : IDisposable
    {
        public string Purpose => purpose;
        public Func<CancellationToken, Task> Callback => callback;
        public bool Disposed { get; private set; }
        public void Dispose() => Disposed = true;
    }

    private sealed class Harness
    {
        public required LatticeLockGrain Grain { get; init; }
        public required FakePersistentState<LatticeLockState> State { get; init; }
        public required MutableClock Clock { get; init; }
        public required List<CapturedTimer> Timers { get; init; }

        public Task FireLatestAsync(string purpose)
        {
            for (var i = Timers.Count - 1; i >= 0; i--)
            {
                if (Timers[i].Purpose == purpose && !Timers[i].Disposed)
                {
                    return Timers[i].Callback(CancellationToken.None);
                }
            }

            throw new InvalidOperationException($"No live '{purpose}' timer captured.");
        }
    }

    private static Harness CreateGrain(FakePersistentState<LatticeLockState>? existingState = null, LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice-lock", LockName));

        var reminderRegistry = Substitute.For<IReminderRegistry>();

        var opts = options ?? new LatticeOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(opts);
        optionsMonitor.Get(Arg.Any<string>()).Returns(opts);

        var state = existingState ?? new FakePersistentState<LatticeLockState>();
        var clock = new MutableClock(T0);
        var timers = new List<CapturedTimer>();

        var grain = new LatticeLockGrain(
            context,
            reminderRegistry,
            optionsMonitor,
            new LoggerFactory().CreateLogger<LatticeLockGrain>(),
            state)
        {
            Clock = clock,
            TimerFactory = (purpose, callback, _) =>
            {
                var timer = new CapturedTimer(purpose, callback);
                timers.Add(timer);
                return timer;
            },
        };

        return new Harness { Grain = grain, State = state, Clock = clock, Timers = timers };
    }

    private static LockAcquireRequest Request(double leaseSeconds = 30, double maxWaitSeconds = 0) =>
        new(TimeSpan.FromSeconds(leaseSeconds), TimeSpan.FromSeconds(maxWaitSeconds));

    // --- Acquire / basic grant ---

    [Test]
    public async Task AcquireAsync_when_free_grants_the_first_fencing_token()
    {
        var h = CreateGrain();

        var lease = await h.Grain.AcquireAsync(Request());

        Assert.That(lease.Token.FencingToken, Is.EqualTo(1));
        Assert.That(h.State.State.IsHeld, Is.True);
        Assert.That(h.State.State.HolderToken, Is.EqualTo(1));
    }

    [Test]
    public void AcquireAsync_rejects_negative_max_wait()
    {
        var h = CreateGrain();
        Assert.That(async () => await h.Grain.AcquireAsync(new LockAcquireRequest(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(-1))),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void AcquireAsync_non_blocking_on_held_lock_times_out()
    {
        var h = CreateGrain();
        _ = h.Grain.AcquireAsync(Request()).GetAwaiter().GetResult();

        Assert.That(async () => await h.Grain.AcquireAsync(Request(maxWaitSeconds: 0)),
            Throws.InstanceOf<TimeoutException>());
    }

    // --- TryAcquire ---

    [Test]
    public async Task TryAcquireAsync_when_free_returns_a_lease()
    {
        var h = CreateGrain();
        var lease = await h.Grain.TryAcquireAsync(TimeSpan.FromSeconds(30));
        Assert.That(lease, Is.Not.Null);
        Assert.That(lease!.Value.Token.FencingToken, Is.EqualTo(1));
    }

    [Test]
    public async Task TryAcquireAsync_when_held_returns_null()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request());

        var lease = await h.Grain.TryAcquireAsync(TimeSpan.FromSeconds(30));
        Assert.That(lease, Is.Null);
    }

    // --- FIFO fairness ---

    [Test]
    public async Task AcquireAsync_grants_waiters_in_strict_fifo_order()
    {
        var h = CreateGrain();
        var a = await h.Grain.AcquireAsync(Request());

        var bTask = h.Grain.AcquireAsync(Request(maxWaitSeconds: 60));
        var cTask = h.Grain.AcquireAsync(Request(maxWaitSeconds: 60));
        Assert.That(bTask.IsCompleted, Is.False);
        Assert.That(cTask.IsCompleted, Is.False);

        await h.Grain.ReleaseAsync(a.Token);
        var b = await bTask;
        Assert.That(cTask.IsCompleted, Is.False, "C must not be granted before B releases.");

        await h.Grain.ReleaseAsync(b.Token);
        var c = await cTask;

        Assert.That(b.Token.FencingToken, Is.EqualTo(2));
        Assert.That(c.Token.FencingToken, Is.EqualTo(3));
    }

    [Test]
    public async Task AcquireAsync_mints_strictly_increasing_tokens_across_cycles()
    {
        var h = CreateGrain();

        var a = await h.Grain.AcquireAsync(Request());
        await h.Grain.ReleaseAsync(a.Token);
        var b = await h.Grain.AcquireAsync(Request());
        await h.Grain.ReleaseAsync(b.Token);
        var c = await h.Grain.AcquireAsync(Request());

        Assert.That(a.Token.FencingToken, Is.LessThan(b.Token.FencingToken));
        Assert.That(b.Token.FencingToken, Is.LessThan(c.Token.FencingToken));
    }

    // --- Renew ---

    [Test]
    public async Task RenewAsync_extends_the_current_holder_lease()
    {
        var h = CreateGrain();
        var a = await h.Grain.AcquireAsync(Request(leaseSeconds: 30));
        h.Clock.Advance(TimeSpan.FromSeconds(10));

        var renewed = await h.Grain.RenewAsync(a.Token, TimeSpan.FromSeconds(30));

        Assert.That(renewed.Token.FencingToken, Is.EqualTo(a.Token.FencingToken));
        Assert.That(renewed.ExpiresAt, Is.EqualTo(T0 + TimeSpan.FromSeconds(40)));
    }

    [Test]
    public async Task RenewAsync_rejects_a_stale_token()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request());

        Assert.That(async () => await h.Grain.RenewAsync(new LockToken(999), TimeSpan.FromSeconds(30)),
            Throws.InstanceOf<LatticeLockConflictException>());
    }

    // --- Release ---

    [Test]
    public async Task ReleaseAsync_frees_the_lock_for_the_next_acquirer()
    {
        var h = CreateGrain();
        var a = await h.Grain.AcquireAsync(Request());

        await h.Grain.ReleaseAsync(a.Token);

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False);
    }

    [Test]
    public async Task ReleaseAsync_with_a_stale_token_is_a_silent_no_op()
    {
        var h = CreateGrain();
        var a = await h.Grain.AcquireAsync(Request());

        await h.Grain.ReleaseAsync(new LockToken(999));

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.True);
        Assert.That(status.CurrentFencingToken, Is.EqualTo(a.Token.FencingToken));
    }

    // --- Lease reclamation ---

    [Test]
    public async Task Lease_expiry_reclaims_and_grants_the_next_waiter()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request(leaseSeconds: 30));
        var bTask = h.Grain.AcquireAsync(Request(maxWaitSeconds: 600));
        Assert.That(bTask.IsCompleted, Is.False);

        h.Clock.Advance(TimeSpan.FromSeconds(31));
        await h.FireLatestAsync("lease");

        var b = await bTask;
        Assert.That(b.Token.FencingToken, Is.EqualTo(2));
    }

    // --- Wait-timeout ---

    [Test]
    public async Task AcquireAsync_times_out_a_waiter_that_never_reaches_the_head()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request());
        var bTask = h.Grain.AcquireAsync(Request(maxWaitSeconds: 5));

        await h.FireLatestAsync("waiter-timeout");

        Assert.That(async () => await bTask, Throws.InstanceOf<TimeoutException>());
        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.QueueDepth, Is.EqualTo(0), "A timed-out waiter must be removed from the queue.");
    }

    // --- Status ---

    [Test]
    public async Task GetStatusAsync_on_a_free_lock_reports_not_held()
    {
        var h = CreateGrain();
        var status = await h.Grain.GetStatusAsync();

        Assert.That(status.IsHeld, Is.False);
        Assert.That(status.LeaseExpiresAt, Is.Null);
        Assert.That(status.QueueDepth, Is.EqualTo(0));
    }

    [Test]
    public async Task GetStatusAsync_reports_the_holder_and_queue_depth()
    {
        var h = CreateGrain();
        var a = await h.Grain.AcquireAsync(Request());
        _ = h.Grain.AcquireAsync(Request(maxWaitSeconds: 600));

        var status = await h.Grain.GetStatusAsync();

        Assert.That(status.IsHeld, Is.True);
        Assert.That(status.CurrentFencingToken, Is.EqualTo(a.Token.FencingToken));
        Assert.That(status.LeaseExpiresAt, Is.Not.Null);
        Assert.That(status.QueueDepth, Is.EqualTo(1));
    }

    // --- Crash / reactivation ---

    [Test]
    public async Task Reactivation_over_persisted_state_resumes_the_holder_and_fencing_counter()
    {
        var shared = new FakePersistentState<LatticeLockState>();
        var first = CreateGrain(shared);
        var a = await first.Grain.AcquireAsync(Request());

        // A brand-new activation over the same persisted state (simulating a crash
        // and reactivation) must resume the current holder and never rewind the
        // fencing sequence.
        var second = CreateGrain(shared);
        var status = await second.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.True);
        Assert.That(status.CurrentFencingToken, Is.EqualTo(a.Token.FencingToken));

        await second.Grain.ReleaseAsync(a.Token);
        var next = await second.Grain.AcquireAsync(Request());
        Assert.That(next.Token.FencingToken, Is.GreaterThan(a.Token.FencingToken));
    }

    // --- Deactivation ---

    [Test]
    public async Task OnDeactivateAsync_faults_in_flight_waiters()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request());
        var bTask = h.Grain.AcquireAsync(Request(maxWaitSeconds: 600));

        await h.Grain.OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"), CancellationToken.None);

        Assert.That(async () => await bTask, Throws.InstanceOf<OperationCanceledException>());
    }
}
