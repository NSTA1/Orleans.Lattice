using System.Reflection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Reminder-dispatch, lease-clamp and fault-containment coverage for
/// <see cref="LatticeLockGrain"/>, split from the main fixture by concern.
/// <para>
/// The arms here are the ones no happy-path acquire/release exercises: the
/// durable keepalive reminder that reclaims a crashed holder's lease when no
/// live acquirer is present to drive reclamation, the retention-TTL contract the
/// lock deliberately opts out of, the lease-duration cap, and the four
/// swallow-and-log paths that must keep a reminder-service hiccup from failing a
/// user's lock operation. Each is a containment boundary: if one stopped
/// swallowing, a transient reminder-table fault would surface as a failed
/// acquire rather than as degraded-but-correct lazy reclamation.
/// </para>
/// </summary>
public sealed partial class LatticeLockGrainTests
{
    private const string RetentionReminder = "lattice-lock-retention";
    private const string KeepaliveReminder = "lattice-lock-keepalive";

    // --- Retention-TTL contract (the lock opts out) ---

    [Test]
    public async Task The_retention_reminder_runs_the_no_op_cleanup_and_leaves_the_lock_state_intact()
    {
        var h = CreateGrain();
        var lease = await h.Grain.AcquireAsync(Request());

        // The lock grain's OnTtlExpiredAsync is deliberately a no-op: the lock
        // persists for the life of the lock name, so a retention tick must never
        // clear the fencing counter or the current holder.
        await h.Grain.ReceiveReminder(RetentionReminder, new TickStatus());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.IsHeld, Is.True);
            Assert.That(h.State.State.HolderToken, Is.EqualTo(lease.Token.FencingToken));
        });
    }

    [Test]
    public void ResolveTtl_is_infinite_so_the_retention_reminder_is_never_registered()
    {
        var h = CreateGrain();

        // ResolveTtl is a protected override with no production call site - the
        // lock grain never calls SlideTtlAsync - so it is reached by reflection.
        // The assertion is still load-bearing: were it to return a finite TTL,
        // TtlGrain would register the retention reminder and a lock's persisted
        // fencing counter would be cleaned up underneath live holders.
        var resolveTtl = typeof(LatticeLockGrain).GetMethod(
            "ResolveTtl", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(resolveTtl, Is.Not.Null, "ResolveTtl must remain overridden on the lock grain.");

        var ttl = (TimeSpan)resolveTtl!.Invoke(h.Grain, null)!;

        Assert.That(ttl, Is.EqualTo(Timeout.InfiniteTimeSpan),
            "The lock grain must opt out of the retention TTL entirely.");
    }

    // --- Keepalive reminder dispatch ---

    [Test]
    public async Task An_unrecognised_reminder_is_ignored_without_touching_the_lock()
    {
        var h = CreateGrain();
        var lease = await h.Grain.AcquireAsync(Request());
        h.ReminderRegistry.ClearReceivedCalls();

        await h.Grain.ReceiveReminder("some-other-reminder", new TickStatus());

        var status = await h.Grain.GetStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(status.IsHeld, Is.True);
            Assert.That(status.CurrentFencingToken, Is.EqualTo(lease.Token.FencingToken));
        });
        await h.ReminderRegistry.DidNotReceive().GetReminder(Arg.Any<GrainId>(), Arg.Any<string>());
    }

    [Test]
    public async Task The_keepalive_tick_leaves_a_live_lease_held_and_keeps_the_reminder_registered()
    {
        var h = CreateGrain();
        var lease = await h.Grain.AcquireAsync(Request(leaseSeconds: 30));
        h.ReminderRegistry.ClearReceivedCalls();

        // Well inside the lease: the tick must not reclaim, must not unregister
        // the keepalive, and must not deactivate.
        h.Clock.Advance(TimeSpan.FromSeconds(5));
        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        var status = await h.Grain.GetStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(status.IsHeld, Is.True);
            Assert.That(status.CurrentFencingToken, Is.EqualTo(lease.Token.FencingToken));
        });
        await h.ReminderRegistry.DidNotReceive().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task The_keepalive_tick_reclaims_a_crashed_holder_lease_and_retires_the_reminder()
    {
        var reminder = Substitute.For<IGrainReminder>();
        var h = CreateGrain();
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .Returns(Task.FromResult(reminder));

        await h.Grain.AcquireAsync(Request(leaseSeconds: 30));

        // The holder crashed: nobody releases, and no live acquirer is queued to
        // drive lazy reclamation. The durable keepalive is the only thing that
        // can free the lock, which is the whole reason it exists.
        h.Clock.Advance(TimeSpan.FromSeconds(31));
        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False, "An expired lease must be reclaimed by the keepalive tick.");
        await h.ReminderRegistry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    [Test]
    public async Task The_keepalive_tick_hands_a_reclaimed_lease_to_a_queued_waiter()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request(leaseSeconds: 30));
        var queued = h.Grain.AcquireAsync(Request(maxWaitSeconds: 600));
        Assert.That(queued.IsCompleted, Is.False);

        h.Clock.Advance(TimeSpan.FromSeconds(31));
        await h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus());

        var granted = await queued;
        Assert.That(granted.Token.FencingToken, Is.EqualTo(2),
            "Reclaiming under the keepalive must dispatch the head FIFO waiter, not merely free the lock.");
    }

    // --- Lease-expiry timer with an empty queue ---

    [Test]
    public async Task The_lease_timer_retires_the_keepalive_when_the_lease_expires_with_no_waiters()
    {
        var reminder = Substitute.For<IGrainReminder>();
        var h = CreateGrain();
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .Returns(Task.FromResult(reminder));

        await h.Grain.AcquireAsync(Request(leaseSeconds: 30));

        h.Clock.Advance(TimeSpan.FromSeconds(31));
        await h.FireLatestAsync("lease");

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False);
        await h.ReminderRegistry.Received().UnregisterReminder(Arg.Any<GrainId>(), reminder);
    }

    // --- Lease clamp ---

    [Test]
    public async Task A_requested_lease_longer_than_the_configured_maximum_is_capped()
    {
        var options = new LatticeOptions
        {
            DefaultLockLeaseDuration = TimeSpan.FromSeconds(30),
            MaxLockLeaseDuration = TimeSpan.FromSeconds(60),
        };
        var h = CreateGrain(options: options);

        var lease = await h.Grain.AcquireAsync(Request(leaseSeconds: 3600));

        Assert.Multiple(() =>
        {
            Assert.That(lease.LeaseDuration, Is.EqualTo(TimeSpan.FromSeconds(60)),
                "A caller must not be able to hold a lock beyond MaxLockLeaseDuration.");
            Assert.That(lease.ExpiresAt, Is.EqualTo(T0 + TimeSpan.FromSeconds(60)));
        });
    }

    [Test]
    public async Task A_lease_at_or_below_the_configured_maximum_is_granted_verbatim()
    {
        var options = new LatticeOptions
        {
            DefaultLockLeaseDuration = TimeSpan.FromSeconds(30),
            MaxLockLeaseDuration = TimeSpan.FromSeconds(60),
        };
        var h = CreateGrain(options: options);

        var lease = await h.Grain.AcquireAsync(Request(leaseSeconds: 45));

        Assert.That(lease.LeaseDuration, Is.EqualTo(TimeSpan.FromSeconds(45)),
            "The cap must clamp only what exceeds it, never shorten a conforming lease.");
    }

    // --- Waiter-timeout raced by a grant ---

    [Test]
    public async Task A_waiter_timeout_that_fires_after_the_grant_does_not_disturb_the_granted_holder()
    {
        var h = CreateGrain();
        var first = await h.Grain.AcquireAsync(Request());
        var queued = h.Grain.AcquireAsync(Request(maxWaitSeconds: 5));

        // Capture the waiter's timeout timer before the grant disposes it, so the
        // already-settled race can be replayed deterministically.
        var timeoutTimer = h.Timers.Last(t => t.Purpose == "waiter-timeout");

        await h.Grain.ReleaseAsync(first.Token);
        var granted = await queued;

        // The timer callback loses the race and must no-op rather than fault the
        // holder that has already been granted the lock.
        await timeoutTimer.Callback(CancellationToken.None);

        var status = await h.Grain.GetStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(status.IsHeld, Is.True);
            Assert.That(status.CurrentFencingToken, Is.EqualTo(granted.Token.FencingToken));
            Assert.That(status.QueueDepth, Is.EqualTo(0));
        });
    }

    // --- Double-grant guard ---

    [Test]
    public async Task The_dispatch_guard_refuses_to_grant_a_second_holder_while_the_lock_is_held()
    {
        var h = CreateGrain();
        var holder = await h.Grain.AcquireAsync(Request());
        var queued = h.Grain.AcquireAsync(Request(maxWaitSeconds: 600));

        // DispatchNextAsync is the single grant point, and its guard is the last
        // line of defence for mutual exclusion: every production call site
        // pre-checks that the lock is free, so the guard is only reachable
        // directly. Were it removed and any call site's precondition regress, the
        // lock would mint a second live fencing token for the same lock name.
        var dispatch = typeof(LatticeLockGrain).GetMethod(
            "DispatchNextAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.That(dispatch, Is.Not.Null);

        await (Task)dispatch!.Invoke(h.Grain, [h.Clock.GetUtcNow().UtcTicks])!;

        Assert.That(queued.IsCompleted, Is.False, "The queued acquirer must stay parked while the lock is held.");
        var status = await h.Grain.GetStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(status.CurrentFencingToken, Is.EqualTo(holder.Token.FencingToken),
                "No second fencing token may be minted while the lock is held.");
            Assert.That(status.QueueDepth, Is.EqualTo(1));
        });
    }

    // --- Reminder-service fault containment ---

    [Test]
    public async Task A_failing_keepalive_registration_does_not_fail_the_acquire()
    {
        var h = CreateGrain();
        h.ReminderRegistry
            .RegisterOrUpdateReminder(Arg.Any<GrainId>(), KeepaliveReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));

        // The keepalive is a crash-recovery optimisation, not a correctness
        // requirement: lazy reclamation on the next operation still applies, so a
        // reminder-table outage must degrade rather than deny the lock.
        var lease = await h.Grain.AcquireAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(lease.Token.FencingToken, Is.EqualTo(1));
            Assert.That(h.State.State.IsHeld, Is.True);
        });
    }

    [Test]
    public async Task A_failing_keepalive_lookup_does_not_fail_the_release()
    {
        var h = CreateGrain();
        var lease = await h.Grain.AcquireAsync(Request());
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));

        await h.Grain.ReleaseAsync(lease.Token);

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False,
            "A failed keepalive de-registration must never strand the lock as held.");
    }

    [Test]
    public async Task A_failing_keepalive_unregister_does_not_fail_the_release()
    {
        var reminder = Substitute.For<IGrainReminder>();
        var h = CreateGrain();
        var lease = await h.Grain.AcquireAsync(Request());
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .Returns(Task.FromResult(reminder));
        h.ReminderRegistry.UnregisterReminder(Arg.Any<GrainId>(), reminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));

        await h.Grain.ReleaseAsync(lease.Token);

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False);
    }

    [Test]
    public async Task A_keepalive_tick_survives_a_failing_reminder_lookup()
    {
        var h = CreateGrain();
        await h.Grain.AcquireAsync(Request(leaseSeconds: 30));
        h.ReminderRegistry.GetReminder(Arg.Any<GrainId>(), KeepaliveReminder)
            .ThrowsAsync(new InvalidOperationException("reminder table unavailable"));

        h.Clock.Advance(TimeSpan.FromSeconds(31));
        Assert.DoesNotThrowAsync(() => h.Grain.ReceiveReminder(KeepaliveReminder, new TickStatus()));

        var status = await h.Grain.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False, "The lease must still be reclaimed despite the reminder-table fault.");
    }
}
