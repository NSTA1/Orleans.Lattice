using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3592. Every new <c>LatticeGrain</c> worker calls
/// <see cref="TombstoneCompactionGrain.EnsureReminderAsync"/> on its first
/// write, and the call registered the compaction reminder unconditionally.
/// <c>RegisterOrUpdateReminder</c> restarts a schedule at <c>now + dueTime</c>,
/// so each new worker postponed the tree's next pass by a full period. On a
/// tree that gets a new worker more often than once a period - one silo restart
/// a day is enough at the default 24 h grace - the reminder never fires and
/// tombstones are never reaped.
/// <para>
/// These fixtures drive the grain against a stateful model of the reminder
/// table: a registration stores <c>StartAt = now + dueTime</c> exactly as Orleans
/// does, so a re-registration is visible as a moved <c>StartAt</c> rather than
/// only as a call count.
/// </para>
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    private const string CompactionReminder = "tombstone-compaction";

    [Test]
    public async Task EnsureReminder_does_not_move_an_existing_schedule()
    {
        var clock = new ManualTimeProvider();
        var (grain, _, reminderRegistry, _, _) = CreateGrain();
        var table = new ReminderTableModel(reminderRegistry, clock);

        await grain.EnsureReminderAsync();
        var registered = table.Rows[CompactionReminder];
        Assert.That(registered.StartAt, Is.EqualTo(clock.GetUtcNow() + TimeSpan.FromHours(24)),
            "precondition: the first call must register the reminder one grace period out.");

        // Three more workers ask, an hour apart, all well inside one period.
        for (var worker = 0; worker < 3; worker++)
        {
            clock.Advance(TimeSpan.FromHours(1));
            await grain.EnsureReminderAsync();
        }

        Assert.Multiple(() =>
        {
            reminderRegistry.Received(1).RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), CompactionReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
            Assert.That(table.Rows[CompactionReminder], Is.EqualTo(registered),
                "a reminder that already exists must keep its schedule. Re-registering it moved the next "
                + "pass to one period after each new worker's first write, so a tree that kept getting new "
                + "workers never compacted.");
        });
    }

    [Test]
    public async Task EnsureReminder_registers_again_once_the_reminder_is_gone()
    {
        // Tree recovery calls EnsureReminderAsync after a delete unregistered the
        // reminder, so "register only when absent" must still register then.
        var clock = new ManualTimeProvider();
        var (grain, _, reminderRegistry, _, _) = CreateGrain();
        var table = new ReminderTableModel(reminderRegistry, clock);

        await grain.EnsureReminderAsync();
        await grain.UnregisterReminderAsync();
        Assert.That(table.Rows.ContainsKey(CompactionReminder), Is.False,
            "precondition: unregistering must have removed the reminder.");

        clock.Advance(TimeSpan.FromHours(1));
        await grain.EnsureReminderAsync();

        Assert.Multiple(() =>
        {
            reminderRegistry.Received(2).RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), CompactionReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
            Assert.That(table.Rows[CompactionReminder].StartAt,
                Is.EqualTo(clock.GetUtcNow() + TimeSpan.FromHours(24)),
                "an absent reminder must be registered afresh, one grace period out.");
        });
    }

    [Test]
    public async Task A_changed_grace_period_is_applied_by_the_next_tick_not_by_EnsureReminder()
    {
        // The fix relies on this path. EnsureReminderAsync can no longer apply a
        // changed period, because it cannot read the registered one
        // (IGrainReminder exposes only its name) and re-registering would move
        // the schedule. The next tick reports the period it fired under, and
        // ReceiveReminder re-registers when that differs from the configured one.
        var clock = new ManualTimeProvider();
        var options = new LatticeOptions { TombstoneGracePeriod = TimeSpan.FromHours(1) };
        var (grain, _, reminderRegistry, grainFactory, _) = CreateGrain(options);
        StubEmptyShardTopology(grainFactory);
        var table = new ReminderTableModel(reminderRegistry, clock);

        await grain.EnsureReminderAsync();
        var firstTick = table.Rows[CompactionReminder].StartAt;

        options.TombstoneGracePeriod = TimeSpan.FromHours(2);
        clock.Advance(TimeSpan.FromMinutes(10));
        await grain.EnsureReminderAsync();

        Assert.That(table.Rows[CompactionReminder],
            Is.EqualTo((firstTick, TimeSpan.FromHours(1))),
            "EnsureReminderAsync must leave the existing schedule alone even when the period changed.");

        clock.Advance(firstTick - clock.GetUtcNow());
        var tick = new TickStatus(firstTick.UtcDateTime, TimeSpan.FromHours(1), clock.GetUtcNow().UtcDateTime);

        // ReceiveReminder goes on to start a pass, which needs the grain timer
        // service this harness does not have. Only the drift check before it is
        // under test.
        try { await grain.ReceiveReminder(CompactionReminder, tick); }
        catch (InvalidOperationException) { }

        Assert.That(table.Rows[CompactionReminder],
            Is.EqualTo((clock.GetUtcNow() + TimeSpan.FromHours(2), TimeSpan.FromHours(2))),
            "the first tick under the old period must re-register the reminder under the new one.");
    }

    /// <summary>
    /// Serves the registry and shard roots a pass started by
    /// <see cref="TombstoneCompactionGrain.ReceiveReminder"/> reads, all empty.
    /// </summary>
    private static void StubEmptyShardTopology(IGrainFactory grainFactory)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(Task.FromResult<ShardMap?>(
            ShardMap.CreateDefault(4, ShardCount)));
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        for (var i = 0; i < ShardCount; i++)
        {
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shard);
        }
    }

    /// <summary>
    /// A reminder table behind a substitute <see cref="IReminderRegistry"/>.
    /// Registering stores <c>StartAt = now + dueTime</c> and replaces any row of
    /// the same name, as Orleans' <c>RegisterOrUpdateReminder</c> does.
    /// </summary>
    private sealed class ReminderTableModel
    {
        public ReminderTableModel(IReminderRegistry registry, TimeProvider clock)
        {
            registry.RegisterOrUpdateReminder(
                    Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
                .Returns(call =>
                {
                    var name = call.ArgAt<string>(1);
                    Rows[name] = (clock.GetUtcNow() + call.ArgAt<TimeSpan>(2), call.ArgAt<TimeSpan>(3));
                    return Task.FromResult(Reminder(name));
                });

            registry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
                .Returns(call => Task.FromResult<IGrainReminder?>(
                    Rows.ContainsKey(call.ArgAt<string>(1)) ? Reminder(call.ArgAt<string>(1)) : null));

            registry.UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>())
                .Returns(call =>
                {
                    Rows.Remove(call.ArgAt<IGrainReminder>(1).ReminderName);
                    return Task.CompletedTask;
                });
        }

        /// <summary>The registered reminders, by name.</summary>
        public Dictionary<string, (DateTimeOffset StartAt, TimeSpan Period)> Rows { get; } = [];

        private static IGrainReminder Reminder(string name)
        {
            var reminder = Substitute.For<IGrainReminder>();
            reminder.ReminderName.Returns(name);
            return reminder;
        }
    }
}
