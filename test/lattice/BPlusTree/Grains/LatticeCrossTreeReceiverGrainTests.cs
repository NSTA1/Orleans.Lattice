using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="LatticeCrossTreeReceiverGrain"/>, the
/// receiver-side cross-tree visibility barrier. Drives the wait-set gate
/// against synthesized per-tree terminals so the all-or-nothing flip, frozen
/// wait-set validation, partial-replication scoping, abort verdict, and
/// idempotent redelivery paths are exercised without a silo.
/// </summary>
[TestFixture]
public class LatticeCrossTreeReceiverGrainTests
{
    private const string Origin = "cluster-a";
    private const string OperationId = "xop-recv-1";

    private static (LatticeCrossTreeReceiverGrain grain, FakePersistentState<CrossTreeReceiverState> state) CreateGrain(
        FakePersistentState<CrossTreeReceiverState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("cross-tree-receiver", LatticeCrossTreeReceiverGrain.ComputeKey(Origin, OperationId)));

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var state = existingState ?? new FakePersistentState<CrossTreeReceiverState>();
        var grain = new LatticeCrossTreeReceiverGrain(
            context, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<LatticeCrossTreeReceiverGrain>(), state);
        return (grain, state);
    }

    private static CrossTreeReceiverTerminal Terminal(
        string treeId, bool committed, IReadOnlyList<string> waitSet, params int[] shards) => new()
    {
        OriginClusterId = Origin,
        OperationId = OperationId,
        TreeId = treeId,
        TransactionId = Guid.NewGuid(),
        Committed = committed,
        WaitSet = waitSet,
        ObservedSourceShards = shards.Length == 0 ? new[] { 1 } : shards,
        TerminalHlc = HybridLogicalClock.Zero,
    };

    [Test]
    public async Task NotifyTerminalAsync_first_of_two_trees_is_in_flight()
    {
        var (grain, _) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };

        var decision = await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));

        Assert.That(decision.Decided, Is.False, "barrier must stay pending until every wait-set tree arrives");
        Assert.That(decision.TreesToFinalize, Is.Empty);
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task NotifyTerminalAsync_completes_when_every_tree_arrives_committed()
    {
        var (grain, _) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };

        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));
        var decision = await grain.NotifyTerminalAsync(Terminal("inventory", committed: true, waitSet));

        Assert.That(decision.Decided, Is.True);
        Assert.That(decision.Committed, Is.True);
        Assert.That(decision.TreesToFinalize.Select(t => t.TreeId),
            Is.EquivalentTo(new[] { "orders", "inventory" }));
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task NotifyTerminalAsync_any_abort_makes_the_global_verdict_aborted()
    {
        var (grain, _) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };

        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));
        var decision = await grain.NotifyTerminalAsync(Terminal("inventory", committed: false, waitSet));

        Assert.That(decision.Decided, Is.True);
        Assert.That(decision.Committed, Is.False, "any participating tree's abort aborts the whole cross-tree batch");
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task NotifyTerminalAsync_single_tree_wait_set_completes_on_first_terminal()
    {
        // Partial replication: only one participant tree is replicated here, so
        // the wait set is a singleton and the barrier flips on the first
        // terminal - the cross-tree batch is still valid on the present subset.
        var (grain, _) = CreateGrain();
        var waitSet = new[] { "orders" };

        var decision = await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));

        Assert.That(decision.Decided, Is.True);
        Assert.That(decision.Committed, Is.True);
        Assert.That(decision.TreesToFinalize.Single().TreeId, Is.EqualTo("orders"));
    }

    [Test]
    public void NotifyTerminalAsync_rejects_a_terminal_whose_wait_set_differs()
    {
        var (grain, _) = CreateGrain();

        // First terminal freezes the wait set.
        grain.NotifyTerminalAsync(Terminal("orders", committed: true, new[] { "orders", "inventory" })).GetAwaiter().GetResult();

        // A later terminal carrying a different wait set is a protocol drift.
        Assert.ThrowsAsync<InvalidOperationException>(() =>
            grain.NotifyTerminalAsync(Terminal("inventory", committed: true, new[] { "orders", "inventory", "ledger" })));
    }

    [Test]
    public void NotifyTerminalAsync_rejects_a_terminal_whose_tree_is_absent_from_the_wait_set()
    {
        var (grain, _) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };
        grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet)).GetAwaiter().GetResult();

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            grain.NotifyTerminalAsync(Terminal("ledger", committed: true, waitSet)));
    }

    [Test]
    public async Task NotifyTerminalAsync_is_idempotent_on_redelivery_after_decided()
    {
        var (grain, state) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };
        var ordersTerminal = Terminal("orders", committed: true, waitSet);

        await grain.NotifyTerminalAsync(ordersTerminal);
        await grain.NotifyTerminalAsync(Terminal("inventory", committed: true, waitSet));
        var writesAfterDecision = state.WriteCount;

        // Redelivery of an already-recorded terminal re-heals materialization
        // without re-persisting (the decision is durable).
        var redelivered = await grain.NotifyTerminalAsync(ordersTerminal);

        Assert.That(redelivered.Decided, Is.True);
        Assert.That(redelivered.Committed, Is.True);
        Assert.That(redelivered.TreesToFinalize, Has.Count.EqualTo(2),
            "a redelivered terminal returns the full finalize set so every tree re-heals");
        Assert.That(state.WriteCount, Is.EqualTo(writesAfterDecision),
            "a post-decision redelivery must not write state again");
    }

    [Test]
    public async Task NotifyTerminalAsync_persists_before_returning()
    {
        var (grain, state) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };

        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));

        Assert.That(state.WriteCount, Is.GreaterThanOrEqualTo(1),
            "the arrival must be durable before the notify acks (the registration that precedes it is linearized against it)");
    }

    [Test]
    public async Task NotifyTerminalAsync_wait_set_match_is_order_insensitive()
    {
        var (grain, _) = CreateGrain();

        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, new[] { "orders", "inventory" }));
        // Same set, different order - must be accepted, not treated as drift.
        var decision = await grain.NotifyTerminalAsync(Terminal("inventory", committed: true, new[] { "inventory", "orders" }));

        Assert.That(decision.Decided, Is.True);
        Assert.That(decision.Committed, Is.True);
    }

    [Test]
    public async Task GetDecisionAsync_reflects_durable_state_on_reactivation()
    {
        var (grain, state) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };
        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));
        await grain.NotifyTerminalAsync(Terminal("inventory", committed: true, waitSet));

        // A fresh activation over the same persisted state resolves the verdict.
        var (reactivated, _) = CreateGrain(existingState: state);
        Assert.That(await reactivated.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetDecisionAsync_stays_in_flight_when_the_deciding_write_fails()
    {
        // The decision must not be observable until it is durable: if the
        // completing terminal's WriteStateAsync throws, a concurrent reader
        // (and any redelivery short-circuit) must still see InFlight, so the
        // per-tree registry never durably caches a verdict a crash could lose.
        var (grain, state) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };

        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));
        state.ThrowOnWrite = new InvalidOperationException("simulated storage fault");

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            grain.NotifyTerminalAsync(Terminal("inventory", committed: true, waitSet)));

        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight),
            "a decision whose persist failed must read InFlight, not the in-memory verdict");
    }

    [Test]
    public async Task NotifyTerminalAsync_redelivery_redrives_persist_after_a_failed_deciding_write()
    {
        // After the deciding write fails, the next (redelivered) terminal must
        // re-drive the persist rather than short-circuit on the non-durable
        // in-memory decision - and only then publish the verdict.
        var (grain, state) = CreateGrain();
        var waitSet = new[] { "orders", "inventory" };

        await grain.NotifyTerminalAsync(Terminal("orders", committed: true, waitSet));
        var inventory = Terminal("inventory", committed: true, waitSet);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage fault");
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.NotifyTerminalAsync(inventory));

        // ThrowOnWrite self-cleared after the first throw; the redelivered
        // terminal now persists successfully and publishes the decision.
        var decision = await grain.NotifyTerminalAsync(inventory);

        Assert.That(decision.Decided, Is.True);
        Assert.That(decision.Committed, Is.True);
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));
    }
}
