using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the bounded retry loops on
/// <see cref="StaleShardRoutingException"/> and
/// <see cref="StaleTreeRoutingException"/> in
/// <c>AtomicWriteGrain.PrepareAsync</c> and
/// <c>AtomicWriteGrain.MarkOneShardAsync</c>. The pre-fix code did a
/// single-shot retry on prepare (catching only
/// <see cref="StaleShardRoutingException"/>) and a single-shot retry
/// on the terminal broadcast (catching both, but still single-shot).
/// A reshard storm — a 4-to-8 grow that produces multiple sequential
/// ShardMap swaps under a single in-flight saga — generates more
/// stale-routing throws than a single retry can absorb, so the
/// exception escapes the saga and surfaces as
/// "round=N: unknown-round (..., other&gt;0)" in the chaos test
/// <c>ReshardTopologyTests.Continuous_reader_observes_zero_or_all_keys
/// _through_mid_saga_reshard</c>.
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task PrepareAsync_retries_on_repeated_StaleShardRoutingException_until_success()
    {
        // First two GetRawEntryAsync calls throw StaleShardRoutingException;
        // third succeeds. The pre-fix code threw the second exception out
        // of the saga. With the bounded retry budget (4 attempts), the
        // saga absorbs both and proceeds.
        var (grain, _, _, lattice, shard) = CreateGrain();
        var attempts = 0;
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(_ =>
            {
                if (attempts++ < 2)
                    throw new StaleShardRoutingException(0, 1, 0);
                return Task.FromResult<LwwEntry?>(null);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(3),
            "PrepareAsync must retry on each StaleShardRoutingException and succeed on the third attempt.");
        // Routing was refreshed once per stale-routing throw on top of
        // the initial fetch (3 prepare-side calls) plus once by
        // BroadcastTerminalsAsync for its drift-correction pass.
        await lattice.Received(4).GetRoutingAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PrepareAsync_retries_on_StaleTreeRoutingException_during_alias_swap()
    {
        // The pre-fix code never caught StaleTreeRoutingException at the
        // prepare-phase GetRawEntryAsync call site, so an alias swap
        // mid-prepare (online resize, online reshard) produced an
        // unhandled exception. Verify the new bounded retry loop catches
        // it and refreshes routing.
        var (grain, _, _, lattice, shard) = CreateGrain();
        var attempts = 0;
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(_ =>
            {
                if (attempts++ < 1)
                    throw new StaleTreeRoutingException(
                        logicalTreeId: TreeId,
                        stalePhysicalTreeId: TreeId,
                        destinationPhysicalTreeId: $"{TreeId}/resized/op-x");
                return Task.FromResult<LwwEntry?>(null);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(2));
        // Prepare-side: initial fetch + one refresh for the stale-tree
        // throw. Plus one for BroadcastTerminalsAsync's drift-correction
        // pass. Three calls in total.
        await lattice.Received(3).GetRoutingAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void PrepareAsync_surfaces_StaleShardRoutingException_after_budget_exhausted()
    {
        // Confirm the retry budget is finite — a pathological storm of
        // stale-routing throws beyond the configured budget surfaces
        // the last exception to the caller rather than retrying
        // forever.
        var (grain, _, _, _, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Throws(new StaleShardRoutingException(0, 1, 0));

        var ex = Assert.ThrowsAsync<StaleShardRoutingException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));
        Assert.That(ex, Is.Not.Null);
    }

    [Test]
    public async Task PrepareAsync_handles_mixed_stale_routing_exceptions_within_budget()
    {
        // Real-world reshard storm: one StaleShardRoutingException
        // (split commit) followed by one StaleTreeRoutingException
        // (alias swap from online reshard). The fix catches both
        // within the same retry loop.
        var (grain, _, _, lattice, shard) = CreateGrain();
        var attempts = 0;
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(_ =>
            {
                attempts++;
                if (attempts == 1)
                    throw new StaleShardRoutingException(0, 1, 0);
                if (attempts == 2)
                    throw new StaleTreeRoutingException(
                        logicalTreeId: TreeId,
                        stalePhysicalTreeId: TreeId,
                        destinationPhysicalTreeId: $"{TreeId}/v2");
                return Task.FromResult<LwwEntry?>(null);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(3));
        // Prepare-side: initial fetch + one refresh per stale-routing
        // throw (mixed shard / tree). Plus one for
        // BroadcastTerminalsAsync's drift-correction pass.
        await lattice.Received(4).GetRoutingAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BroadcastTerminals_retries_on_repeated_stale_routing_until_success()
    {
        // Saga's per-shard terminal broadcast under repeated
        // stale-routing throws (sequential split commits during a
        // reshard window). Pre-fix: one-shot retry — second throw
        // escapes the saga's CompleteSagaAsync path and the registry
        // ForgetAsync never runs, leaking the saga's persisted state.
        var (grain, _, _, _, shard) = CreateGrain();
        // Prepare phase succeeds normally.
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        // Two of the per-shard terminal append calls throw stale-routing
        // before the third succeeds. AppendTxTerminalAsync is shard-keyed
        // and the same mocked shard substitute is returned for every
        // factory.GetGrain&lt;IShardRootGrain&gt;(...) call, so all three
        // attempts route through this single configured Returns.
        var attempts = 0;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (attempts++ < 2)
                    throw new StaleShardRoutingException(0, 1, 0);
                return Task.CompletedTask;
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(3),
            "MarkOneShardAsync must retry each stale-routing throw and succeed within the budget.");
    }

    [Test]
    public async Task BroadcastTerminals_retries_on_StaleTreeRoutingException()
    {
        // Online resize alias swap landing between prepare and
        // terminal broadcast: target shard throws
        // StaleTreeRoutingException because the source physical tree
        // has been retired. Refresh routing once per throw and retry
        // against the new physical tree.
        var (grain, _, _, _, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        var attempts = 0;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (attempts++ < 1)
                    throw new StaleTreeRoutingException(
                        logicalTreeId: TreeId,
                        stalePhysicalTreeId: TreeId,
                        destinationPhysicalTreeId: $"{TreeId}/v2");
                return Task.CompletedTask;
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(2));
    }

    [Test]
    public async Task PrepareAsync_absorbs_many_sequential_stale_routing_throws_within_deadline()
    {
        // The retry budget is wall-clock (60 seconds), not attempt-count
        // bounded. A reshard storm under CI load can produce many
        // sequential ShardMap / alias swaps under a single in-flight
        // saga, each retiring one stale-routing throw. The pre-fix
        // 4-attempt budget surfaced
        // StaleShardRoutingException / StaleTreeRoutingException out of
        // the saga under that storm; the deadline-bounded loop absorbs
        // an unbounded number of swaps so long as forward progress is
        // made. Verify by replaying 16 sequential stale-routing throws
        // (a mix of both exception kinds, modelling a 4-to-8 reshard
        // followed by an alias swap) and asserting all are absorbed.
        var (grain, _, _, lattice, shard) = CreateGrain();
        var attempts = 0;
        const int Storm = 16;
        shard.GetRawEntryAsync(Arg.Any<string>())
            .Returns(_ =>
            {
                if (attempts < Storm)
                {
                    attempts++;
                    if ((attempts & 1) == 1)
                        throw new StaleShardRoutingException(0, 1, 0);
                    throw new StaleTreeRoutingException(
                        logicalTreeId: TreeId,
                        stalePhysicalTreeId: TreeId,
                        destinationPhysicalTreeId: $"{TreeId}/v{attempts}");
                }
                attempts++;
                return Task.FromResult<LwwEntry?>(null);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(Storm + 1),
            "Deadline-bounded retry must absorb every sequential stale-routing throw and succeed on the next attempt.");
        // Routing was refreshed once per stale-routing throw on top of
        // the initial fetch (Storm + 1 prepare-side calls), plus one
        // by BroadcastTerminalsAsync's drift-correction pass.
        await lattice.Received(Storm + 2).GetRoutingAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task BroadcastTerminals_absorbs_many_sequential_stale_routing_throws_within_deadline()
    {
        // Same invariant as the prepare-side regression test, applied
        // to MarkOneShardAsync's retry loop. The chaos-test failure on
        // ResizeTopologyTests.Continuous_reader_observes_zero_or_all
        // _keys_through_mid_saga_resize was a StaleTreeRoutingException
        // escape from the saga's terminal broadcast; verify the
        // deadline-bounded loop absorbs an unbounded number of alias
        // swaps and split commits between prepare and broadcast.
        var (grain, _, _, _, shard) = CreateGrain();
        shard.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));

        var attempts = 0;
        const int Storm = 12;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (attempts < Storm)
                {
                    attempts++;
                    if ((attempts & 1) == 1)
                        throw new StaleShardRoutingException(0, 1, 0);
                    throw new StaleTreeRoutingException(
                        logicalTreeId: TreeId,
                        stalePhysicalTreeId: TreeId,
                        destinationPhysicalTreeId: $"{TreeId}/v{attempts}");
                }
                attempts++;
                return Task.CompletedTask;
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(attempts, Is.EqualTo(Storm + 1),
            "Deadline-bounded retry on MarkOneShardAsync must absorb every sequential stale-routing throw.");
    }
}
