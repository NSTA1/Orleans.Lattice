using System.Diagnostics;
using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for issue #5775: a replay that holds a per-silo replay permit must not
/// wait on a grain call that itself needs one.
/// <para>
/// Each nested call is modelled at the seam the replay reaches: the stub takes a
/// permit from the real gate, as a cold system-tree leaf or a cold destination leaf
/// would, and gives up after <see cref="NestedPermitWait"/>, as a response timeout
/// would. The gate is sized to one permit, so a replay that keeps its permit across
/// the call starves it. The paths are the registry shard-map lookup, pass 2.5's
/// tx-registry status resolve, and the forward of a replayed commit to the leaf that
/// now declares the key.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly TimeSpan NestedPermitWait = TimeSpan.FromSeconds(2);

    private static async Task TakeNestedReplayPermitAsync(string call, List<string> starved)
    {
        if (BPlusLeafGrain.ReplayConcurrencyGateForTest is not { } gate)
            return;

        if (!await gate.WaitAsync(NestedPermitWait))
        {
            lock (starved)
                starved.Add(call);
            throw new TimeoutException($"The {call} waited {NestedPermitWait} for a replay permit the calling replay held.");
        }

        gate.Release();
    }

    [Test]
    [NonParallelizable]
    public async Task A_cold_leafs_replay_does_not_hold_its_permit_across_the_registry_shard_map_lookup()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var starved = new List<string>();
            var lookups = 0;
            IGrainFactory? grainFactory = null;
            var coordinator = BuildCoordinator(head: 1, new CommitLogSliceEntry(0, BuildCommittedSet("a", [1])));
            var (grain, _, _, _) = CreateGrainWithMaterialiser(
                coordinator,
                persistedCheckpoint: -1,
                maxConcurrentReplays: 1,
                seedState: s => s.ShardIndex = 0,
                configureGrainFactory: factory => grainFactory = factory);

            // Installed after the options resolver has wired its own registry stub.
            var registry = grainFactory!.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            registry.GetShardMapAsync(Arg.Any<string>()).Returns(async _ =>
            {
                Interlocked.Increment(ref lookups);
                await TakeNestedReplayPermitAsync("registry shard-map lookup", starved);
                return (ShardMap?)null;
            });

            var elapsed = Stopwatch.StartNew();
            await ActivateAsync(grain);
            elapsed.Stop();

            Assert.Multiple(() =>
            {
                Assert.That(lookups, Is.GreaterThanOrEqualTo(1), "precondition: the replay resolved its shard map");
                Assert.That(starved, Is.Empty, "the registry's system-tree leaf found a permit while the replay ran");
                Assert.That(elapsed.Elapsed, Is.LessThan(NestedPermitWait), "the replay never waited out a nested call");
                Assert.That(grain.EntriesForTest.ContainsKey("a"), Is.True);
                Assert.That(BPlusLeafGrain.ReplayConcurrencyGateForTest!.CurrentCount, Is.EqualTo(1), "the one permit is back in the gate");
                Assert.That(BPlusLeafGrain.ReplayPermitCensusCountForTest, Is.Zero, "no holder is left in the census");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_cold_leafs_replay_returns_its_permit_for_the_tx_registry_resolve_and_the_commit_forward()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var starved = new List<string>();
            var barrierSatisfiedWhileOut = new List<bool>();
            string? holdersWhileOut = null;
            var txid = Guid.NewGuid();
            var successorId = GrainId.Create("leaf", "replay-permit-nesting-successor");
            var delivered = new List<string>();
            Orleans.Lattice.BPlusTree.State.LeafNodeState? leafState = null;

            var destination = Substitute.For<IBPlusLeafGrain>();
            destination.ApplyPreparedReplayCommitAsync(
                    Arg.Any<Guid>(), Arg.Any<List<PendingMutationSnapshot>>(), Arg.Any<HybridLogicalClock>(), Arg.Any<int>())
                .Returns(async call =>
                {
                    await TakeNestedReplayPermitAsync("commit forward", starved);
                    lock (delivered)
                        delivered.AddRange(call.Arg<List<PendingMutationSnapshot>>().Select(m => m.Key));
                });

            var txRegistry = Substitute.For<ITxRegistryGrain>();
            txRegistry.GetStatusForSettlementAsync(txid).Returns(async _ =>
            {
                await TakeNestedReplayPermitAsync("tx-registry status resolve", starved);

                // A division narrowed the leaf while its replay resolved the saga,
                // so the committed key now belongs to the successor and the commit
                // is forwarded there.
                leafState!.HighKeyExclusive = "m";
                return TxStatus.Committed;
            });

            var coordinator = BuildCoordinator(
                head: 1,
                new CommitLogSliceEntry(0, BuildPreparedSet(txid, "z", Encoding.UTF8.GetBytes("committed"))));
            var (grain, state, _, _) = CreateGrainWithMaterialiser(
                coordinator,
                persistedCheckpoint: -1,
                maxConcurrentReplays: 1,
                seedState: s => s.NextSibling = successorId,
                configureGrainFactory: factory =>
                {
                    factory.GetGrain<IBPlusLeafGrain>(successorId).Returns(destination);
                    factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(txRegistry);
                });
            leafState = state.State;
            BPlusLeafGrain.ReplayPermitSuspendedForTest = leaf =>
            {
                lock (barrierSatisfiedWhileOut)
                {
                    barrierSatisfiedWhileOut.Add(leaf.ReplayBarrierSatisfiedForTest);
                    holdersWhileOut ??= BPlusLeafGrain.DescribeReplayPermitHolders();
                }
            };

            var elapsed = Stopwatch.StartNew();
            await ActivateAsync(grain);
            elapsed.Stop();

            Assert.Multiple(() =>
            {
                Assert.That(starved, Is.Empty, "neither nested call waited on the permit its caller held");
                Assert.That(delivered, Is.EquivalentTo(new[] { "z" }), "the commit was forwarded to the successor");
                Assert.That(elapsed.Elapsed, Is.LessThan(NestedPermitWait), "the replay never waited out a nested call");
                Assert.That(barrierSatisfiedWhileOut, Has.Count.EqualTo(2), "the permit went out once per nested call");
                Assert.That(barrierSatisfiedWhileOut, Has.All.False, "the leaf served nothing while its permit was out");
                Assert.That(holdersWhileOut ?? string.Empty, Does.Contain("permit out on a nested call"), "the census names the holder and its nested call");
                Assert.That(BPlusLeafGrain.ReplayConcurrencyGateForTest!.CurrentCount, Is.EqualTo(1), "the one permit is back in the gate");
                Assert.That(BPlusLeafGrain.ReplayPermitCensusCountForTest, Is.Zero, "no holder is left in the census");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task An_abandoned_drive_whose_permit_is_out_on_a_nested_call_does_not_release_it_twice()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var park = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var wal = new GrowingWal();
            var txid = Guid.NewGuid();
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var txRegistry = Substitute.For<ITxRegistryGrain>();
            txRegistry.GetStatusForSettlementAsync(txid).Returns(async _ =>
            {
                entered.TrySetResult();
                await park.Task;
                return TxStatus.InFlight;
            });
            var (grain, _, _, _) = CreateGrainWithMaterialiser(
                wal.Coordinator,
                persistedCheckpoint: -1,
                maxConcurrentReplays: 1,
                starvationDriveBudget: TimeSpan.FromMilliseconds(400),
                configureGrainFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(txRegistry));
            await ActivateAsync(grain);
            var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
            Assert.That(gate.CurrentCount, Is.EqualTo(1), "precondition: the activation returned its permit");

            // The drive's replay pends a prepare with no terminal, so pass 2.5
            // resolves its saga under the drive's permit; the resolve returns the
            // permit for the call and then outlives the drive's budget.
            wal.GrowTo(1, _ => BuildPreparedSet(txid, "p", Encoding.UTF8.GetBytes("v")));

            var verdict = await grain.DriveStarvedCheckpointAsync();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Multiple(() =>
            {
                Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.TimedOut), "precondition: the drive was abandoned mid-resolve");
                Assert.That(gate.CurrentCount, Is.EqualTo(1),
                    "the permit is in the gate exactly once: the resolve returned it and the abandoned frame did not release it again");
            });

            Assert.That(grain.ReplayPermitsSuspendedForTest, Is.EqualTo(1), "precondition: the detached replay still has the permit out");
            park.TrySetResult();
            var deadline = Stopwatch.StartNew();
            while (grain.ReplayPermitsSuspendedForTest > 0 && deadline.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(20);

            Assert.Multiple(() =>
            {
                Assert.That(grain.ReplayPermitsSuspendedForTest, Is.Zero, "the detached replay's resume completed");
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero, "the resume left the queue");
                Assert.That(gate.CurrentCount, Is.EqualTo(1),
                    "the detached replay re-took the permit after its frame ended and released it, leaving the gate whole");
                Assert.That(BPlusLeafGrain.ReplayPermitCensusCountForTest, Is.Zero, "no holder is left in the census");
            });
        }
        finally
        {
            park.TrySetResult();
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
    [Test]
    [NonParallelizable]
    public void The_no_progress_refusal_names_the_permit_holders()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        Assert.That(BPlusLeafGrain.DescribeReplayPermitHolders(), Does.Contain("Permit holders: none recorded"));
    }
}
