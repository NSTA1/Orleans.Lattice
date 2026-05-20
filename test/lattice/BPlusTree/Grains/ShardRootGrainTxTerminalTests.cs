using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the saga terminal-mark primitive on
/// <see cref="ShardRootGrain"/>. The load-bearing invariant is that
/// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> stamps the
/// outgoing <c>TxCommit</c> / <c>TxAbort</c> WAL record with an HLC
/// strictly greater than every prepare's stamp on this shard's leaf
/// chain. Cross-cluster receivers merge inbound records by HLC across
/// WAL partitions, so a regression to <see cref="HybridLogicalClock.Zero"/>
/// stamping would always sort the terminal ahead of non-Zero prepares
/// and flush an empty pending bucket on too-early arrival.
/// </summary>
[TestFixture]
public class ShardRootGrainTxTerminalTests
{
    private const string TreeId = "tx-tree";
    private const int ShardIndex = 0;
    private static string ShardKey => $"{TreeId}/{ShardIndex}";

    private sealed class CapturingCommitLogWriter : ICommitLogWriter
    {
        public List<LatticeMutation> Appended { get; } = [];
        public Task<long> AppendAsync(LatticeMutation mutation, CancellationToken cancellationToken = default)
        {
            Appended.Add(mutation);
            return Task.FromResult((long)Appended.Count - 1);
        }
        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<LatticeMutation> mutations, CancellationToken cancellationToken = default)
        {
            var offsets = new long[mutations.Count];
            for (var i = 0; i < mutations.Count; i++)
            {
                Appended.Add(mutations[i]);
                offsets[i] = Appended.Count - 1;
            }
            return Task.FromResult<IReadOnlyList<long>>(offsets);
        }
    }

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required CapturingCommitLogWriter? Writer { get; init; }
        public required IReadOnlyList<IBPlusLeafGrain> Leaves { get; init; }
    }

    /// <summary>
    /// Builds a <see cref="ShardRootGrain"/> wired to a configurable
    /// leaf chain. Each entry in <paramref name="leafClocks"/> becomes a
    /// substituted <see cref="IBPlusLeafGrain"/> returning that HLC from
    /// <see cref="IBPlusLeafGrain.GetClockAsync"/>; the chain is linked
    /// via successive <c>GetNextSiblingAsync</c> returns terminating in
    /// <see langword="null"/>. When <paramref name="registerWriter"/> is
    /// true, an in-memory <see cref="CapturingCommitLogWriter"/> is
    /// resolved through the activation services so the WAL append path
    /// is observable; otherwise the resolution returns
    /// <see langword="null"/> and the WAL path is skipped.
    /// </summary>
    private static Harness CreateHarness(
        IReadOnlyList<HybridLogicalClock> leafClocks,
        bool registerWriter = true,
        bool emptyChain = false)
    {
        CapturingCommitLogWriter? writer = registerWriter ? new CapturingCommitLogWriter() : null;

        var sc = new ServiceCollection();
        if (writer is not null)
            sc.AddSingleton<ICommitLogWriter>(writer);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<ShardRootState>();
        if (!emptyChain)
        {
            state.State.RootNodeId = GrainId.Create("leaf", $"{TreeId}-leaf-0");
            state.State.RootIsLeaf = true;
        }
        else
        {
            // Pre-populate state so EnsureRootAsync is a no-op (avoids
            // calling GetGrainId() on a substitute, which requires a
            // real grain reference). The AppendTxTerminalAsync pre-flight
            // runs PrepareForOperationAsync -> EnsureRootAsync on every
            // entry; pre-populating RootNodeId makes that early-return,
            // so the test exercises the post-root-init code path
            // (BroadcastTerminalToLeavesAsync) without requiring
            // grain-extension support on the substitute. The bug shape
            // itself - silent-skip on a null RootNodeId during the
            // backstop branch - is exercised end-to-end by the reshard
            // chaos suite where the real cluster actually creates leaves.
            state.State.RootNodeId = GrainId.Create("leaf", $"{TreeId}-empty-leaf-sentinel");
            state.State.RootIsLeaf = true;
            state.State.IsRegistered = true;
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(Substitute.For<ILeafCacheGrain>());

        var leaves = new List<IBPlusLeafGrain>();
        if (!emptyChain && leafClocks.Count > 0)
        {
            for (var i = 0; i < leafClocks.Count; i++)
            {
                var leaf = Substitute.For<IBPlusLeafGrain>();
                leaf.GetClockAsync().Returns(Task.FromResult(leafClocks[i]));
                leaf.ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>()).Returns(Task.CompletedTask);
                var nextId = i + 1 < leafClocks.Count
                    ? (GrainId?)GrainId.Create("leaf", $"{TreeId}-leaf-{i + 1}")
                    : null;
                leaf.GetNextSiblingAsync().Returns(Task.FromResult(nextId));
                leaves.Add(leaf);

                var leafId = i == 0
                    ? state.State.RootNodeId!.Value
                    : GrainId.Create("leaf", $"{TreeId}-leaf-{i}");
                factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);
            }
        }
        else if (emptyChain)
        {
            // For the empty-chain harness, register a sentinel leaf at
            // the pre-populated RootNodeId. The leaf has no siblings,
            // so the chain walk completes in one step. Its
            // GetClockAsync returns Zero, so the terminal HLC ticks
            // to a non-Zero value (the existing assertion).
            var sentinelLeaf = Substitute.For<IBPlusLeafGrain>();
            sentinelLeaf.GetClockAsync().Returns(Task.FromResult(HybridLogicalClock.Zero));
            sentinelLeaf.ApplyTxTerminalAsync(
                Arg.Any<Guid>(),
                Arg.Any<bool>(),
                Arg.Any<IReadOnlyDictionary<string, byte[]>?>())
                .Returns(Task.CompletedTask);
            sentinelLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
            factory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value).Returns(sentinelLeaf);
        }

        var resolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var grain = new ShardRootGrain(
            context, state, factory, resolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Writer = writer, Leaves = leaves };
    }

    // --- Terminal HLC ordering invariant ---

    [Test]
    public async Task AppendTxTerminalAsync_stamps_terminal_HLC_strictly_greater_than_every_leaf_clock()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 100, Counter = 5 },
            new HybridLogicalClock { WallClockTicks = 200, Counter = 3 },
            new HybridLogicalClock { WallClockTicks = 150, Counter = 9 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        Assert.That(h.Writer!.Appended, Has.Count.EqualTo(1));
        var terminal = h.Writer.Appended[0];
        var maxLeaf = HybridLogicalClock.Zero;
        foreach (var c in leafClocks) if (c > maxLeaf) maxLeaf = c;
        Assert.That(terminal.Timestamp, Is.GreaterThan(maxLeaf),
            $"terminal HLC {terminal.Timestamp} must be strictly greater than max leaf HLC {maxLeaf}");
    }

    [Test]
    public async Task AppendTxTerminalAsync_stamps_non_zero_terminal_HLC_when_all_leaves_at_zero()
    {
        var leafClocks = new[] { HybridLogicalClock.Zero, HybridLogicalClock.Zero };
        var h = CreateHarness(leafClocks);

        await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);

        Assert.That(h.Writer!.Appended, Has.Count.EqualTo(1));
        Assert.That(h.Writer.Appended[0].Timestamp, Is.GreaterThan(HybridLogicalClock.Zero));
    }

    // --- Override propagation ---

    [Test]
    public async Task AppendTxTerminalAsync_honors_HlcOverrideContext_verbatim_over_chain_max()
    {
        // Even though leaf clocks are very high, an explicit override
        // (the receiver-side relay path) must be stamped verbatim so
        // the receiver's local WAL matches the source cluster's HLC
        // bit-identically.
        var leafClocks = new[] { new HybridLogicalClock { WallClockTicks = 9999, Counter = 99 } };
        var h = CreateHarness(leafClocks);
        var overrideHlc = new HybridLogicalClock { WallClockTicks = 42, Counter = 7 };

        using (LatticeHlcOverrideContext.With(overrideHlc))
        {
            await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);
        }

        Assert.That(h.Writer!.Appended, Has.Count.EqualTo(1));
        Assert.That(h.Writer.Appended[0].Timestamp, Is.EqualTo(overrideHlc));
    }

    // --- Empty chain ---

    [Test]
    public async Task AppendTxTerminalAsync_with_empty_chain_stamps_Tick_of_Zero()
    {
        var h = CreateHarness(leafClocks: [], emptyChain: true);

        await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: false);

        Assert.That(h.Writer!.Appended, Has.Count.EqualTo(1));
        Assert.That(h.Writer.Appended[0].Timestamp, Is.GreaterThan(HybridLogicalClock.Zero));
    }

    // --- WAL writer optionality ---

    [Test]
    public async Task AppendTxTerminalAsync_skips_WAL_append_when_no_writer_registered()
    {
        var leafClocks = new[] { new HybridLogicalClock { WallClockTicks = 100, Counter = 0 } };
        var h = CreateHarness(leafClocks, registerWriter: false);
        var txid = Guid.NewGuid();

        // Should complete cleanly without a writer; the leaf fan-out
        // remains the sole delivery channel.
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        Assert.That(h.Writer, Is.Null);
        await h.Leaves[0].Received(1).ApplyTxTerminalAsync(txid, true);
    }

    // --- Leaf fan-out ---

    [Test]
    public async Task AppendTxTerminalAsync_fans_out_terminal_to_every_leaf()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 30, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        foreach (var leaf in h.Leaves)
            await leaf.Received(1).ApplyTxTerminalAsync(txid, true);
    }

    // --- Mutation kind selection ---

    [Test]
    public async Task AppendTxTerminalAsync_stamps_TxCommit_when_committed_true()
    {
        var h = CreateHarness([new HybridLogicalClock { WallClockTicks = 50, Counter = 0 }]);
        await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: true);
        Assert.That(h.Writer!.Appended[0].Kind, Is.EqualTo(MutationKind.TxCommit));
    }

    [Test]
    public async Task AppendTxTerminalAsync_stamps_TxAbort_when_committed_false()
    {
        var h = CreateHarness([new HybridLogicalClock { WallClockTicks = 50, Counter = 0 }]);
        await h.Grain.AppendTxTerminalAsync(Guid.NewGuid(), committed: false);
        Assert.That(h.Writer!.Appended[0].Kind, Is.EqualTo(MutationKind.TxAbort));
    }

    // --- Typed shard-index slot ---

    [Test]
    public async Task AppendTxTerminalAsync_stamps_typed_ShardIndex_slot_on_terminal_mutation()
    {
        var h = CreateHarness([new HybridLogicalClock { WallClockTicks = 50, Counter = 0 }]);
        var txid = Guid.NewGuid();

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        var terminal = h.Writer!.Appended[0];
        Assert.Multiple(() =>
        {
            Assert.That(terminal.ShardIndex, Is.EqualTo(ShardIndex));
            Assert.That(terminal.TransactionId, Is.EqualTo(txid));
            Assert.That(terminal.IsPrepared, Is.False);
            Assert.That(terminal.TreeId, Is.EqualTo(TreeId));
        });
    }

    // --- Empty-guid no-op ---

    [Test]
    public async Task AppendTxTerminalAsync_with_empty_transactionId_is_a_no_op()
    {
        var h = CreateHarness([new HybridLogicalClock { WallClockTicks = 50, Counter = 0 }]);

        await h.Grain.AppendTxTerminalAsync(Guid.Empty, committed: true);

        Assert.That(h.Writer!.Appended, Is.Empty);
        await h.Leaves[0].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());
    }

    // --- Affected-leaves tracking ---

    /// <summary>
    /// When prepare-phase writes routed through this shard recorded a
    /// proper subset of the chain (e.g. the saga touched 1 of 4 leaves),
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> must fan
    /// Channel 2 only to that subset and skip the untouched leaves
    /// entirely so a wide tree does not pay an activation-pressure
    /// spike per saga.
    /// </summary>
    [Test]
    public async Task AppendTxTerminalAsync_fans_out_only_to_recorded_affected_leaves()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 30, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 40, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        // Simulate a prepare-phase write that touched leaf #2 only,
        // recorded via the prepared-context gate. The hook lives on
        // the routing layer so we drive it through the same Set call
        // a real saga would issue.
        // Here we directly invoke the public surface that records:
        // a SetAsync issued under both contexts will route through
        // TraverseForWriteAsync and call RecordAffectedLeafIfPrepared.
        // The tree state has RootIsLeaf=true with leaf #0 as root,
        // so the routing layer always records leaf #0 in this harness.
        // To exercise a *subset* we issue a single prepare-routed
        // write through the public ShardRoot surface, then drive the
        // terminal.
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k1", new byte[] { 1 });
        }

        // Now drive the terminal. Channel 2 must fan only to the
        // recorded leaves (#0 in this single-leaf-routing harness),
        // not the full chain.
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        await h.Leaves[0].Received(1).ApplyTxTerminalAsync(txid, true);
        // Leaves #1..#3 are part of the chain but were never recorded
        // as affected - they must NOT receive the terminal RPC.
        await h.Leaves[1].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());
        await h.Leaves[2].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());
        await h.Leaves[3].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());
    }

    /// <summary>
    /// The terminal-HLC computation must run only over the recorded
    /// affected leaves - untouched leaves contribute no prepare for
    /// this saga and so cannot influence the per-saga max. Verifying
    /// that <see cref="IBPlusLeafGrain.GetClockAsync"/> is queried
    /// only on the recorded subset is the cheapest way to enforce
    /// the activation-pressure invariant on the clock-collection
    /// path (which would otherwise silently re-broaden if a future
    /// edit forgot to use the same target list for both fan-outs).
    /// </summary>
    [Test]
    public async Task AppendTxTerminalAsync_queries_GetClockAsync_only_on_affected_leaves()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 30, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k1", new byte[] { 1 });
        }

        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        // Leaf #0 (the routed target in this RootIsLeaf harness)
        // is the only recorded affected leaf - it is the only one
        // we should have queried for a clock.
        await h.Leaves[0].Received(1).GetClockAsync();
        await h.Leaves[1].DidNotReceive().GetClockAsync();
        await h.Leaves[2].DidNotReceive().GetClockAsync();
    }

    /// <summary>
    /// When no per-saga affected-leaves entry exists (the shard-root
    /// reactivated mid-saga, or the call arrives via a path that
    /// bypasses the routing layer), the code falls back to walking
    /// the full chain. This test exercises the fallback by issuing
    /// the terminal under a fresh transaction id that was never
    /// recorded - every leaf in the chain must receive the terminal
    /// RPC.
    /// </summary>
    [Test]
    public async Task AppendTxTerminalAsync_falls_back_to_full_chain_when_no_tracking_entry()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 30, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        // No prepare-phase writes recorded. The terminal call must
        // walk the chain and fan to every leaf so a deactivated
        // shard-root does not silently drop the immediate-visibility
        // delivery for an in-flight saga.
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        foreach (var leaf in h.Leaves)
            await leaf.Received(1).ApplyTxTerminalAsync(txid, true);
    }

    /// <summary>
    /// The per-saga affected-leaves entry must be removed after the
    /// terminal completes so the in-memory map cannot grow unboundedly
    /// across the activation lifetime. Verified by issuing a second
    /// terminal under the same transaction id with no intervening
    /// prepare-phase writes - the second call must take the fallback
    /// (full-chain) path, proving the first call evicted the entry.
    /// </summary>
    [Test]
    public async Task AppendTxTerminalAsync_evicts_tracking_entry_after_terminal_completes()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k1", new byte[] { 1 });
        }

        // First terminal - uses the recorded subset (leaf #0 only).
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);
        h.Leaves[0].ClearReceivedCalls();
        h.Leaves[1].ClearReceivedCalls();
        h.Writer!.Appended.Clear();

        // Second terminal under the same txid - entry was evicted, so
        // the call must fall back to the full chain walk and fan to
        // every leaf.
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        foreach (var leaf in h.Leaves)
            await leaf.Received(1).ApplyTxTerminalAsync(txid, true);
    }

    /// <summary>
    /// Writes issued outside the prepared-context gate must not
    /// populate the affected-leaves map - they do not produce a
    /// pending bucket on the leaf and so the terminal-mark fan-out
    /// has no business targeting them. Verified by issuing a regular
    /// (non-prepared) <c>SetAsync</c> followed by a terminal call
    /// under the same ambient transaction id: the absence of tracking
    /// drives the fallback path.
    /// </summary>
    [Test]
    public async Task RecordAffectedLeafIfPrepared_skips_record_when_PreparedContext_is_inactive()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        // Set the txid but DO NOT open a prepared scope - a regular
        // user-driven SetAsync.
        LatticeTransactionContext.Set(txid);
        await h.Grain.SetAsync("k1", new byte[] { 1 });

        // The terminal call must fall back to the full chain because
        // the prior write was not recorded.
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        foreach (var leaf in h.Leaves)
            await leaf.Received(1).ApplyTxTerminalAsync(txid, true);
    }

    /// <summary>
    /// Writes issued under a prepared scope but with no ambient
    /// transaction id (defensive case - the receiver-side prepared
    /// apply seam always sets both, but a stray scope outside that
    /// flow must not leak into the tracking map under
    /// <c>Guid.Empty</c>). Verified by issuing a write under the
    /// scope with the txid left at <c>Guid.Empty</c>, then
    /// confirming the map remains empty by driving a terminal under
    /// a fresh txid and watching the fallback path engage.
    /// </summary>
    [Test]
    public async Task RecordAffectedLeafIfPrepared_skips_record_when_TransactionContext_is_empty()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);

        // Open a prepared scope but never set a non-empty txid.
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k1", new byte[] { 1 });
        }

        // Drive a terminal under a freshly minted txid: tracking is
        // empty, fallback engages.
        var txid = Guid.NewGuid();
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);

        foreach (var leaf in h.Leaves)
            await leaf.Received(1).ApplyTxTerminalAsync(txid, true);
    }

    /// <summary>
    /// Two concurrent sagas (different transaction ids) racing on the
    /// same shard must each retain an independent affected-leaves
    /// subset: saga A's terminal must consume only saga A's tracking
    /// entry and dispatch under saga A's transaction id, leaving saga
    /// B's tracking entry intact for its own subsequent terminal. The
    /// harness's <c>RootIsLeaf=true</c> routes every <c>SetAsync</c>
    /// to leaf #0, so we verify isolation by asserting (a) each
    /// terminal dispatches with the correct transaction id and not
    /// the other's, and (b) leaf #1 never receives a terminal -
    /// confirming the tracked-path was used for both sagas rather
    /// than the fallback chain walk.
    /// </summary>
    [Test]
    public async Task AppendTxTerminalAsync_isolates_affected_leaves_per_transaction()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        // Record saga A's prepare on leaf #0.
        LatticeTransactionContext.Set(txA);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("kA", new byte[] { 1 });
        }

        // Record saga B's prepare on leaf #0 under a different txid.
        LatticeTransactionContext.Set(txB);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("kB", new byte[] { 2 });
        }

        // Drive saga A's terminal - must fan with txA only, and only
        // to the recorded leaf (#0). Leaf #1 must remain untouched
        // (proves tracked path, not fallback).
        await h.Grain.AppendTxTerminalAsync(txA, committed: true);
        await h.Leaves[0].Received(1).ApplyTxTerminalAsync(txA, true);
        await h.Leaves[0].DidNotReceive().ApplyTxTerminalAsync(txB, Arg.Any<bool>());
        await h.Leaves[1].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());

        h.Leaves[0].ClearReceivedCalls();
        h.Leaves[1].ClearReceivedCalls();

        // Saga B's tracking entry must still be present - its terminal
        // must dispatch with txB on leaf #0 only.
        await h.Grain.AppendTxTerminalAsync(txB, committed: true);
        await h.Leaves[0].Received(1).ApplyTxTerminalAsync(txB, true);
        await h.Leaves[0].DidNotReceive().ApplyTxTerminalAsync(txA, Arg.Any<bool>());
        await h.Leaves[1].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());
    }

    // --- Regression: pre-flight must initialize root on freshly-activated shard ---
    //
    // The unit-level pin previously sketched here required GetGrainId() to
    // succeed on an NSubstitute proxy of IBPlusLeafGrain - Orleans'
    // GrainExtensions.GetGrainId rejects bare interface proxies (see the
    // canonical pre-populate-RootNodeId workaround in
    // ShardRootGrainHotnessTests.cs). The end-to-end pin lives in the
    // reshard chaos suite's continuous-reader fixture, which exercises the
    // bug shape (fresh destination shard receives a saga terminal with
    // committedValues whose backstop must reach the destination's leaves)
    // against a real cluster where Orleans' own machinery creates the
    // leaves - the only place the call path is faithfully reproduced.
    // The fix itself (PrepareForOperationAsync replacing
    // ThrowIfTreeRejecting in AppendTxTerminalAsync) is documented at the
    // call site.

    /// <summary>
    /// Mirrors <see cref="AppendTxTerminalAsync_evicts_tracking_entry_after_terminal_completes"/>
    /// for the abort path: the second terminal call under the same
    /// transaction id must take the fallback (full-chain) path,
    /// proving the first call evicted the entry regardless of
    /// outcome. This is a complementary regression for the eviction
    /// behaviour - the implementation must not key eviction on
    /// <c>committed=true</c>.
    /// </summary>
    [Test]
    public async Task AppendTxTerminalAsync_evicts_tracking_entry_after_terminal_aborts()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k1", new byte[] { 1 });
        }

        // First terminal - uses the recorded subset (leaf #0 only).
        await h.Grain.AppendTxTerminalAsync(txid, committed: false);
        h.Leaves[0].ClearReceivedCalls();
        h.Leaves[1].ClearReceivedCalls();
        h.Writer!.Appended.Clear();

        // Second terminal under the same txid - entry was evicted, so
        // the call must fall back to the full chain walk and fan to
        // every leaf.
        await h.Grain.AppendTxTerminalAsync(txid, committed: false);

        foreach (var leaf in h.Leaves)
            await leaf.Received(1).ApplyTxTerminalAsync(txid, false);
    }

    /// <summary>
    /// Interleaving a non-prepared <c>SetAsync</c> (no
    /// <c>LatticePreparedContext</c> scope) with a prepared
    /// <c>SetAsync</c> under the same ambient transaction id must
    /// not pollute the affected-leaves map: only the prepared write
    /// passes the gate, so the subsequent terminal-mark must fan
    /// using the recorded subset (leaf #0 only) rather than fall
    /// back to the full chain walk. Verified in this harness by
    /// asserting leaf #1 receives nothing - fallback would fan to
    /// both leaves, while the tracked path fans only to the leaf
    /// recorded under the prepared write.
    /// </summary>
    [Test]
    public async Task RecordAffectedLeafIfPrepared_isolates_prepared_from_non_prepared_writes()
    {
        var leafClocks = new[]
        {
            new HybridLogicalClock { WallClockTicks = 10, Counter = 0 },
            new HybridLogicalClock { WallClockTicks = 20, Counter = 0 },
        };
        var h = CreateHarness(leafClocks);
        var txid = Guid.NewGuid();

        // First: a non-prepared write - the gate must reject. No
        // ambient txid, no prepared scope.
        await h.Grain.SetAsync("k0", new byte[] { 9 });

        // Then: a prepared write under explicit txid + scope - the
        // gate accepts and records leaf #0 under txid.
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await h.Grain.SetAsync("k1", new byte[] { 1 });
        }

        // Drive the terminal. The recorded subset must contain only
        // leaf #0 (from the prepared write); leaf #1 must remain
        // untouched. Pollution from the earlier non-prepared write
        // would either widen the subset (no - the gate rejects it)
        // or trip the fallback path (no - the prepared entry is
        // present), so the only valid observation is a single
        // dispatch on leaf #0.
        await h.Grain.AppendTxTerminalAsync(txid, committed: true);
        await h.Leaves[0].Received(1).ApplyTxTerminalAsync(txid, true);
        await h.Leaves[1].DidNotReceive().ApplyTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>());
    }
}
