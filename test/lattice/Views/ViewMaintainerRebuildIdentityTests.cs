using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class ViewMaintainerRebuildIdentityTests
{
    private const string Logical = "orders";
    private const string First = "orders-first";
    private const string Second = "orders-second";
    private const string Retired = "orders-retired";

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        FakePersistentState<ViewCheckpointState> State,
        IWalCursorRegistry Cursors,
        ILattice Source,
        ICommitLogReader Reader,
        IWalOffsetConsumerRegistryGrain OffsetRegistry,
        ILatticeRegistry Registry,
        Func<FakePersistentState<ViewCheckpointState>, ViewMaintainerGrain> Reactivate);

    [TestCase(false)]
    [TestCase(true)]
    public async Task Rebuild_source_moves_preserve_the_captured_tuple_or_reject_before_publication(bool afterValidation)
    {
        var h = Create(afterValidation);
        if (afterValidation)
        {
            await h.Grain.RebuildAsync();
        }
        else
        {
            Assert.That(() => h.Grain.RebuildAsync(), Throws.TypeOf<StaleTreeRoutingException>());
        }

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(afterValidation ? 1 : 0),
                "a move observed before validation rejects; a later move must never relabel the captured generation");
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(-1));
        });
        await h.Cursors.DidNotReceive().ReportCursorAsync(Second, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Rebuild_failed_checkpoint_response_reactivation_uses_the_persisted_physical_tuple(bool committed)
    {
        var h = Create(moveSource: false, bound: Retired);
        h.State.State.AppliedOffsets[0] = 7;
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        var durable = new ViewCheckpointState
        {
            BoundPhysicalTreeId = Retired,
            LegacyGenerationCeiling = 0,
            AppliedOffsets = new Dictionary<int, long> { [0] = 7 },
        };
        if (committed)
        {
            h.State.OnWriteState = written =>
            {
                durable = new ViewCheckpointState
                {
                    BoundPhysicalTreeId = written.BoundPhysicalTreeId,
                    ActiveGeneration = written.ActiveGeneration,
                    HighestAppliedTimestamp = written.HighestAppliedTimestamp,
                    LegacyGenerationCeiling = written.LegacyGenerationCeiling,
                    AppliedOffsets = new Dictionary<int, long>(written.AppliedOffsets),
                };
                throw new TimeoutException("checkpoint committed but response lost");
            };
        }
        else
        {
            h.State.ThrowOnWrite = new TimeoutException("checkpoint not committed");
        }

        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RebuildAsync());
        await h.Cursors.DidNotReceive().UnregisterAsync(Retired, Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }),
            "a failed checkpoint response cannot retire the old log's cross-silo offset entitlement");
        var restartedState = new FakePersistentState<ViewCheckpointState> { State = durable };
        var restarted = h.Reactivate(restartedState);
        await ((IGrainBase)restarted).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(restartedState.State.ActiveGeneration, Is.EqualTo(committed ? 1 : 0));
            Assert.That(restartedState.State.BoundPhysicalTreeId, Is.EqualTo(committed ? First : Retired));
            Assert.That(restartedState.State.HighestAppliedTimestamp.WallClockTicks, Is.EqualTo(committed ? 10 : 0));
        });
        Assert.That(await restarted.GetDurableReadPositionsAsync(committed ? First : Retired),
            Is.EqualTo(new long[] { committed ? 0 : 8 }));
        Assert.That(await restarted.GetDurableReadPositionsAsync(committed ? Retired : First), Is.Null,
            "a leftover offset registration on the other log must hold nothing after reactivation");
    }

    [Test]
    public async Task Rebuild_retires_the_old_source_pin_only_after_the_captured_tuple_is_durable()
    {
        var h = Create(moveSource: false, bound: Retired);
        h.State.BeforeWrite = async () =>
        {
            await h.Cursors.DidNotReceive().UnregisterAsync(Retired, Arg.Any<string>(), Arg.Any<CancellationToken>());
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First));
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1));
        };

        await h.Grain.RebuildAsync();

        await h.Cursors.Received(1).UnregisterAsync(Retired, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await h.Cursors.Received(1).ReportCursorAsync(First, Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Rebuild_new_alias_log_is_held_before_registration_and_at_the_captured_floor_before_scanning()
    {
        var h = Create(moveSource: false, bound: Retired);
        h.State.State.AppliedOffsets[0] = 7;
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        var registered = false;
        var captured = false;
        var scanned = false;
        h.OffsetRegistry.RegisterAsync(Arg.Any<GrainId>()).Returns(async _ =>
        {
            registered = true;
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(First), Is.Empty,
                "registration may become observable before its response; the new log must already be held in full");
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
        });
        h.Reader.GetHeadOffsetAsync(First, 0, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            captured = true;
            Assert.That(registered, Is.True);
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(First), Is.Empty,
                "old-log offsets cannot authorize reclamation while new-log heads are captured");
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
            return 2L;
        });
        h.Source.GetWithVersionAsync("k", Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            scanned = true;
            Assert.That(captured, Is.True);
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Retired));
            Assert.That(h.State.State.ActiveGeneration, Is.Zero);
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(First), Is.EqualTo(new long[] { 2 }),
                "the candidate's captured floor must hold every entry appended during its scan");
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
            return new VersionedValue { Value = [1], Version = new HybridLogicalClock { WallClockTicks = 10 } };
        });

        await h.Grain.RebuildAsync();

        Assert.That(registered && captured && scanned, Is.True);
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(First), Is.EqualTo(new long[] { 2 }));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.Null,
            "only a successful durable publication retires the old log entitlement");
    }

    [Test]
    public async Task Rebuild_rejected_candidates_never_replace_the_persisted_bound_log_entitlement()
    {
        var h = Create(moveSource: false, bound: Retired);
        h.State.State.AppliedOffsets[0] = 7;
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        var alias = First;
        h.Registry.GetEntryAsync(Logical).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = alias, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
        foreach (var candidate in new[] { First, Second, "orders-third", First })
        {
            alias = candidate;
            h.Reader.GetHeadOffsetAsync(candidate, 0, Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                Assert.That(await h.Grain.GetDurableReadPositionsAsync(candidate), Is.Empty,
                    "an already registered candidate must regain its full hold before head capture");
                Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }),
                    "the hold belongs to the durable bound identity, never to the last rejected candidate");
                alias = candidate + "-moved";
                return 2L;
            });
            Assert.ThrowsAsync<StaleTreeRoutingException>(async () => await h.Grain.RebuildAsync());
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(Retired));
            Assert.That(h.State.State.ActiveGeneration, Is.Zero);
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(candidate), Is.EqualTo(new long[] { 2 }));
        }
    }

    [Test]
    public async Task Rebuild_failed_publication_then_another_candidate_preserves_the_original_bound_entitlement()
    {
        var h = Create(moveSource: false, bound: Retired);
        h.State.State.AppliedOffsets[0] = 7;
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        h.State.ThrowOnWrite = new TimeoutException("checkpoint not committed");
        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RebuildAsync());
        Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First),
            "the failed write mutated the in-memory tuple, not the durable binding");
        h.Registry.GetEntryAsync(Logical).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = Second, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
        h.Reader.GetHeadOffsetAsync(Second, 0, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
            return 2L;
        });

        h.State.ThrowOnWrite = new TimeoutException("second checkpoint not committed");
        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RebuildAsync());

        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Second), Is.EqualTo(new long[] { 2 }));
    }

    [Test]
    public async Task Rebuild_revisiting_original_bound_log_after_failed_publication_keeps_its_durable_read_floor()
    {
        var h = Create(moveSource: false, bound: Retired);
        h.State.State.AppliedOffsets[0] = 7;
        await ((IGrainBase)h.Grain).OnActivateAsync(CancellationToken.None);
        h.Reader.GetHeadOffsetAsync(First, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(20L));
        h.State.ThrowOnWrite = new TimeoutException("new-log checkpoint not committed");
        Assert.ThrowsAsync<TimeoutException>(async () => await h.Grain.RebuildAsync());
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(19));
        h.Registry.GetEntryAsync(Logical).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = Retired, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
        h.Reader.GetHeadOffsetAsync(Retired, 0, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }),
                "new-log offsets from a failed write cannot advance the old log's durable floor");
            return await Task.FromException<long>(new OperationCanceledException("stop at the observed head-capture boundary"));
        });

        Assert.ThrowsAsync<OperationCanceledException>(async () => await h.Grain.RebuildAsync());

        Assert.That(await h.Grain.GetDurableReadPositionsAsync(Retired), Is.EqualTo(new long[] { 8 }));
    }

    private static Harness Create(bool afterValidation = false, bool moveSource = true, string bound = First)
    {
        var physical = First;
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { PhysicalTreeId = physical, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var offsetRegistry = Substitute.For<IWalOffsetConsumerRegistryGrain>();
        factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Arg.Any<string>())
            .Returns(offsetRegistry);
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => Keys(call.ArgAt<string?>(0)));
        tree.GetWithVersionAsync("k", Arg.Any<CancellationToken>()).Returns(_ =>
        {
            if (moveSource && !afterValidation)
            {
                physical = Second;
            }
            return Task.FromResult(new VersionedValue
                { Value = [1], Version = new HybridLogicalClock { WallClockTicks = 10 } });
        });
        var shadow = Substitute.For<ILattice>();
        shadow.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(EmptyKeys());
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
            call.ArgAt<string>(0) == First ? tree : shadow);
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.Project(Arg.Any<LatticeMutation>()).Returns(call =>
            new[] { ViewWrite.Upsert("k", [1], call.Arg<LatticeMutation>().Timestamp) });
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet("orders-view").Returns(new ViewRegistration("orders-view", Logical, projection));
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", "orders-view"));
        context.ActivationServices.Returns(Substitute.For<IServiceProvider>());
        var options = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = bound;
        state.State.LegacyGenerationCeiling = 0;
        state.State.AppliedOffsets[0] = -1;
        if (moveSource && afterValidation)
        {
            state.BeforeWrite = () =>
            {
                physical = Second;
                return Task.CompletedTask;
            };
        }
        var cursors = Substitute.For<IWalCursorRegistry>();
        var reader = Substitute.For<ICommitLogReader>();
        ViewMaintainerGrain Construct(FakePersistentState<ViewCheckpointState> checkpoint) =>
            new(context, factory, null!, NullLogger<ViewMaintainerGrain>.Instance,
            catalog, reader, Substitute.For<IWalSubscriber>(), cursors,
            TestOptionsResolver.ForFactory(factory, new LatticeOptions { WalPartitions = 1 }),
            options, null!, Substitute.For<ILatticeReplicationContext>(),
            null, null!, checkpoint);
        return new Harness(Construct(state), state, cursors, tree, reader, offsetRegistry, registry, Construct);
    }

    private static async IAsyncEnumerable<string> Keys(string? start)
    {
        await Task.Yield();
        if (start is null || string.CompareOrdinal(start, "k") <= 0)
        {
            yield return "k";
        }
    }

    private static async IAsyncEnumerable<string> EmptyKeys()
    {
        await Task.CompletedTask;
        yield break;
    }
}
