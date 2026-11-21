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

/// <summary>
/// Unit coverage for the view maintainer's responsiveness to decommission
/// (#5729) and the sliced source-identity heal (#5546). The grain is driven
/// directly, so every interleaving is sequenced by the test rather than by a
/// scheduler or a wall clock.
/// </summary>
[TestFixture]
public sealed partial class ViewMaintainerMaintenanceCancellationTests
{
    private const string ViewName = "orders-view";
    private const string Logical = "orders";
    private const string First = "orders-first";
    private const string Retired = "orders-retired";

    private sealed class ManualClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utcNow = start;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        FakePersistentState<ViewCheckpointState> State,
        ICommitLogReader Reader,
        IViewCatalog Catalog,
        ViewRegistration Registration,
        ManualClock Clock);

    [Test]
    public async Task CancelMaintenanceForDecommission_cancels_an_in_flight_rebuild()
    {
        var h = Create(bound: First);
        var headReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Reader.GetHeadOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var token = call.ArgAt<CancellationToken>(2);
                var pending = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                token.Register(() => pending.TrySetCanceled(token));
                headReached.TrySetResult();
                return pending.Task;
            });

        var rebuild = h.Grain.RebuildAsync();
        await headReached.Task;
        Assert.That(rebuild.IsCompleted, Is.False, "the rebuild must be parked on the source head read");

        await h.Grain.CancelMaintenanceForDecommissionAsync();

        Assert.That(async () => await rebuild, Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(0),
                "a cancelled rebuild must not swap in its partial generation");
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First));
        });
    }

    [Test]
    public async Task RunKeepaliveTick_does_not_start_a_drain_while_the_decommission_hold_is_active()
    {
        var h = Create(bound: First);
        await h.Grain.CancelMaintenanceForDecommissionAsync();
        h.Catalog.ClearReceivedCalls();

        await h.Grain.RunKeepaliveTickAsync();

        h.Catalog.DidNotReceive().TryGet(Arg.Any<string>());
        await h.Reader.DidNotReceive().GetHeadOffsetAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task RunKeepaliveTick_resumes_once_the_decommission_hold_lapses()
    {
        var h = Create(bound: First);
        await h.Grain.CancelMaintenanceForDecommissionAsync();
        h.Clock.Advance(ViewMaintainerGrain.DecommissionHoldWindow + TimeSpan.FromSeconds(1));
        h.Catalog.ClearReceivedCalls();

        await h.Grain.RunKeepaliveTickAsync();

        h.Catalog.Received().TryGet(ViewName);
    }

    [Test]
    public async Task Rebuild_after_a_cancellation_runs_on_a_fresh_maintenance_token()
    {
        var h = Create(bound: Retired);
        await h.Grain.CancelMaintenanceForDecommissionAsync();

        await h.Grain.RebuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ActiveGeneration, Is.EqualTo(1),
                "the cancellation must be consumed, not latched onto later maintenance");
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First));
        });
    }

    private static Harness Create(string bound, int sourceKeyCount = 1)
    {
        var sourceKeys = new string[sourceKeyCount];
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            sourceKeys[i] = "k" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
        }

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(call =>
        {
            var id = call.ArgAt<string>(0);
            return Task.FromResult<TreeRegistryEntry?>(
                new TreeRegistryEntry
                {
                    PhysicalTreeId = id == Logical ? First : null,
                    ShardCount = 1,
                    MaxLeafKeys = 128,
                    MaxInternalChildren = 128
                });
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IWalOffsetConsumerRegistryGrain>());
        var source = Substitute.For<ILattice>();
        source.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => KeysFrom(sourceKeys, call.ArgAt<string?>(0)));
        source.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ =>
            Task.FromResult(new VersionedValue
                { Value = [1], Version = new HybridLogicalClock { WallClockTicks = 10 } }));
        var shadow = Substitute.For<ILattice>();
        shadow.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => KeysFrom([], null));
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
            call.ArgAt<string>(0) == First ? source : shadow);
        var shardRoot = Substitute.For<IShardRootGrain>();
        shardRoot.GetWithVersionAsync(Arg.Any<string>()).Returns(call =>
            source.GetWithVersionAsync(call.ArgAt<string>(0), CancellationToken.None));
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.Project(Arg.Any<LatticeMutation>()).Returns(call =>
        {
            var mutation = call.Arg<LatticeMutation>();
            return new[] { ViewWrite.Upsert(mutation.Key, [1], mutation.Timestamp) };
        });
        var registration = new ViewRegistration(ViewName, Logical, projection);
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet(ViewName).Returns(registration);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", ViewName));
        context.ActivationServices.Returns(Substitute.For<IServiceProvider>());
        var options = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = bound;
        state.State.LegacyGenerationCeiling = 0;
        state.State.AppliedOffsets[0] = -1;
        var reader = Substitute.For<ICommitLogReader>();
        var grain = new ViewMaintainerGrain(
            context, factory, null!, NullLogger<ViewMaintainerGrain>.Instance,
            catalog, reader, Substitute.For<IWalSubscriber>(), Substitute.For<IWalCursorRegistry>(),
            TestOptionsResolver.ForFactory(factory, new LatticeOptions { WalPartitions = 1 }),
            options, null!, Substitute.For<ILatticeReplicationContext>(),
            null, null!, state);
        var clock = new ManualClock(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        grain.SetSourceIdentityClockForTesting(clock);
        return new Harness(grain, state, reader, catalog, registration, clock);
    }

    private static async IAsyncEnumerable<string> KeysFrom(string[] keys, string? startInclusive)
    {
        await Task.Yield();
        foreach (var key in keys)
        {
            if (startInclusive is null || string.CompareOrdinal(key, startInclusive) >= 0)
            {
                yield return key;
            }
        }
    }
}
