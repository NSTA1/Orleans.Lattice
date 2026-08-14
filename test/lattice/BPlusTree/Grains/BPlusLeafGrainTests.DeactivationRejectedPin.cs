using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #1464: the awaited retention-pin flush is
/// rejected and swallowed during a full-silo graceful shutdown, because the
/// pin-store grain is itself deactivating and the stopping silo refuses to
/// create its activation. The worst case is a leaf whose <b>first</b>
/// real-frontier checkpoint is produced by the deactivation flush: both barrier
/// A (first real frontier) and barrier B (deactivation flush) then fire during
/// teardown, both route to <see cref="LeafCursorReporter"/>'s durable flush, and
/// both were rejected - leaving <b>no</b> durable floor at all, so the WAL GC
/// trims past the leaf's checkpoint and the next cold activation throws
/// <c>LeafProjectionStaleException</c>.
/// <para>
/// The fix makes the reporter fall back to a direct durable-store write when the
/// grain call is rejected mid-teardown. This test drives the real
/// <see cref="BPlusLeafGrain"/> through a rejecting <see cref="WalMaterialiserPinGrain"/>
/// with a real durable store behind the reporter, forces the first checkpoint to
/// occur inside <c>OnDeactivateAsync</c>, and asserts the leaf's final frontier
/// still lands durably (criteria b and c). That the durable pin then floors the
/// GC is already proven by the #1453 suite
/// (<see cref="Dormant_leaf_pin_survives_registry_wipe_and_blocks_trim_past_checkpoint"/>),
/// which this composes with.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task First_checkpoint_at_graceful_deactivation_persists_durable_floor_despite_pin_grain_rejection()
    {
        var storage = new RejectedPinDirectStore();
        var (leaf, treeId) = CreateLeafWithRejectingPinStore("deact-reject-leaf", storage);

        // Apply a mutation and stage a checkpoint offset, but do NOT flush the
        // checkpoint here - so the leaf has never produced a real frontier and
        // its FIRST checkpoint is produced by OnDeactivateAsync below, firing
        // both barriers during teardown while the pin grain rejects.
        var projection = AsProjection(leaf);
        projection.Apply(BuildSet("k1", Encoding.UTF8.GetBytes("v"), hlcPhysical: 55, treeId: treeId));
        await projection.SetCheckpointOffsetAsync(1, default);

        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        Assert.That(storage.TryReadAnyPin(treeId, out var pinned), Is.True,
            "A leaf whose first checkpoint occurs during graceful deactivation must still leave a durable floor "
            + "via the direct-store fallback, even though every pin-grain call is rejected mid-teardown.");
        Assert.That(pinned, Is.EqualTo(PinHlc(55)),
            "The durable floor must record the leaf's final checkpoint frontier.");
    }

    private static (BPlusLeafGrain Leaf, string TreeId) CreateLeafWithRejectingPinStore(
        string leafKey, RejectedPinDirectStore storage)
    {
        var registry = new InMemoryWalCursorRegistry();

        var rejectingPin = new RejectedPinGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(rejectingPin);

        var reporter = new LeafCursorReporter(
            registry,
            factory,
            options: null,
            logger: null,
            pinStorage: storage,
            pinGrainIdResolver: key => GrainId.Create("wal-materialiser-pin", key));

        var services = new ServiceCollection();
        services.AddSingleton<ILeafCursorReporter>(reporter);
        var provider = services.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey));
        context.ActivationServices.Returns(provider);

        var state = new FakePersistentState<LeafNodeState> { State = { TreeId = PinSeamTreeId } };

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 }, maxLeafKeys: 128, shardCount: 1, factory: factory);
        var leaf = new BPlusLeafGrain(
            context, state, factory, optionsResolver, TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (leaf, PinSeamTreeId);
    }

    /// <summary>
    /// Pin grain that rejects every durable write with the canonical
    /// activation-collection rejection message, standing in for a pin-store
    /// grain whose activation the stopping silo refuses to create.
    /// </summary>
    private sealed class RejectedPinGrain : IWalMaterialiserPinGrain
    {
        private static Task Reject() =>
            throw new InvalidOperationException(
                "Unable to create local activation for grain wal-materialiser-pin. Rejecting now.");

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => Reject();
        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Reject();
        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Reject();
        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() => Reject()
            .ContinueWith<IReadOnlyDictionary<string, HybridLogicalClock>>(_ => null!);
        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() => Reject()
            .ContinueWith<IReadOnlyDictionary<string, long>>(_ => null!);
        public Task RemoveAsync(string consumerId) => Reject();
        public Task ClearAsync() => Reject();
    }

    /// <summary>
    /// Minimal in-memory <see cref="IGrainStorage"/> keyed by
    /// <c>{stateName}/{grainId}</c>, standing in for the durable "lattice"
    /// provider the reporter's teardown fallback writes through.
    /// </summary>
    private sealed class RejectedPinDirectStore : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _store =
            new(StringComparer.Ordinal);

        public bool TryReadAnyPin(string treeName, out HybridLogicalClock frontier)
        {
            var key = MakeKey(WalMaterialiserPinState.StateName, GrainId.Create("wal-materialiser-pin", treeName));
            if (_store.TryGetValue(key, out var state) && state.Pins.Count > 0)
            {
                frontier = state.Pins.Values.First();
                return true;
            }
            frontier = HybridLogicalClock.Zero;
            return false;
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (_store.TryGetValue(MakeKey(stateName, grainId), out var state))
            {
                grainState.State = (T)(object)Clone(state);
                grainState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }
            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _store[MakeKey(stateName, grainId)] = Clone((WalMaterialiserPinState)(object)grainState.State!);
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _store.TryRemove(MakeKey(stateName, grainId), out _);
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }

        private static WalMaterialiserPinState Clone(WalMaterialiserPinState source) =>
            new()
            {
                Pins = new Dictionary<string, HybridLogicalClock>(source.Pins, StringComparer.Ordinal),
                Offsets = new Dictionary<string, long>(source.Offsets, StringComparer.Ordinal),
            };

        private static string MakeKey(string stateName, GrainId grainId) => $"{stateName}/{grainId}";
    }
}
