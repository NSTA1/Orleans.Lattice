using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The coalesced pin mirror (issue #6166): a silo keeps one mirror call in flight per pin
/// shard, and the reports that arrive meanwhile go together in the next call rather than
/// each queueing an exclusive turn of its own on the shard.
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterMirrorCoalescingTests
{
    private const string Tree = "tree-6166-mirror";

    [SetUp]
    public void ResetPressure() => WalMaterialiserPinPressure.ResetForTests();

    [TearDown]
    public void ClearPressure() => WalMaterialiserPinPressure.ResetForTests();

    private static string Consumer(int i) => $"_lattice_materialiser_{Tree}_leaf-{i}";

    [Test]
    public async Task Reports_arriving_while_a_mirror_call_is_in_flight_share_the_next_call()
    {
        var pin = new GatedPinGrain();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);
        var reporter = new LeafCursorReporter(Substitute.For<IWalCursorRegistry>(), factory, monitor);
        var frontier = new HybridLogicalClock { WallClockTicks = 100 };

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer(0), frontier, 10);
        Assert.That(pin.Calls.Count, Is.EqualTo(1), "precondition: the first report's call is in flight");
        for (var i = 1; i <= 20; i++)
        {
            reporter.NoteDurableMaterialiserFrontier(Tree, Consumer(i), frontier, 10);
        }

        Assert.That(pin.Calls.Count, Is.EqualTo(1), "reports behind a call in flight wait rather than each calling the shard");
        pin.Release();
        await TestPoll.UntilAsync(() => pin.Calls.Count == 2, "the waiting reports go in one more call", TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        Assert.Multiple(() =>
        {
            Assert.That(pin.Calls.Count, Is.EqualTo(2), "twenty-one reports cost the shard two turns");
            Assert.That(pin.Calls.ToArray().Last().Select(r => r.ConsumerId),
                Is.EquivalentTo(Enumerable.Range(1, 20).Select(Consumer)), "the second call carries every waiting report");
        });
    }

    /// <summary>A pin grain whose first report call waits until <see cref="Release"/>.</summary>
    private sealed class GatedPinGrain : IWalMaterialiserPinGrain
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ConcurrentQueue<IReadOnlyList<MaterialiserPinReport>> Calls { get; } = new();

        public void Release() => _gate.TrySetResult();

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Calls.Enqueue(reports.ToArray());
            return Calls.Count == 1 ? _gate.Task : Task.CompletedTask;
        }

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => Task.CompletedTask;
        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Task.CompletedTask;
        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(new Dictionary<string, HybridLogicalClock>());
        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(new Dictionary<string, long>());
        public Task RaiseOverrideHoldsAsync(IReadOnlyList<string> consumerIds) => Task.CompletedTask;
        public Task<IReadOnlyCollection<string>> GetOverrideHoldsAsync() =>
            Task.FromResult<IReadOnlyCollection<string>>(Array.Empty<string>());
        public Task RemoveAsync(string consumerId) => Task.CompletedTask;
        public Task<WalMaterialiserPinToken> GetRetirementTokenAsync() => throw new NotSupportedException();
        public Task<bool> RemoveIfUnmergedSinceAsync(string consumerId, WalMaterialiserPinToken token) => throw new NotSupportedException();
        public Task ClearAsync() => Task.CompletedTask;
    }
}
