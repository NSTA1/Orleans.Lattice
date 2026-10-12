using Microsoft.Extensions.DependencyInjection;
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
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class ViewMaintainerHistoryAdmissionTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Accumulative_history_retains_ordinary_and_overlapping_atomic_revisions_with_independent_clocks_and_replay(
        bool loseResponse)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var codec = new HistoryRowCodec(services.GetRequiredService<Serializer<HistoryRow>>());
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var log = new List<LatticeMutation>
        {
            Revision("k", 2, 20, first, 0),
            Revision("j", 2, 40, first, 1),
            Revision("k", 5, 50),
            Revision("k", 3, 30, second, 0),
            Revision("j", 3, 10, second, 1),
            Revision("k", 1, 5),
            Terminal(first, 41),
            Terminal(second, 31),
        };
        var expected = log.Where(m => m.Kind == MutationKind.Set)
            .Select(m => HistoryKey.Encode(m.Key, m.Timestamp)).ToArray();
        var rows = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var completed = new HashSet<string>(StringComparer.Ordinal);
        var tree = Substitute.For<ILattice>();
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                rows[call.ArgAt<string>(0)] = call.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });
        tree.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (!completed.Add(call.ArgAt<string>(1)))
                {
                    return Task.CompletedTask;
                }

                foreach (var row in call.ArgAt<List<KeyValuePair<string, byte[]>>>(0))
                {
                    rows[row.Key] = row.Value;
                }

                if (loseResponse)
                {
                    loseResponse = false;
                    return Task.FromException(new TimeoutException("completed history batch response lost"));
                }

                return Task.CompletedTask;
            });
        tree.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => tree.SetManyAtomicAsync(call.ArgAt<List<KeyValuePair<string, byte[]>>>(0),
                call.ArgAt<string>(2), call.ArgAt<CancellationToken>(3)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree);
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var catalog = Substitute.For<IViewCatalog>();
        catalog.TryGet("history").Returns(new ViewRegistration("history", "orders",
            new HistoryLatticeViewProjection(codec), Accumulative: true));
        var state = new FakePersistentState<ViewCheckpointState>();
        state.State.BoundPhysicalTreeId = "orders";
        var subscriber = Substitute.For<IWalSubscriber>();
        subscriber.DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var from = call.ArgAt<WalSubscriptionContext>(0).Checkpoints.GetValueOrDefault(0, -1) + 1;
                for (var offset = from; offset < log.Count; offset++)
                {
                    call.ArgAt<IWalSubscriptionHandler>(1).OnEntry(new WalSubscriptionEntry(0, offset, log[(int)offset]));
                }

                return Task.FromResult(new WalDrainResult
                {
                    EntriesRead = Math.Max(0, log.Count - from),
                    HighestTimestamp = log.MaxBy(m => m.Timestamp.WallClockTicks)!.Timestamp,
                    AdvancedOffsets = from < log.Count ? new Dictionary<int, long> { [0] = log.Count - 1 } : [],
                });
            });
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(call.ArgAt<int>(1) == 0 ? (long)log.Count : 0));
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", "history"));
        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalRetention = TimeSpan.Zero });
        ViewMaintainerGrain Create() => new(context, factory, reminderRegistry: null!,
            NullLogger<ViewMaintainerGrain>.Instance, catalog, reader, subscriber,
            Substitute.For<IWalCursorRegistry>(), TestOptionsResolver.ForFactory(factory, new LatticeOptions()),
            viewOptions, options, Substitute.For<ILatticeReplicationContext>(), saturationSignal: null,
            historyRowCodec: codec, state);
        var grain = Create();
        if (loseResponse)
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await grain.DrainAsync());
            grain = Create();
        }

        await grain.DrainAsync();

        Assert.That(rows.Keys, Is.EquivalentTo(expected),
            "Accumulative admission must retain every revision, not only current source winners.");
        Assert.That(state.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        await grain.DrainAsync();
        Assert.That(rows.Keys, Is.EquivalentTo(expected));
    }

    private static LatticeMutation Revision(string key, byte value, long ticks, Guid? transaction = null, int index = 0) => new()
    {
        TreeId = "orders",
        Kind = MutationKind.Set,
        Key = key,
        Value = [value],
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        Category = MutationCategory.User,
        IsPrepared = transaction.HasValue,
        TransactionId = transaction.GetValueOrDefault(),
        AtomicBatchSize = transaction.HasValue ? 2 : 0,
        AtomicBatchIndex = transaction.HasValue ? index : 0,
    };

    private static LatticeMutation Terminal(Guid transaction, long ticks) => new()
    {
        TreeId = "orders",
        Kind = MutationKind.TxCommit,
        Key = string.Empty,
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        Category = MutationCategory.User,
        TransactionId = transaction,
    };
}
