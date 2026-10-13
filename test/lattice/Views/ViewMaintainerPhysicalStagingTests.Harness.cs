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

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    private sealed class Harness
    {
        public required ViewMaintainerGrain Grain { get; set; }
        public required Func<ViewMaintainerGrain> NewActivation { get; init; }
        public required FakePersistentState<ViewCheckpointState> State { get; init; }
        public required IWalCursorRegistry Cursors { get; init; }
        public string Alias { get; set; } = Old;
        public Func<string, Task>? OnScan { get; set; }
        public Action<string>? BeforeTailEntries { get; set; }
        public List<(string Tree, List<KeyValuePair<string, byte[]>> Writes)> AtomicWrites { get; } = [];
        public List<(string Tree, long From)> Reads { get; } = [];
        public List<(string Tree, long From)> HistoryReads { get; } = [];
        public List<(string Tree, long Offset)> HistoryEntries { get; } = [];
        public List<(string Tree, int Partition, long From, long Through, int Limit)> HistoryRequests { get; } = [];
        public Dictionary<int, long> TailLimits { get; } = [];
        public List<(string Tree, long Generation, long Offset)> PublishedSnapshots { get; } = [];
        public Dictionary<(string Tree, string Key), LwwEntry> SourceRows { get; } = [];
        private readonly Dictionary<string, List<(long Offset, LatticeMutation Mutation)>> _logs = new();
        private readonly Dictionary<(string Tree, int Partition), List<(long Offset, LatticeMutation Mutation)>> _partitionLogs = new();
        private readonly Dictionary<(string Tree, int Partition), long> _partitionHeads = new();
        private readonly Dictionary<string, long> _heads = new() { [Old] = 7, [New] = 2, [Third] = 5 };

        public List<LwwEntry?> VisibleRows(string physical, List<string> keys)
        {
            var entries = _logs.GetValueOrDefault(physical, []);
            var committed = entries.Where(x => x.Mutation.Kind == MutationKind.TxCommit)
                .Select(x => x.Mutation.TransactionId).ToHashSet();
            return keys.Select(key =>
            {
                if (SourceRows.TryGetValue((physical, key), out var observation)) return (LwwEntry?)observation;
                LatticeMutation? winner = null;
                foreach (var (_, mutation) in entries)
                {
                    if (mutation.Key != key || mutation.Kind is not (MutationKind.Set or MutationKind.Delete)
                        || (mutation.IsPrepared && !committed.Contains(mutation.TransactionId))) continue;
                    if (winner is null || mutation.Timestamp > winner.Value.Timestamp) winner = mutation;
                }
                return winner is { } row
                    ? (LwwEntry?)new LwwEntry
                    {
                        Key = key, Value = row.Value, Timestamp = row.Timestamp,
                        IsTombstone = row.IsTombstone || row.Kind == MutationKind.Delete,
                        ExpiresAtTicks = row.ExpiresAtTicks,
                        OriginClusterId = row.OriginClusterId,
                        VectorClock = row.VectorClock?.Clone(),
                    }
                    : null;
            }).ToList();
        }

        public void Append(string tree, long offset, LatticeMutation mutation, int partition = 0)
        {
            if (partition != 0)
            {
                var id = (tree, partition);
                if (!_partitionLogs.TryGetValue(id, out var log)) _partitionLogs[id] = log = [];
                log.Add((offset, mutation));
                _partitionHeads[id] = Math.Max(_partitionHeads.GetValueOrDefault(id, 7), offset + 1);
                return;
            }
            if (!_logs.TryGetValue(tree, out var entries)) _logs[tree] = entries = [];
            entries.Add((offset, mutation));
            _heads[tree] = Math.Max(_heads[tree], offset + 1);
        }

        public static async Task<Harness> CreateAsync(int prepareScanLimit = 0, int partitions = 1, int maxStagedTransactions = 0)
        {
            Harness h = null!;
            var factory = Substitute.For<IGrainFactory>();
            var registry = Substitute.For<ILatticeRegistry>();
            registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
                new TreeRegistryEntry { PhysicalTreeId = h.Alias, ShardCount = 1, MaxLeafKeys = 128, MaxInternalChildren = 128 }));
            factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
            factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Arg.Any<string>())
                .Returns(Substitute.For<IWalOffsetConsumerRegistryGrain>());
            foreach (var physical in new[] { Old, New, Third })
            {
                var root = Substitute.For<IShardRootGrain>();
                root.GetVisibleRawEntriesAsync(Arg.Any<List<string>>())
                    .Returns(call => Task.FromResult(h.VisibleRows(physical, call.Arg<List<string>>())));
                root.GetWithVersionAsync("snapshot").Returns(async _ =>
                {
                    if (h.OnScan is { } onScan) await onScan(physical);
                    return new VersionedValue { Value = [2], Version = Clock(physical == Third ? 50 : physical == New ? 20 : 10) };
                });
                factory.GetGrain<IShardRootGrain>($"{physical}/0").Returns(root);
            }
            var trees = new Dictionary<string, ILattice>();
            ILattice Tree(string id)
            {
                if (trees.TryGetValue(id, out var existing)) return existing;
                var tree = Substitute.For<ILattice>();
                var rows = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                trees.Add(id, tree);
                tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>()).Returns(call =>
                    Task.FromResult(call.Arg<List<string>>().Where(rows.ContainsKey)
                        .Distinct(StringComparer.Ordinal).ToDictionary(key => key, key => rows[key].ToArray(),
                            StringComparer.Ordinal)));
                tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>()).Returns(call =>
                {
                    rows[call.Arg<string>()] = call.Arg<byte[]>().ToArray();
                    return Task.CompletedTask;
                });
                tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(call =>
                    Task.FromResult(rows.Remove(call.Arg<string>())));
                tree.GetRoutingAsync(Arg.Any<CancellationToken>()).Returns(_ =>
                    new ValueTask<RoutingInfo>(new RoutingInfo(id == Logical ? h.Alias : id, ShardMap.CreateDefault(1, 1))));
                tree.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(_ =>
                    new ValueTask<RoutingInfo>(new RoutingInfo(id == Logical ? h.Alias : id, ShardMap.CreateDefault(1, 1))));
                tree.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                    .Returns(call => Keys(call.ArgAt<string?>(0), id == Old || id == New || id == Third));
                tree.GetWithVersionAsync("snapshot", Arg.Any<CancellationToken>()).Returns(async _ =>
                {
                    if (h.OnScan is { } onScan) await onScan(id);
                    return new VersionedValue { Value = [2], Version = Clock(id == Third ? 50 : id == New ? 20 : 10) };
                });
                tree.SetManyAtomicAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<List<string>>(), Arg.Any<string>(),
                    Arg.Any<CancellationToken>()).Returns(call =>
                {
                    h.AtomicWrites.Add((id, [.. call.Arg<List<KeyValuePair<string, byte[]>>>()]));
                    foreach (var write in call.Arg<List<KeyValuePair<string, byte[]>>>())
                        rows[write.Key] = write.Value.ToArray();
                    foreach (var key in call.Arg<List<string>>()) rows.Remove(key);
                    return Task.FromResult(new AtomicWriteOutcome());
                });
                return tree;
            }
            factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call => Tree(call.ArgAt<string>(0)));
            var projection = Substitute.For<ILatticeViewProjection>();
            projection.Project(Arg.Any<LatticeMutation>()).Returns(call =>
            {
                var m = call.Arg<LatticeMutation>();
                return new[] { ViewWrite.Upsert(m.Key, m.Value!, m.Timestamp) };
            });
            var registration = new ViewRegistration("physical-staging", Logical, projection);
            var catalog = Substitute.For<IViewCatalog>();
            catalog.TryGet("physical-staging").Returns(registration);
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("viewmaintainer", "physical-staging"));
            context.ActivationServices.Returns(Substitute.For<IServiceProvider>());
            var state = new FakePersistentState<ViewCheckpointState>();
            state.State.BoundPhysicalTreeId = Old;
            state.State.LegacyGenerationCeiling = 0;
            state.State.AppliedOffsets[0] = 6;
            for (var partition = 1; partition < partitions; partition++) state.State.AppliedOffsets[partition] = 6;
            state.State.ProjectionVersion = registration.ProjectionVersion;
            var views = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
            views.Get(Arg.Any<string>()).Returns(new LatticeViewOptions
            {
                RebuildPrepareScanLimit = prepareScanLimit, MaxStagedTransactions = maxStagedTransactions,
            });
            var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = partitions, WalRetention = TimeSpan.Zero });
            var reader = Substitute.For<ICommitLogReader>();
            reader.GetHeadOffsetAsync(Arg.Any<string>(), 0, Arg.Any<CancellationToken>())
                .Returns(call => Task.FromResult(h._heads[call.ArgAt<string>(0)]));
            for (var partition = 1; partition < partitions; partition++)
            {
                var capturedPartition = partition;
                reader.GetHeadOffsetAsync(Arg.Any<string>(), capturedPartition, Arg.Any<CancellationToken>())
                    .Returns(call => Task.FromResult(h._partitionHeads.GetValueOrDefault(
                        (call.ArgAt<string>(0), capturedPartition), 7)));
                reader.ReadAsync(Arg.Any<string>(), capturedPartition, Arg.Any<long>(), Arg.Any<CancellationToken>())
                    .Returns(call => h.ReadHistoryAsync(call.ArgAt<string>(0), call.ArgAt<long>(2), partition: capturedPartition));
                reader.ReadFilteredAsync(Arg.Any<string>(), Arg.Is(capturedPartition), Arg.Any<long>(), Arg.Any<long>(),
                        Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
                    .Returns(call => h.ReadHistoryAsync(call.ArgAt<string>(0), call.ArgAt<long>(2),
                        call.ArgAt<long>(3), call.ArgAt<int>(4), capturedPartition));
            }
            reader.ReadAsync(Arg.Any<string>(), 0, Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(call => h.ReadHistoryAsync(call.ArgAt<string>(0), call.ArgAt<long>(2)));
            reader.ReadFilteredAsync(Arg.Any<string>(), Arg.Is(0), Arg.Any<long>(), Arg.Any<long>(),
                    Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
                .Returns(call => h.ReadHistoryAsync(call.ArgAt<string>(0), call.ArgAt<long>(2),
                    call.ArgAt<long>(3), call.ArgAt<int>(4)));
            var subscriber = Substitute.For<IWalSubscriber>();
            subscriber.DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var c = call.Arg<WalSubscriptionContext>();
                    var handler = call.Arg<IWalSubscriptionHandler>();
                    var entries = new List<(int Partition, long Offset, LatticeMutation Mutation)>();
                    for (var partition = 0; partition < partitions; partition++)
                    {
                        var from = c.Checkpoints.GetValueOrDefault(partition, -1);
                        h.Reads.Add((c.SourceTreeId, from));
                        var log = partition == 0 ? h._logs.GetValueOrDefault(c.SourceTreeId, [])
                            : h._partitionLogs.GetValueOrDefault((c.SourceTreeId, partition), []);
                        entries.AddRange(log.Where(x => x.Offset > from
                                && x.Offset <= h.TailLimits.GetValueOrDefault(partition, long.MaxValue))
                            .Select(x => (partition, x.Offset, x.Mutation)));
                    }
                    h.BeforeTailEntries?.Invoke(c.SourceTreeId);
                    foreach (var e in entries) handler.OnEntry(new WalSubscriptionEntry(e.Partition, e.Offset, e.Mutation));
                    return Task.FromResult(new WalDrainResult
                    {
                        EntriesRead = entries.Count, EntriesSurfaced = entries.Count,
                        HighestTimestamp = entries.Aggregate(c.HighestApplied,
                            (highest, entry) => entry.Mutation.Timestamp > highest ? entry.Mutation.Timestamp : highest),
                        AdvancedOffsets = entries.GroupBy(x => x.Partition)
                            .ToDictionary(group => group.Key, group => group.Max(x => x.Offset)),
                    });
                });
            var cursors = Substitute.For<IWalCursorRegistry>();
            ViewMaintainerGrain NewActivation() => new(context, factory, null!, NullLogger<ViewMaintainerGrain>.Instance,
                catalog, reader, subscriber, cursors,
                TestOptionsResolver.ForFactory(factory, new LatticeOptions { WalPartitions = partitions, WalRetention = TimeSpan.Zero }),
                views, options, Substitute.For<ILatticeReplicationContext>(), null, null!, state);
            var grain = NewActivation();
            h = new Harness { Grain = grain, NewActivation = NewActivation, State = state, Cursors = cursors };
            state.OnWriteState = s => h.PublishedSnapshots.Add((s.BoundPhysicalTreeId, s.ActiveGeneration,
                s.AppliedOffsets.GetValueOrDefault(0, -1)));
            await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
            return h;
        }

        private async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadHistoryAsync(
            string tree, long from, long through = long.MaxValue, int maxExamined = int.MaxValue, int partition = 0)
        {
            HistoryReads.Add((tree, from));
            HistoryRequests.Add((tree, partition, from, through, maxExamined));
            await Task.Yield();
            var log = partition == 0 ? _logs.GetValueOrDefault(tree, [])
                : _partitionLogs.GetValueOrDefault((tree, partition), []);
            foreach (var entry in log
                .Where(x => x.Offset > from && x.Offset <= through).OrderBy(x => x.Offset).Take(maxExamined))
            {
                HistoryEntries.Add((tree, entry.Offset));
                yield return entry;
            }
        }

        private static async IAsyncEnumerable<string> Keys(string? start, bool source)
        {
            await Task.Yield();
            if (source && (start is null || string.CompareOrdinal(start, "snapshot") <= 0)) yield return "snapshot";
        }
    }
}
