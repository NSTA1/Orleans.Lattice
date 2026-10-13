using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    private sealed class IdentityProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "identity-v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
        {
            if (mutation.Kind == MutationKind.Set && mutation.Value is not null)
            {
                yield return ViewWrite.Upsert(mutation.Key, mutation.Value, mutation.Timestamp, sourceKey: mutation.Key);
            }
            else if (mutation.Kind == MutationKind.DeleteRange)
            {
                yield return ViewWrite.RangeDelete(mutation.Key, mutation.EndExclusiveKey!, mutation.Timestamp);
            }
            else if (mutation.Kind == MutationKind.Delete)
            {
                yield return ViewWrite.Delete(mutation.Key, mutation.Timestamp, sourceKey: mutation.Key);
            }
        }
    }

    private sealed class StaticCatalog(ViewRegistration registration, ViewRegistration? peer = null) : IViewCatalog
    {
        public ViewRegistration? TryGet(string viewName) => peer is not null && viewName == peer.ViewName ? peer : registration;

        public void Register(ViewRegistration registration)
        {
        }

        public void Remove(string viewName)
        {
        }

        public IReadOnlyCollection<ViewRegistration> All() => peer is null ? [registration] : [registration, peer];
    }

    /// <summary>
    /// A view tree that holds its rows in memory and deduplicates atomic writes by
    /// operation id: a re-submitted id that already completed returns at once
    /// without writing, the real <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, IReadOnlyList{string}, string, CancellationToken)"/>
    /// re-attach semantics.
    /// </summary>
    private sealed class FakeViewTree
    {
        public Dictionary<string, byte[]> Rows { get; } = new(StringComparer.Ordinal);

        public HashSet<string> CompletedOperations { get; } = new(StringComparer.Ordinal);

        /// <summary>Every non-atomic point write (<c>SetAsync</c> / <c>DeleteAsync</c>) in call order, by key.</summary>
        public List<string> PointWrites { get; } = [];

        /// <summary>Every non-atomic point delete in call order, by key.</summary>
        public List<string> DeletedKeys { get; } = [];

        /// <summary>Every atomic write's operation id in call order, including re-attaching ones.</summary>
        public List<string> AtomicOperations { get; } = [];

        /// <summary>The view's rows after every write, in call order: what a reader could observe between writes.</summary>
        public List<Dictionary<string, byte[]>> Snapshots { get; } = [];

        /// <summary>When set, the next atomic write completes its saga and then loses its response.</summary>
        public bool LoseNextAtomicResponse { get; set; }

        public ILattice Tree { get; }

        public FakeViewTree()
        {
            var tree = Substitute.For<ILattice>();
            tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    PointWrites.Add(call.ArgAt<string>(0));
                    Rows[call.ArgAt<string>(0)] = call.ArgAt<byte[]>(1);
                    Snap();
                    return Task.CompletedTask;
                });
            tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    PointWrites.Add(call.ArgAt<string>(0));
                    DeletedKeys.Add(call.ArgAt<string>(0));
                    var removed = Rows.Remove(call.ArgAt<string>(0));
                    Snap();
                    return Task.FromResult(removed);
                });
            tree.KeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                .Returns(call => ListKeys(call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
            tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())                .Returns(call => Task.FromResult(Rows.TryGetValue(call.ArgAt<string>(0), out var v) ? v : null));
            tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var found = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                    foreach (var key in call.ArgAt<List<string>>(0))
                    {
                        if (Rows.TryGetValue(key, out var v))
                        {
                            found[key] = v;
                        }
                    }

                    return Task.FromResult(found);
                });
            tree.SetManyAtomicAsync(
                    Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                    Arg.Any<IReadOnlyList<string>>(),
                    Arg.Any<string>(),
                    Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var operationId = call.ArgAt<string>(2);
                    AtomicOperations.Add(operationId);
                    if (!CompletedOperations.Add(operationId))
                    {
                        return Task.CompletedTask;
                    }

                    foreach (var (key, value) in call.ArgAt<List<KeyValuePair<string, byte[]>>>(0))
                    {
                        Rows[key] = value;
                    }

                    foreach (var key in call.ArgAt<IReadOnlyList<string>>(1))
                    {
                        Rows.Remove(key);
                    }

                    Snap();
                    if (LoseNextAtomicResponse)
                    {
                        LoseNextAtomicResponse = false;
                        return Task.FromException(new TimeoutException("simulated lost response after the view saga completed"));
                    }

                    return Task.CompletedTask;
                });
            tree.SetManyAtomicAsync(
                    Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                    Arg.Any<string>(),
                    Arg.Any<CancellationToken>())
                .Returns(call => tree.SetManyAtomicAsync(call.ArgAt<List<KeyValuePair<string, byte[]>>>(0),
                    [], call.ArgAt<string>(1), call.ArgAt<CancellationToken>(2)));
            Tree = tree;
        }

        private void Snap() => Snapshots.Add(new Dictionary<string, byte[]>(Rows, StringComparer.Ordinal));

        private async IAsyncEnumerable<string> ListKeys(string? startInclusive, string? endExclusive)
        {
            var keys = Rows.Keys
                .Where(k => (startInclusive is null || string.CompareOrdinal(k, startInclusive) >= 0)
                    && (endExclusive is null || string.CompareOrdinal(k, endExclusive) < 0))
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToList();
            foreach (var key in keys)
            {
                await Task.Yield();
                yield return key;
            }
        }
    }

    private sealed record Harness(
        ViewMaintainerGrain Grain,
        FakePersistentState<ViewCheckpointState> State,
        IWalSubscriber Subscriber,
        List<WalSubscriptionContext> Contexts,
        List<long> ReadFrom,
        Dictionary<string, byte[]> SourceRows,
        Dictionary<string, HybridLogicalClock> SourceVersions,
        StrongBox<long> HeadOffset,
        IShardRootGrain SourceRoot);

    /// <summary>
    /// Seeds the source tree with the last-writer state of <paramref name="log"/>:
    /// ordinary writes, range deletes and committed prepares applied in HLC order
    /// (a prepare of an uncommitted batch never becomes visible).
    /// </summary>
    private static void SeedSource(Harness h, IReadOnlyList<LatticeMutation> log)
    {
        var committed = log.Where(m => m.Kind == MutationKind.TxCommit).Select(m => m.TransactionId).ToHashSet();
        h.SourceRows.Clear();
        h.SourceVersions.Clear();
        foreach (var mutation in log
            .Where(m => m.Kind is MutationKind.Set or MutationKind.Delete or MutationKind.DeleteRange)
            .Where(m => !m.IsPrepared || committed.Contains(m.TransactionId))
            .OrderBy(m => m.Timestamp.WallClockTicks).ThenBy(m => m.Timestamp.Counter))
        {
            switch (mutation.Kind)
            {
                case MutationKind.Set:
                    h.SourceRows[mutation.Key] = mutation.Value!;
                    h.SourceVersions[mutation.Key] = mutation.Timestamp;
                    break;
                case MutationKind.Delete:
                    h.SourceRows.Remove(mutation.Key);
                    h.SourceVersions[mutation.Key] = mutation.Timestamp;
                    break;
                default:
                    foreach (var key in h.SourceRows.Keys
                        .Where(k => string.CompareOrdinal(k, mutation.Key) >= 0 && string.CompareOrdinal(k, mutation.EndExclusiveKey) < 0)
                        .ToList())
                    {
                        h.SourceRows.Remove(key);
                        h.SourceVersions[key] = mutation.Timestamp;
                    }

                    break;
            }
        }
    }

    private static void ReportNoStandaloneSourceVersions(Harness h) =>
        h.SourceRoot.GetRawVersionsAsync(Arg.Any<List<string>>()).Returns(call =>
            Task.FromResult(Enumerable.Repeat(HybridLogicalClock.Zero, call.ArgAt<List<string>>(0).Count).ToList()));

    /// <summary>
    /// Arms the stub subscriber to surface the source log from the first offset the
    /// drain context asks for, the cursored read the real subscriber performs, and
    /// records that first offset (the context's checkpoint map is live, so it is
    /// snapshotted at drain time).
    /// </summary>
    private static void ArmLog(Harness h, IReadOnlyList<LatticeMutation> log)
    {
        SeedSource(h, log);
        h.Subscriber
            .DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var drainContext = call.ArgAt<WalSubscriptionContext>(0);
                h.Contexts.Add(drainContext);
                var handler = call.ArgAt<IWalSubscriptionHandler>(1);
                var from = drainContext.Checkpoints.GetValueOrDefault(0, -1) + 1;
                h.ReadFrom.Add(from);
                var highest = HybridLogicalClock.Zero;
                for (var offset = from; offset < log.Count; offset++)
                {
                    handler.OnEntry(new WalSubscriptionEntry(0, offset, log[(int)offset]));
                    highest = log[(int)offset].Timestamp;
                }

                return Task.FromResult(new WalDrainResult
                {
                    EntriesRead = Math.Max(0, log.Count - from),
                    HighestTimestamp = highest,
                    AdvancedOffsets = from < log.Count
                        ? new Dictionary<int, long> { [0] = log.Count - 1 }
                        : new Dictionary<int, long>(),
                });
            });
    }

}
