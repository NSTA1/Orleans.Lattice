using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A test rig for a leaf's live checkpoint advance (epic #5605, member 4): everything
/// the advance's <c>Armed</c> decision reads, with each input under the test's control.
/// The leaf gets a real <see cref="LeafAppendEpochFence"/>, over an in-memory grain
/// storage that enforces ETags; a fenced commit-log writer that drives the fence the
/// way <see cref="WalFencedAppendDispatch"/> does (claim, admission check, the
/// watermark in every stamped response, the self-report of an unstamped append); a
/// WAL shard whose claims return <see cref="ClaimHead"/>; a shard root whose
/// live-advance view the test sets; and a storage fencing verdict.
/// </summary>
internal sealed class LiveAdvanceRig
{
    /// <summary>Creates a rig over <paramref name="partitions"/> WAL partitions, the first own offset at <paramref name="firstOffset"/>.</summary>
    public LiveAdvanceRig(int partitions = 1, long firstOffset = 0)
    {
        Partitions = partitions;
        Writer = new FencedWriter(this, firstOffset);
        Shard.ClaimAppendEpochAsync(LeafId, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(WalAppendFenceClaimResult.Accept(ClaimHead)));
        ShardRoot.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(call => Task.FromResult(LeafLiveAdvanceView.For(
                ShardMarker, call.ArgAt<string?>(1), call.ArgAt<string?>(2), Reachable, OrphanRepairInProgress)));
        ShardRoot.IsReachableByDescentAsync(Arg.Any<GrainId>(), Arg.Any<string?>())
            .Returns(_ => Task.FromResult(Reachable));
        FencingCheck = new GrainStorageFencingCheck(
            new ServiceCollection().BuildServiceProvider(),
            Options.Create(new LatticeGrainStorageFencingOptions()),
            NullLogger<GrainStorageFencingCheck>.Instance)
        {
            LastResult = new GrainStorageFencingProbeResult(GrainStorageFencingVerdict.Fenced, "rig", null),
        };
    }

    /// <summary>The leaf's grain key.</summary>
    public Guid LeafId { get; } = Guid.NewGuid();

    /// <summary>The tree's WAL partition count.</summary>
    public int Partitions { get; }

    /// <summary>The fence gate; open by default.</summary>
    public Gate FenceGate { get; } = new();

    /// <summary>Grain storage for the fence and marker rows; enforces ETags.</summary>
    public EnumerableMemoryGrainStorage Storage { get; } = new();

    /// <summary>The WAL shard every partition resolves to.</summary>
    public IWalShardGrain Shard { get; } = Substitute.For<IWalShardGrain>();

    /// <summary>The allocator head a claim returns.</summary>
    public long ClaimHead { get; set; }

    /// <summary>The unstamped watermark every stamped response reports.</summary>
    public long ReportedWatermark { get; set; } = -1;

    /// <summary>The leaf's shard root.</summary>
    public IShardRootGrain ShardRoot { get; } = Substitute.For<IShardRootGrain>();

    /// <summary>The shard's marker the view returns; initialised and empty by default.</summary>
    public ShardHandOffState ShardMarker { get; set; } = new() { Initialised = true };

    /// <summary>Whether descent reaches the leaf.</summary>
    public bool Reachable { get; set; } = true;

    /// <summary>Whether the shard root is repairing orphans.</summary>
    public bool OrphanRepairInProgress { get; set; }

    /// <summary>The storage fencing verdict the leaf reads.</summary>
    public GrainStorageFencingCheck FencingCheck { get; }

    /// <summary>The fenced commit-log writer.</summary>
    public FencedWriter Writer { get; }

    /// <summary>A shard root routing stamp, as a shard root admits a write under.</summary>
    public static WalRoutingStamp Routing => WalRoutingStamp.ForShardRoot(0, 1);

    /// <summary>Registers the rig's services for the leaf's activation.</summary>
    public void Register(IServiceCollection services)
    {
        services.AddSingleton<IWalAppendFenceGate>(FenceGate);
        services.AddKeyedSingleton<IGrainStorage>(LatticeOptions.StorageProviderName, new HookedStorage(this));
        services.AddSingleton(FencingCheck);
        services.AddSingleton<ICommitLogWriter>(Writer);
    }

    /// <summary>A service provider holding the rig's services only.</summary>
    public IServiceProvider BuildServices()
    {
        var services = new ServiceCollection();
        Register(services);
        return services.BuildServiceProvider();
    }

    /// <summary>Routes the leaf's shard-root calls to the rig.</summary>
    public void Configure(IGrainFactory factory) =>
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(ShardRoot);

    /// <summary>Writes the leaf's own hand-off marker row.</summary>
    public Task SetLeafMarkerAsync(Func<LeafHandOffState, bool> mutate) =>
        HandOffMarkerRows.UpdateLeafAsync(Storage, LeafId, mutate);

    /// <summary>
    /// When set, runs once, after the leaf's next read of its own hand-off marker row has
    /// read the row and before that read returns, so a test can act between the read and
    /// the leaf's use of it (#5768). The read returns what it read before the hook ran.
    /// Writes through <see cref="Storage"/> are not hooked.
    /// </summary>
    public Func<Task>? AfterNextLeafMarkerRead { get; set; }

    /// <summary>
    /// When set, the durable ETag of the leaf's own state row, served for the #5841 probe's
    /// fresh read of it, for a leaf whose persistent state is a test double rather than a row
    /// in <see cref="Storage"/>.
    /// </summary>
    public Func<string?>? LeafStateEtag { get; set; }

    /// <summary>Called with the state name of every read the leaf makes through its storage, before the read (#5841).</summary>
    public Action<string>? OnStorageRead { get; set; }

    /// <summary>Reads the leaf's own hand-off marker row.</summary>
    public async Task<LeafHandOffState> LeafMarkerAsync() =>
        (await HandOffMarkerRows.ReadLeafAsync(Storage, LeafId)).State;

    /// <summary>The storage the leaf resolves: <see cref="Storage"/>, with <see cref="AfterNextLeafMarkerRead"/> applied.</summary>
    private sealed class HookedStorage(LiveAdvanceRig rig) : IGrainStorage
    {
        public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            rig.OnStorageRead?.Invoke(stateName);
            if (stateName == LeafStateCurrencyProbe.LeafStateName && rig.LeafStateEtag is { } etag)
            {
                // The leaf's own state row lives in a test double, not in this storage: the
                // #5841 probe reads that row's ETag, as a provider shared with it would serve.
                grainState.ETag = etag();
                grainState.RecordExists = true;
                return;
            }

            await rig.Storage.ReadStateAsync(stateName, grainId, grainState);
            if (stateName == HandOffMarkerRows.LeafGrainType
                && rig.AfterNextLeafMarkerRead is { } hook)
            {
                rig.AfterNextLeafMarkerRead = null;
                await hook();
            }
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            rig.Storage.WriteStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            rig.Storage.ClearStateAsync(stateName, grainId, grainState);
    }

    /// <summary>The fence gate's view.</summary>
    internal sealed class Gate : IWalAppendFenceGate
    {
        public bool IsOpen { get; set; } = true;
    }

    /// <summary>
    /// A fenced commit-log writer that drives the leaf's fence as the production
    /// dispatch does, routing each record by its key's partition and handing out
    /// offsets per partition from <c>firstOffset + p * 1000</c>.
    /// </summary>
    internal sealed class FencedWriter(LiveAdvanceRig rig, long firstOffset) : IFencedCommitLogWriter
    {
        private readonly long[] _next = Enumerable.Range(0, rig.Partitions).Select(p => firstOffset + (p * 1000L)).ToArray();

        /// <summary>When set, the next append waits on it before returning.</summary>
        public TaskCompletionSource? HoldNext { get; set; }

        /// <summary>When set, the next append throws it.</summary>
        public Exception? ThrowNext { get; set; }

        /// <summary>The offsets handed out, by partition.</summary>
        public long NextOffset(int partition) => _next[partition];

        /// <summary>Another author's append at <paramref name="partition"/>: consumes one offset the leaf never sees.</summary>
        public long ForeignAppend(int partition = 0) => _next[partition]++;

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default) =>
            Task.FromResult(_next[Route(entry)]++);

        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<long>>(entries.Select(e => _next[Route(e)]++).ToArray());

        public Task<long> AppendAsync(WalRecord entry, ILeafAppendFence fence, CancellationToken cancellationToken) =>
            AppendAsync(entry, fence, WalRoutingStamp.None, cancellationToken);

        public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, ILeafAppendFence fence, CancellationToken cancellationToken) =>
            AppendManyAsync(entries, fence, WalRoutingStamp.None, cancellationToken);

        public async Task<long> AppendAsync(WalRecord entry, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken)
        {
            try
            {
                if (ThrowNext is { } fault)
                {
                    ThrowNext = null;
                    throw fault;
                }

                var offset = await DispatchAsync(entry, fence, routing, cancellationToken);
                if (HoldNext is { } hold)
                {
                    HoldNext = null;
                    await hold.Task;
                }

                return offset;
            }
            catch (Exception ex) when (ReportUnallocated(fence, ex, 1))
            {
                throw;
            }
        }

        public async Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken)
        {
            try
            {
                if (ThrowNext is { } fault)
                {
                    ThrowNext = null;
                    throw fault;
                }

                var offsets = new long[entries.Count];
                for (var i = 0; i < offsets.Length; i++)
                {
                    offsets[i] = await DispatchAsync(entries[i], fence, routing, cancellationToken);
                }

                return offsets;
            }
            catch (Exception ex) when (ReportUnallocated(fence, ex, entries.Count))
            {
                throw;
            }
        }

        // As WalCommitLogWriter does for a call that allocated nothing (#5708).
        private static bool ReportUnallocated(ILeafAppendFence fence, Exception exception, int entries)
        {
            if (WalFencedAppendDispatch.IsUnallocatedRefusal(exception, entries))
            {
                fence.OnRefusedUnallocated();
            }

            return false;
        }

        private async Task<long> DispatchAsync(WalRecord entry, ILeafAppendFence fence, WalRoutingStamp routing, CancellationToken cancellationToken)
        {
            var partition = Route(entry);
            var stamp = await fence.StampForAsync(rig.Shard, partition, cancellationToken);
            if (stamp is null || routing.ForcesUnstamped)
            {
                var unstamped = _next[partition]++;
                fence.OnUnstampedAppended(partition, unstamped);
                return unstamped;
            }

            if (fence.OnAdmitted(routing.AdmissionMark) is { } refused)
            {
                throw refused;
            }

            var offset = _next[partition]++;
            fence.OnStampedAppended(partition, rig.ReportedWatermark);
            return offset;
        }

        private int Route(WalRecord entry) => BPlusLeafGrain.OverrideHoldPartitionOf(entry, rig.Partitions);
    }
}
