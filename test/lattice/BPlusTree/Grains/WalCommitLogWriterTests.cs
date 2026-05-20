using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit-level reproduction of the WAL <c>OriginClusterId</c> stamping
/// behaviour. Prior to the introduction of
/// <see cref="ILatticeOriginClusterIdResolver"/>, both
/// <see cref="WalCommitLogWriter"/> and <see cref="WalShardGrain.ReadAsync"/>
/// hardcoded <c>originClusterId: ""</c> on the
/// <see cref="WalRecordConverter.ToWalRecord"/> call site, so multi-site
/// hosts persisted an empty origin even when the local cluster id was
/// configured. The only existing reproduction lived in the chaos-category
/// <c>MultiSiteClusterFixtureSmokeTests</c> (excluded by the standard
/// <c>--filter "TestCategory!=Chaos"</c> dev gates), so the regression
/// shipped silently. These tests exercise the producer-side stamping
/// at unit-test granularity so any future revert lights up immediately.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalCommitLogWriterTests
{
    private const string TreeId = "tree-x";

    private static (WalCommitLogWriter writer, List<WalRecord> captured) CreateWriter(
        string clusterId = "site-test",
        LatticeMergeMode? mode = LatticeMergeMode.LwwRegister)
    {
        var captured = new List<WalRecord>();
        var shard = Substitute.For<IWalShardGrain>();
        shard
            .AppendAsync(Arg.Do<WalRecord>(r => captured.Add(r)), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(mode);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(clusterId);

        var writer = new WalCommitLogWriter(grainFactory, optionsMonitor, modeResolver, clusterIdResolver);
        return (writer, captured);
    }

    private static WalRecord MakeMutation(string? originClusterId = null) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = "k",
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = originClusterId,
    };

    [Test]
    public async Task AppendAsync_stamps_originClusterId_from_resolver_when_mutation_origin_is_null()
    {
        // Reproduces the pre-fix bug: foreground single-cluster commits
        // arrive with mutation.OriginClusterId == null (LatticeOriginContext.Current
        // is null on a single-cluster host), and the WAL writer must
        // ask the resolver for the local cluster id rather than
        // hardcoding string.Empty into the converter call.
        var (writer, captured) = CreateWriter(clusterId: "site-test");

        await writer.AppendAsync(MakeMutation(originClusterId: null));

        Assert.That(captured, Has.Count.EqualTo(1));
        Assert.That(captured[0].OriginClusterId, Is.EqualTo("site-test"));
    }

    [Test]
    public async Task AppendAsync_preserves_mutation_originClusterId_when_present()
    {
        // Remote-replay path: the mutation already carries a non-null
        // OriginClusterId stamped by the upstream cluster, and the
        // WalRecordConverter's `mutation.OriginClusterId ?? originClusterId`
        // fallback must keep the upstream value verbatim. The resolver-
        // supplied local cluster id is the fallback only.
        var (writer, captured) = CreateWriter(clusterId: "site-local");

        await writer.AppendAsync(MakeMutation(originClusterId: "site-remote"));

        Assert.That(captured, Has.Count.EqualTo(1));
        Assert.That(captured[0].OriginClusterId, Is.EqualTo("site-remote"));
    }

    [Test]
    public async Task AppendAsync_uses_empty_string_when_resolver_returns_empty()
    {
        // Single-cluster default: DefaultLatticeOriginClusterIdResolver
        // returns string.Empty for every tree, so the resulting record
        // carries an empty OriginClusterId and downstream consumers
        // ignore it.
        var (writer, captured) = CreateWriter(clusterId: string.Empty);

        await writer.AppendAsync(MakeMutation(originClusterId: null));

        Assert.That(captured, Has.Count.EqualTo(1));
        Assert.That(captured[0].OriginClusterId, Is.EqualTo(string.Empty));
    }

    [Test]
    public async Task AppendAsync_overwrites_empty_string_origin_with_resolver_value()
    {
        // Semantic divergence from the pre-builder converter path.
        // The OLD WalRecordConverter applied
        // `mutation.OriginClusterId ?? originClusterId`, which preserved
        // a deliberate empty string supplied by the producer. The NEW
        // WalCommitLogWriter.Route applies
        // `string.IsNullOrEmpty(entry.OriginClusterId) ? resolver : entry.OriginClusterId`,
        // which overwrites both null AND "" with the resolver value.
        // No production producer stamps "" deliberately
        // (LatticeOriginContext.Current is either null or a populated
        // cluster id), so the change is benign in practice. This test
        // pins the new behaviour so a future revert to the null-coalesce
        // form lights up.
        var (writer, captured) = CreateWriter(clusterId: "site-resolved");

        await writer.AppendAsync(MakeMutation(originClusterId: string.Empty));

        Assert.That(captured, Has.Count.EqualTo(1));
        Assert.That(captured[0].OriginClusterId, Is.EqualTo("site-resolved"),
            "empty-string origin must be replaced with the resolver-supplied cluster id");
    }

    [Test]
    public async Task AppendAsync_calls_resolver_with_mutation_treeId()
    {
        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
             .Returns(Task.FromResult(0L));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var writer = new WalCommitLogWriter(grainFactory, optionsMonitor, modeResolver, clusterIdResolver);

        await writer.AppendAsync(MakeMutation(originClusterId: null));

        clusterIdResolver.Received(1).Resolve(TreeId);
    }

    // --- AppendManyAsync (batched leaf write path) ---

    [Test]
    public async Task AppendManyAsync_empty_list_returns_empty_and_makes_no_grain_call()
    {
        var (writer, captured) = CreateWriter();

        var result = await writer.AppendManyAsync(new List<WalRecord>());

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Empty);
            Assert.That(captured, Is.Empty);
        });
    }

    [Test]
    public void AppendManyAsync_throws_on_null_mutations()
    {
        var (writer, _) = CreateWriter();
        Assert.That(async () => await writer.AppendManyAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task AppendManyAsync_single_mutation_uses_single_dispatch_fast_path()
    {
        var (writer, captured) = CreateWriter();

        var result = await writer.AppendManyAsync(new[] { MakeMutation() });

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(1));
            Assert.That(captured, Has.Count.EqualTo(1));
            Assert.That(captured[0].Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public async Task AppendManyAsync_dispatches_one_grain_batch_per_partition()
    {
        // Default options pin WalPartitions=1, so every key in a batch
        // hashes to the same WAL grain, which means a 16-key batched
        // dispatch produces exactly one AppendBatchAsync call.
        var partitionCalls = new List<int>();
        var shard = Substitute.For<IWalShardGrain>();
        shard
            .AppendBatchAsync(Arg.Do<IReadOnlyList<WalRecord>>(r => partitionCalls.Add(r.Count)), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entries = (IReadOnlyList<WalRecord>)call[0];
                var offsets = new long[entries.Count];
                for (var i = 0; i < entries.Count; i++) offsets[i] = i;
                return Task.FromResult<IReadOnlyList<long>>(offsets);
            });
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var writer = new WalCommitLogWriter(grainFactory, optionsMonitor, modeResolver, clusterIdResolver);

        var mutations = new List<WalRecord>();
        for (var i = 0; i < 16; i++)
        {
            mutations.Add(new WalRecord
            {
                TreeId = TreeId,
                Op = MutationKind.Set,
                Key = $"k{i:D2}",
                Value = new byte[] { (byte)i },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            });
        }

        var offsets = await writer.AppendManyAsync(mutations);

        Assert.Multiple(() =>
        {
            Assert.That(partitionCalls, Has.Count.EqualTo(1),
                "All keys hash to the single default WAL partition so the writer must dispatch one batched call.");
            Assert.That(partitionCalls[0], Is.EqualTo(16));
            Assert.That(offsets, Has.Count.EqualTo(16));
        });
    }

    [Test]
    public async Task AppendManyAsync_returns_offsets_in_input_order_across_multiple_partitions()
    {
        // Two WAL partitions: the batched dispatch fans out per
        // partition, but the writer must reassemble the per-partition
        // offsets back into the caller's input order.
        var perGrainOffsets = new Dictionary<string, long>(StringComparer.Ordinal);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>())
            .Returns(call =>
            {
                var key = (string)call[0];
                var shardLocal = Substitute.For<IWalShardGrain>();
                shardLocal
                    .AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
                    .Returns(c =>
                    {
                        var entries = (IReadOnlyList<WalRecord>)c[0];
                        var offsets = new long[entries.Count];
                        for (var i = 0; i < entries.Count; i++)
                        {
                            perGrainOffsets.TryGetValue(key, out var next);
                            offsets[i] = next;
                            perGrainOffsets[key] = next + 1;
                        }
                        return Task.FromResult<IReadOnlyList<long>>(offsets);
                    });
                return shardLocal;
            });

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 4 });
        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var writer = new WalCommitLogWriter(grainFactory, optionsMonitor, modeResolver, clusterIdResolver);

        var mutations = new List<WalRecord>();
        for (var i = 0; i < 32; i++)
        {
            mutations.Add(new WalRecord
            {
                TreeId = TreeId,
                Op = MutationKind.Set,
                Key = $"k{i:D2}",
                Value = new byte[] { (byte)i },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            });
        }

        var offsets = await writer.AppendManyAsync(mutations);

        Assert.That(offsets, Has.Count.EqualTo(32));
        // For every input index i, offsets[i] must equal the
        // per-partition position of the i-th key inside its partition's
        // bucket. We re-derive that by hashing each key.
        var partitions = new Dictionary<int, int>();
        for (var i = 0; i < mutations.Count; i++)
        {
            var partition = Orleans.Lattice.BPlusTree.Grains.WalPartitionHash.Compute(mutations[i].Key, 4);
            partitions.TryGetValue(partition, out var counter);
            Assert.That(offsets[i], Is.EqualTo((long)counter),
                $"input index {i} (key {mutations[i].Key}) should hold its partition-local offset");
            partitions[partition] = counter + 1;
        }
    }
}
