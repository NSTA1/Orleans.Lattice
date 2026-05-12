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

    private static LatticeMutation MakeMutation(string? originClusterId = null) => new()
    {
        TreeId = TreeId,
        Kind = MutationKind.Set,
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
}
