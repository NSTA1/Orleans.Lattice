using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the claimless advance's split (#5837) under the pre-arm gate:
/// the donor closes its sibling's birth window itself, in one marker write carrying its
/// deferred bound (rule S), and the split's transfer into the sibling is appended under
/// the span's generation, attributed to the sibling, never raising the shared watermark
/// (T1, T3).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ClaimlessSplitIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
        await _cluster.WaitForAppendFenceGateAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private async Task<(ILattice Tree, Guid Leaf, int Partitions)> NewTreeAsync(string name)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            MaxLeafKeys = 8,
            MaxInternalChildren = 16,
            ShardCount = 1,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(name);
        await tree.SetAsync("k00", Encoding.UTF8.GetBytes("v"));
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{name}/0"));
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(name);
        return (tree, new Guid(hash.AsSpan(0, 16)), Math.Max(1, partitions));
    }

    private async Task<long[]> SharedWatermarksAsync(string name, int partitions)
    {
        var watermarks = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            var cut = await _cluster.Client.GetGrain<IWalShardGrain>($"{name}/{p}")
                .GetSplitCutAsync(default, null, null, CancellationToken.None);
            watermarks[p] = cut.UnstampedWatermark;
        }

        return watermarks;
    }

    private static async Task SplitOnceAsync(ILattice tree)
    {
        for (var i = 1; i <= 9; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes("v"));
        }
    }

    [Test]
    public async Task A_pre_arm_split_closes_its_sibling_with_the_donor_bound_in_one_write()
    {
        // DonorCloseWithoutDeferredBound and DonorCloseHeadAtHeads (rule S).
        const string Name = "claimless-split-close";
        var (tree, donor, _) = await NewTreeAsync(Name);

        await SplitOnceAsync(tree);
        var sibling = await _cluster.Client.GetGrain<IBPlusLeafGrain>(donor).GetNextSiblingAsync();
        Assert.That(sibling, Is.Not.Null, "precondition: the donor split");
        var marker = (await HandOffMarkerRows.ReadLeafAsync(Storage, sibling!.Value.GetGuidKey())).State;
        var close = marker.Closes.SingleOrDefault(c => c.DonorLeaf == donor);

        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Is.Empty, "the sibling's birth window was left open");
            Assert.That(close, Is.Not.Null, "the donor did not close its sibling's window with its bound");
            Assert.That(close!.DonorToken, Is.Not.EqualTo(Guid.Empty), "the bound does not name the closing activation");
            Assert.That(close.AdmittedGeneration, Is.GreaterThan(0), "the bound records no admitted generation");
            Assert.That(close.Heads, Has.All.GreaterThanOrEqualTo(0L));
        });
    }

    [Test]
    public async Task A_pre_arm_split_transfer_never_raises_the_shared_watermark()
    {
        // TransferCostWitness / TransferRaisesSharedWatermark (T3).
        const string Name = "claimless-split-transfer";
        var (tree, donor, partitions) = await NewTreeAsync(Name);
        var before = await SharedWatermarksAsync(Name, partitions);

        await SplitOnceAsync(tree);
        var sibling = await _cluster.Client.GetGrain<IBPlusLeafGrain>(donor).GetNextSiblingAsync();
        var after = await SharedWatermarksAsync(Name, partitions);

        Assert.Multiple(() =>
        {
            Assert.That(sibling, Is.Not.Null, "precondition: the donor split");
            Assert.That(after, Is.EqualTo(before), "the split's transfer raised the shared unstamped watermark");
        });
    }
    [Test]
    public async Task A_wal_records_key_is_in_the_key_space_of_the_leaf_ranges_the_unstamped_filter_compares_it_with()
    {
        // #5851 N1 (a5e2e2ef on #5873): the WAL shard's unstamped filter (K1-K6) owns a
        // record's Key against a reader's declared range with SplitBoundary.Owns. Both are the
        // tree's own key space, unprefixed: every record a leaf writes for a user key carries
        // that key, and exactly one leaf of the chain owns it.
        const string Name = "claimless-keyspace";
        var (tree, _, partitions) = await NewTreeAsync(Name);
        var keys = Enumerable.Range(0, 40).Select(i => $"tenant-a/user/{i:D3}").ToArray();
        foreach (var key in keys)
        {
            await tree.SetAsync(key, Encoding.UTF8.GetBytes("v"));
        }

        var ranges = new List<LeafKeyRange>();
        var leafId = await _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/0").GetLeftmostLeafIdAsync();
        while (leafId is { } id && ranges.Count < 1_000)
        {
            var link = await _cluster.Client.GetGrain<IBPlusLeafGrain>(id).GetKeyRangeAndNextSiblingAsync();
            ranges.Add(link.KeyRange);
            leafId = link.NextSibling;
        }

        var recordKeys = new HashSet<string>(StringComparer.Ordinal);
        for (var p = 0; p < partitions; p++)
        {
            var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{p}");
            var from = 0L;
            while (true)
            {
                var page = await wal.ReadAsync(from, 1_000, CancellationToken.None);
                foreach (var entry in page.Entries)
                {
                    if (entry.Entry.Op == MutationKind.Set && entry.Entry.Key is { } recordKey)
                    {
                        recordKeys.Add(recordKey);
                    }
                }

                if (page.Entries.Count == 0 || page.NextSequence <= from)
                {
                    break;
                }

                from = page.NextSequence;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(ranges, Has.Count.GreaterThan(1), "precondition: the tree split");
            Assert.That(recordKeys, Is.SupersetOf(keys), "a write's WAL record does not carry the user key the leaf owns");
            foreach (var key in keys)
            {
                Assert.That(ranges.Count(r => SplitBoundary.Owns(key, r.LowKeyInclusive, r.HighKeyExclusive)), Is.EqualTo(1),
                    $"'{key}': not owned by exactly one leaf range in the record key space");
            }
        });
    }
}
