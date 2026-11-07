using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for a WAL partition's append-epoch directory (#5634): every listed leaf
/// is enumerable, listing and delisting are idempotent, a bucket pages past
/// <see cref="WalAppendEpochDirectory.PageCapacity"/> and reuses freed space, and a
/// full page stays within the provider's row limits in binary and in JSON.
/// </summary>
[TestFixture]
public sealed class WalAppendEpochDirectoryTests
{
    private const string Tree = "dir-tree";
    private const int Partition = 3;

    private static List<Guid> LeavesInBucket(int bucket, int count)
    {
        var leaves = new List<Guid>(count);
        var seed = 0;
        while (leaves.Count < count)
        {
            var candidate = new Guid(seed++, 5634, 0, new byte[8]);
            if (WalAppendEpochDirectory.BucketOf(candidate) == bucket)
            {
                leaves.Add(candidate);
            }
        }

        return leaves;
    }

    [Test]
    public async Task A_listed_leaf_is_enumerated_once_with_its_listing_time()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var leaf = Guid.NewGuid();

        await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, leaf, 100);
        await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, leaf, 200);
        var listed = await WalAppendEpochDirectory.ListAsync(storage, Tree, Partition, CancellationToken.None);
        var otherPartition = await WalAppendEpochDirectory.ListAsync(storage, Tree, Partition + 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(listed.Keys, Is.EqualTo(new[] { leaf }), "a repeated listing is a no-op");
            Assert.That(listed[leaf], Is.EqualTo(100), "the first listing's time is kept");
            Assert.That(otherPartition, Is.Empty, "a listing belongs to its partition");
        });
    }

    [Test]
    public async Task A_delisted_leaf_is_no_longer_enumerated()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var kept = Guid.NewGuid();
        var removed = Guid.NewGuid();
        await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, kept, 1);
        await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, removed, 1);

        await WalAppendEpochDirectory.RemoveAsync(storage, Tree, Partition, removed);
        await WalAppendEpochDirectory.RemoveAsync(storage, Tree, Partition, Guid.NewGuid());
        var listed = await WalAppendEpochDirectory.ListAsync(storage, Tree, Partition, CancellationToken.None);

        Assert.That(listed.Keys, Is.EquivalentTo(new[] { kept }), "delisting an unlisted leaf is a no-op");
    }

    [Test]
    public async Task A_bucket_pages_past_capacity_and_reuses_freed_space()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var leaves = LeavesInBucket(bucket: 7, WalAppendEpochDirectory.PageCapacity + 2);
        foreach (var leaf in leaves)
        {
            await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, leaf, 1);
        }

        var firstPage = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, Tree, Partition, 7, 0);
        var secondPage = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, Tree, Partition, 7, 1);
        var listed = await WalAppendEpochDirectory.ListAsync(storage, Tree, Partition, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(firstPage.State.Leaves, Has.Count.EqualTo(WalAppendEpochDirectory.PageCapacity), "a page never exceeds its capacity");
            Assert.That(secondPage.State.Leaves, Has.Count.EqualTo(2));
            Assert.That(listed.Keys, Is.EquivalentTo(leaves), "every page of the bucket is enumerated");
        });

        // A delisting from the first page frees a slot the next listing reuses.
        await WalAppendEpochDirectory.RemoveAsync(storage, Tree, Partition, leaves[0]);
        var newcomer = LeavesInBucket(bucket: 7, WalAppendEpochDirectory.PageCapacity + 3)[^1];
        await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, newcomer, 1);
        firstPage = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, Tree, Partition, 7, 0);
        secondPage = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, Tree, Partition, 7, 1);

        Assert.Multiple(() =>
        {
            Assert.That(firstPage.State.Leaves, Does.Contain(newcomer));
            Assert.That(secondPage.State.Leaves, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task Every_page_of_every_bucket_is_enumerated()
    {
        // Written directly, so the enumeration is tested independently of how the
        // listing fills pages.
        var storage = new EnumerableMemoryGrainStorage();
        var onFirstPage = Guid.NewGuid();
        var onSecondPage = Guid.NewGuid();
        var inLastBucket = Guid.NewGuid();
        await WritePageAsync(storage, bucket: 4, page: 0, onFirstPage);
        await WritePageAsync(storage, bucket: 4, page: 1, onSecondPage);
        await WritePageAsync(storage, bucket: WalAppendEpochDirectory.Buckets - 1, page: 0, inLastBucket);

        var listed = await WalAppendEpochDirectory.ListAsync(storage, Tree, Partition, CancellationToken.None);

        Assert.That(listed.Keys, Is.EquivalentTo(new[] { onFirstPage, onSecondPage, inLastBucket }));
    }

    private static Task WritePageAsync(IGrainStorage storage, int bucket, int page, Guid leaf) =>
        WalAppendFenceRows.WriteAsync(
            storage,
            WalAppendFenceRows.DirectoryPageGrainType,
            WalAppendFenceRows.DirectoryPageKey(Tree, Partition, bucket, page),
            new GrainState<WalAppendEpochDirectoryPageState>(new WalAppendEpochDirectoryPageState
            {
                Leaves = new List<Guid> { leaf },
                AddedUtcTicks = new List<long> { 1 },
            }));

    [Test]
    public async Task A_leaf_listed_on_a_later_page_is_not_listed_twice_after_space_frees()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var leaves = LeavesInBucket(bucket: 9, WalAppendEpochDirectory.PageCapacity + 1);
        foreach (var leaf in leaves)
        {
            await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, leaf, 1);
        }

        // The last leaf sits on page 1; free a slot on page 0, then list it again.
        await WalAppendEpochDirectory.RemoveAsync(storage, Tree, Partition, leaves[0]);
        await WalAppendEpochDirectory.AddAsync(storage, Tree, Partition, leaves[^1], 2);
        var firstPage = await WalAppendFenceRows.ReadDirectoryPageAsync(storage, Tree, Partition, 9, 0);

        Assert.That(firstPage.State.Leaves, Does.Not.Contain(leaves[^1]));
    }

    [Test]
    public void A_full_page_stays_within_an_azure_table_property_in_json_and_binary()
    {
        // Azure Table: a property holds at most 64 KiB (32K UTF-16 characters for a
        // string); an entity at most 1 MiB.
        var page = new WalAppendEpochDirectoryPageState();
        for (var i = 0; i < WalAppendEpochDirectory.PageCapacity; i++)
        {
            page.Leaves.Add(Guid.NewGuid());
            page.AddedUtcTicks.Add(DateTime.MaxValue.Ticks);
        }

        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddSingleton<OrleansJsonSerializer>();
        services.AddSingleton<JsonGrainStorageSerializer>();
        using var provider = services.BuildServiceProvider();
        var json = provider.GetRequiredService<JsonGrainStorageSerializer>().Serialize(page).ToString();
        var binary = new LatticeGrainStorageSerializer(
            provider.GetRequiredService<Serializer>(),
            provider.GetRequiredService<JsonGrainStorageSerializer>()).Serialize(page).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(json.Length, Is.LessThan(32 * 1024), "JSON characters, within a 64 KiB string property");
            Assert.That(binary.Length, Is.LessThan(32 * 1024), "binary bytes, within a 64 KiB binary property");
        });
    }

    [Test]
    public void The_bucket_of_a_leaf_is_stable_and_in_range()
    {
        // The bucket is part of the row-key format: a listing written by one build
        // must be found by the next, so the function is pinned to a known value.
        var leaf = new Guid("0f0f0f0f-0000-0000-0000-000000005634");

        Assert.Multiple(() =>
        {
            Assert.That(WalAppendEpochDirectory.BucketOf(leaf), Is.EqualTo(30));
            Assert.That(Enumerable.Range(0, 1000).Select(i => WalAppendEpochDirectory.BucketOf(Guid.NewGuid())),
                Is.All.InRange(0, WalAppendEpochDirectory.Buckets - 1));
        });
    }
}
