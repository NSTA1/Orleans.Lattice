using System.Text;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class DataReaderTests
{
    [Test]
    public async Task ScanAsync_BuildsRequestWithNormalizedSizeAndBudget()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 40);

        Assert.Multiple(() =>
        {
            Assert.That(client.LastScan!.TreeId, Is.EqualTo("tree-1"));
            Assert.That(client.LastScan!.PageSize, Is.EqualTo(50));
            Assert.That(client.LastScan!.ContinuationToken, Is.Null);
            Assert.That(client.LastScan!.ValuePreviewBudget, Is.EqualTo(DataReader.ScanPreviewBudget));
        });
    }

    [Test]
    public async Task ScanAsync_DefaultsToLiveMode()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 40);

        Assert.That(client.LastScan!.Mode, Is.EqualTo(EntryScanMode.Live));
    }

    [Test]
    public async Task ScanAsync_PassesSnapshotMode()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 40, mode: EntryScanMode.Snapshot);

        Assert.That(client.LastScan!.Mode, Is.EqualTo(EntryScanMode.Snapshot));
    }

    [Test]
    public async Task ScanAsync_PassesContinuationToken()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 25, continuationToken: "next");

        Assert.That(client.LastScan!.ContinuationToken, Is.EqualTo("next"));
    }

    [Test]
    public async Task ScanAsync_MapsEntriesAndToken()
    {
        var client = new FakeEntryStateClient
        {
            OnScan = _ => new EntryScanResponse
            {
                TreeId = "t",
                ContinuationToken = "more",
                Entries = new[]
                {
                    new EntryRecord
                    {
                        Key = "k1",
                        ValuePreview = Encoding.UTF8.GetBytes("v1"),
                        ValueLength = 2,
                        IsTombstone = false,
                    },
                },
            },
        };
        var reader = new DataReader(client);

        var page = await reader.ScanAsync("t", pageSize: 25);

        Assert.Multiple(() =>
        {
            Assert.That(page.ContinuationToken, Is.EqualTo("more"));
            Assert.That(page.HasMore, Is.True);
            Assert.That(page.Entries.Single().Key, Is.EqualTo("k1"));
            Assert.That(page.Entries.Single().ValueLength, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task ScanAsync_WithTagFilter_PassesIndexAndTag()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("orders", pageSize: 25, continuationToken: null,
            tagFilter: new TagFilter("by-status", "open"));

        Assert.Multiple(() =>
        {
            Assert.That(client.LastScan!.TreeId, Is.EqualTo("orders"));
            Assert.That(client.LastScan!.IndexName, Is.EqualTo("by-status"));
            Assert.That(client.LastScan!.Tag, Is.EqualTo("open"));
        });
    }

    [Test]
    public async Task ScanAsync_WithoutTagFilter_LeavesIndexAndTagNull()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("orders", pageSize: 25);

        Assert.Multiple(() =>
        {
            Assert.That(client.LastScan!.IndexName, Is.Null);
            Assert.That(client.LastScan!.Tag, Is.Null);
        });
    }

    [Test]
    public async Task ScanAsync_WithKeyPrefix_SetsRangeBounds()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 25, keyPrefix: "abc");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastScan!.StartInclusive, Is.EqualTo("abc"));
            Assert.That(client.LastScan!.EndExclusive, Is.EqualTo("abd"));
        });
    }

    [Test]
    public async Task ScanAsync_WithKeyPrefixAndTagFilter_IgnoresPrefixBounds()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 25, keyPrefix: "abc",
            tagFilter: new TagFilter("by-status", "open"));

        Assert.Multiple(() =>
        {
            Assert.That(client.LastScan!.StartInclusive, Is.Null);
            Assert.That(client.LastScan!.EndExclusive, Is.Null);
            Assert.That(client.LastScan!.IndexName, Is.EqualTo("by-status"));
            Assert.That(client.LastScan!.Tag, Is.EqualTo("open"));
        });
    }

    [Test]
    public async Task ScanAsync_WithEmptyKeyPrefix_LeavesRangeBoundsNull()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.ScanAsync("tree-1", pageSize: 25, keyPrefix: "");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastScan!.StartInclusive, Is.Null);
            Assert.That(client.LastScan!.EndExclusive, Is.Null);
        });
    }

    [Test]
    public void PrefixUpperBound_IncrementsLastCodeUnit()
    {
        Assert.That(DataReader.PrefixUpperBound("abc"), Is.EqualTo("abd"));
    }

    [Test]
    public void PrefixUpperBound_RollsOverTrailingMaxCodeUnits()
    {
        // 'a' followed by U+FFFF: the trailing max unit is dropped and the
        // preceding 'a' is incremented to 'b'.
        Assert.That(DataReader.PrefixUpperBound("a\uFFFF"), Is.EqualTo("b"));
    }

    [Test]
    public void PrefixUpperBound_AllMaxCodeUnits_ReturnsNull()
    {
        Assert.That(DataReader.PrefixUpperBound("\uFFFF\uFFFF"), Is.Null);
        Assert.That(DataReader.PrefixUpperBound(string.Empty), Is.Null);
    }

    [Test]
    public async Task ListTagIndexesForTreeAsync_PassesSourceTreeId_AndPagesAllEntries()
    {
        var calls = 0;
        var client = new FakeEntryStateClient
        {
            OnListTagIndexes = req =>
            {
                calls++;
                return calls == 1
                    ? new TagIndexCatalogPage
                    {
                        Entries = new[] { new TagIndexStateSummary { IndexName = "by-status", TreeId = "tag-by-status" } },
                        NextPageToken = "tag-by-status",
                    }
                    : new TagIndexCatalogPage
                    {
                        Entries = new[] { new TagIndexStateSummary { IndexName = "by-owner", TreeId = "tag-by-owner" } },
                        NextPageToken = null,
                    };
            },
        };
        var reader = new DataReader(client);

        var indexes = await reader.ListTagIndexesForTreeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastTagIndexes!.SourceTreeId, Is.EqualTo("orders"));
            Assert.That(indexes.Select(i => i.IndexName), Is.EqualTo(new[] { "by-status", "by-owner" }));
            Assert.That(indexes.Select(i => i.TreeId), Is.EqualTo(new[] { "tag-by-status", "tag-by-owner" }));
        });
    }

    [Test]
    public async Task ListTagValuesForIndexAsync_PassesTreeAndIndex_AndPagesAllValues()
    {
        var calls = 0;
        var client = new FakeEntryStateClient
        {
            OnListTagValues = req =>
            {
                calls++;
                return calls == 1
                    ? new TagValueCatalogPage
                    {
                        Entries = new[] { "closed", "open" },
                        NextPageToken = "open",
                    }
                    : new TagValueCatalogPage
                    {
                        Entries = new[] { "pending" },
                        NextPageToken = null,
                    };
            },
        };
        var reader = new DataReader(client);

        var values = await reader.ListTagValuesForIndexAsync("orders", "by-status");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastTagValues!.SourceTreeId, Is.EqualTo("orders"));
            Assert.That(client.LastTagValues!.IndexName, Is.EqualTo("by-status"));
            Assert.That(values, Is.EqualTo(new[] { "closed", "open", "pending" }));
        });
    }

    [Test]
    public async Task GetEntryAsync_Found_ReturnsMappedEntry()
    {
        var client = new FakeEntryStateClient
        {
            OnGet = r => new EntryGetResponse
            {
                TreeId = r.TreeId,
                Key = r.Key,
                Status = StateQueryStatus.Found,
                Entry = new EntryRecord
                {
                    Key = r.Key,
                    ValuePreview = Encoding.UTF8.GetBytes("hello"),
                    ValueLength = 5,
                    Hlc = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 },
                    CrdtShape = "lww",
                },
            },
        };
        var reader = new DataReader(client);

        var entry = await reader.GetEntryAsync("t", "k1");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastGet!.TreeId, Is.EqualTo("t"));
            Assert.That(client.LastGet!.Key, Is.EqualTo("k1"));
            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.Value, Is.EqualTo(Encoding.UTF8.GetBytes("hello")));
            Assert.That(entry.Hlc.Counter, Is.EqualTo(4));
            Assert.That(entry.CrdtShape, Is.EqualTo("lww"));
        });
    }

    [Test]
    public async Task GetEntryAsync_CrdtRecord_MapsCurrentMembers()
    {
        var client = new FakeEntryStateClient
        {
            OnGet = r => new EntryGetResponse
            {
                TreeId = r.TreeId,
                Key = r.Key,
                Status = StateQueryStatus.Found,
                Entry = new EntryRecord
                {
                    Key = r.Key,
                    ValuePreview = Encoding.UTF8.GetBytes("{opaque-crdt-blob}"),
                    ValueLength = 18,
                    CrdtShape = "OrSet",
                    CurrentMembers = new[]
                    {
                        new CrdtMemberValue
                        {
                            Element = Encoding.UTF8.GetBytes("apple"),
                            ReplicaId = "eu",
                            Ordinal = 1,
                        },
                    },
                },
            },
        };
        var reader = new DataReader(client);

        var entry = await reader.GetEntryAsync("t", "k1");

        Assert.That(entry, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(entry!.CrdtShape, Is.EqualTo("OrSet"));
            Assert.That(entry.CurrentMembers, Has.Count.EqualTo(1));
            Assert.That(entry.CurrentMembers[0].ElementText, Is.EqualTo("apple"));
            Assert.That(entry.CurrentMembers[0].ReplicaId, Is.EqualTo("eu"));
        });
    }

    [Test]
    public async Task GetEntryAsync_KeyNotFound_ReturnsNull()
    {
        var client = new FakeEntryStateClient
        {
            OnGet = r => new EntryGetResponse { TreeId = r.TreeId, Key = r.Key, Status = StateQueryStatus.KeyNotFound },
        };
        var reader = new DataReader(client);

        Assert.That(await reader.GetEntryAsync("t", "missing"), Is.Null);
    }

    [Test]
    public async Task GetEntryAsync_TreeNotFound_ReturnsNull()
    {
        var client = new FakeEntryStateClient
        {
            OnGet = r => new EntryGetResponse { TreeId = r.TreeId, Key = r.Key, Status = StateQueryStatus.TreeNotFound },
        };
        var reader = new DataReader(client);

        Assert.That(await reader.GetEntryAsync("t", "k"), Is.Null);
    }

    [Test]
    public async Task CancelScanAsync_BuildsRequestWithTreeAndToken()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.CancelScanAsync("tree-1", "cursor-9");

        Assert.That(client.LastCancel, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(client.LastCancel!.TreeId, Is.EqualTo("tree-1"));
            Assert.That(client.LastCancel.ContinuationToken, Is.EqualTo("cursor-9"));
        });
    }

    [Test]
    public async Task CancelScanAsync_WithEmptyToken_SkipsTheRoundTrip()
    {
        var client = new FakeEntryStateClient();
        var reader = new DataReader(client);

        await reader.CancelScanAsync("tree-1", null);
        await reader.CancelScanAsync("tree-1", string.Empty);

        Assert.That(client.LastCancel, Is.Null, "an empty token names no cursor, so no cancel call is made");
    }

    [Test]
    public async Task ListCoveredTreesForIndexAsync_PassesIndex_AndPagesAllTrees()
    {
        var calls = 0;
        var client = new FakeEntryStateClient
        {
            OnListCoveredTrees = _ =>
            {
                calls++;
                return calls == 1
                    ? new CoveredTreeCatalogPage { Entries = new[] { "eu", "us" }, NextPageToken = "us" }
                    : new CoveredTreeCatalogPage { Entries = new[] { "za" }, NextPageToken = null };
            },
        };
        var reader = new DataReader(client);

        var trees = await reader.ListCoveredTreesForIndexAsync("by-status");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastCoveredTrees!.IndexName, Is.EqualTo("by-status"));
            Assert.That(trees, Is.EqualTo(new[] { "eu", "us", "za" }));
        });
    }

    [Test]
    public async Task ListTagsForIndexAsync_PassesIndex_AndPagesAllTags()
    {
        var calls = 0;
        var client = new FakeEntryStateClient
        {
            OnListIndexTags = _ =>
            {
                calls++;
                return calls == 1
                    ? new TagValueCatalogPage { Entries = new[] { "closed", "open" }, NextPageToken = "open" }
                    : new TagValueCatalogPage { Entries = new[] { "pending" }, NextPageToken = null };
            },
        };
        var reader = new DataReader(client);

        var tags = await reader.ListTagsForIndexAsync("by-status");

        Assert.Multiple(() =>
        {
            Assert.That(client.LastIndexTags!.IndexName, Is.EqualTo("by-status"));
            Assert.That(tags, Is.EqualTo(new[] { "closed", "open", "pending" }));
        });
    }

    [Test]
    public async Task ScanTagMembersAsync_BuildsNormalizedRequest_AndMapsMembers()
    {
        var client = new FakeEntryStateClient
        {
            OnScanTagMembers = _ => new TagMemberScanPage
            {
                Entries = new[]
                {
                    new TagMember { TreeId = "eu", Key = "key-1" },
                    new TagMember { TreeId = "us", Key = "key-2" },
                },
                NextPageToken = "eu\0key-1",
            },
        };
        var reader = new DataReader(client);

        var page = await reader.ScanTagMembersAsync("by-status", "open", 50, continuationToken: null);

        Assert.Multiple(() =>
        {
            Assert.That(client.LastTagMembers!.IndexName, Is.EqualTo("by-status"));
            Assert.That(client.LastTagMembers!.Tag, Is.EqualTo("open"));
            Assert.That(client.LastTagMembers!.PageSize, Is.EqualTo(DataPaging.Normalize(50)));
            Assert.That(client.LastTagMembers!.PageToken, Is.Null);
            Assert.That(page.Members.Select(m => (m.TreeId, m.Key)),
                Is.EqualTo(new[] { ("eu", "key-1"), ("us", "key-2") }));
            Assert.That(page.ContinuationToken, Is.EqualTo("eu\0key-1"));
            Assert.That(page.HasMore, Is.True);
        });
    }

    [Test]
    public async Task ScanTagMembersAsync_EmptyPage_ReturnsEmptyWithoutContinuation()
    {
        var client = new FakeEntryStateClient
        {
            OnScanTagMembers = _ => new TagMemberScanPage(),
        };
        var reader = new DataReader(client);

        var page = await reader.ScanTagMembersAsync("by-status", "open", 50);

        Assert.Multiple(() =>
        {
            Assert.That(page.Members, Is.Empty);
            Assert.That(page.HasMore, Is.False);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task ScanTagMembersAsync_WithContinuationToken_PassesTokenThrough()
    {
        var client = new FakeEntryStateClient
        {
            OnScanTagMembers = _ => new TagMemberScanPage(),
        };
        var reader = new DataReader(client);

        await reader.ScanTagMembersAsync("by-status", "open", 50, continuationToken: "eu\0key-1");

        Assert.That(client.LastTagMembers!.PageToken, Is.EqualTo("eu\0key-1"));
    }
}
