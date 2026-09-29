using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// An in-memory state API: trees, views, entries, history, dead letters, metrics
/// and tag indexes that a test seeds, with scriptable faults, gates that hold a
/// call until the test releases it, and change feeds the test writes into. The
/// real Core readers wrap it, so the Data area is tested through them.
/// </summary>
internal sealed class FakeStateClient : ILatticeStateClient
{
    private int _observerCount;

    /// <summary>The tree catalogue, in the order the fake answers it.</summary>
    public List<TreeCatalogEntry> Trees { get; } = [];

    /// <summary>The view catalogue.</summary>
    public List<ViewStateSummary> Views { get; } = [];

    /// <summary>The tag-index catalogue.</summary>
    public List<TagIndexStateSummary> TagIndexes { get; } = [];

    /// <summary>Each tree's entries, by state id.</summary>
    public Dictionary<string, SortedDictionary<string, EntryRecord>> Entries { get; } = new(StringComparer.Ordinal);

    /// <summary>Each key's revisions, oldest first.</summary>
    public Dictionary<(string Tree, string Key), List<EntryRevisionRecord>> History { get; } = [];

    /// <summary>Each tree's dead letters.</summary>
    public Dictionary<string, List<DeadLetterEntryRecord>> DeadLetters { get; } = new(StringComparer.Ordinal);

    /// <summary>Each tree's metrics.</summary>
    public Dictionary<string, TreeMetrics> Metrics { get; } = new(StringComparer.Ordinal);

    /// <summary>Each index's covered trees.</summary>
    public Dictionary<string, List<string>> Covered { get; } = new(StringComparer.Ordinal);

    /// <summary>Each index's members by tag.</summary>
    public Dictionary<(string Index, string Tag), List<TagMember>> Members { get; } = [];

    /// <summary>When set, the named call throws what it returns (null for no fault).</summary>
    public Func<string, Exception?>? Fault { get; set; }

    /// <summary>When set, the tree catalogue waits for it before answering.</summary>
    public TaskCompletionSource? CatalogGate { get; set; }

    /// <summary>When set, a continuation scan throws this.</summary>
    public Exception? ContinuationFault { get; set; }

    /// <summary>Every call made, by name.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>Every change-feed request made, oldest first.</summary>
    public List<StateObserveRequest> ObserveRequests { get; } = [];

    /// <summary>The change feeds opened, oldest first; write into one to deliver a change.</summary>
    public List<Channel<StateChangeNotification>> Feeds { get; } = [];

    /// <summary>The number of open change feeds.</summary>
    public int OpenFeeds => Volatile.Read(ref _observerCount);

    /// <summary>A tree catalogue entry.</summary>
    public static TreeCatalogEntry Tree(string id, int shards = 4, TreeLifecycleState lifecycle = TreeLifecycleState.Active, string? restoreShadowOf = null) =>
        new() { TreeId = id, ShardCount = shards, Lifecycle = lifecycle, Config = new TreeConfigSummary { ShardCount = shards }, RestoreShadowOfTreeId = restoreShadowOf };

    /// <summary>An entry holding <paramref name="value"/> as UTF-8.</summary>
    public static EntryRecord Entry(string key, string value, long ticks = 638_000_000_000_000_000) =>
        Entry(key, Encoding.UTF8.GetBytes(value), ticks);

    /// <summary>An entry holding <paramref name="value"/>.</summary>
    public static EntryRecord Entry(string key, byte[] value, long ticks = 638_000_000_000_000_000) =>
        new() { Key = key, ValuePreview = value, ValueLength = value.Length, Hlc = new HybridLogicalClock { WallClockTicks = ticks } };

    /// <summary>Adds a tree with <paramref name="keys"/> entries named <c>{prefix}{n:D4}</c>.</summary>
    public FakeStateClient WithTree(string id, int keys = 0, string prefix = "key/", int shards = 4)
    {
        Trees.Add(Tree(id, shards));
        var entries = new SortedDictionary<string, EntryRecord>(StringComparer.Ordinal);
        for (var i = 0; i < keys; i++)
        {
            var key = prefix + i.ToString("D4", CultureInfo.InvariantCulture);
            entries[key] = Entry(key, $"{{\"n\":{i}}}");
        }

        Entries[id] = entries;
        return this;
    }

    /// <inheritdoc />
    public async Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListTreesAsync));
        if (CatalogGate is { } gate)
        {
            await gate.Task.WaitAsync(cancellationToken);
        }

        ThrowIfFaulted(nameof(ListTreesAsync));
        var (items, next) = Page(Trees, request.PageToken, request.PageSize);
        return new TreeCatalogPage { Entries = items, NextPageToken = next };
    }

    /// <inheritdoc />
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListViewsAsync));
        ThrowIfFaulted(nameof(ListViewsAsync));
        var (items, next) = Page(Views, request.PageToken, request.PageSize);
        return Task.FromResult(new ViewCatalogPage { Entries = items, NextPageToken = next });
    }

    /// <inheritdoc />
    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListTagIndexesAsync));
        ThrowIfFaulted(nameof(ListTagIndexesAsync));
        var matching = TagIndexes
            .Where(index => request.SourceTreeId is null || (Covered.TryGetValue(index.IndexName, out var trees) && trees.Contains(request.SourceTreeId)))
            .ToList();
        return Task.FromResult(new TagIndexCatalogPage { Entries = matching });
    }

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListTagValuesAsync));
        return Task.FromResult(new TagValueCatalogPage { Entries = TagsOf(request.IndexName) });
    }

    /// <inheritdoc />
    public Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListCoveredTreesAsync));
        return Task.FromResult(new CoveredTreeCatalogPage
        {
            Entries = request.IndexName is { } index && Covered.TryGetValue(index, out var trees) ? trees : [],
        });
    }

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListIndexTagsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListIndexTagsAsync));
        return Task.FromResult(new TagValueCatalogPage { Entries = TagsOf(request.IndexName) });
    }

    /// <inheritdoc />
    public Task<TagMemberScanPage> ScanTagMembersAsync(TagMemberScanRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ScanTagMembersAsync));
        ThrowIfFaulted(nameof(ScanTagMembersAsync));
        var members = Members.TryGetValue((request.IndexName, request.Tag), out var found) ? found : [];
        var (items, next) = Page(members, request.PageToken, request.PageSize);
        return Task.FromResult(new TagMemberScanPage { Entries = items, NextPageToken = next });
    }

    /// <inheritdoc />
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ScanEntriesAsync) + ":" + (request.ContinuationToken ?? "first"));
        ThrowIfFaulted(nameof(ScanEntriesAsync));
        if (request.ContinuationToken is not null && ContinuationFault is { } fault)
        {
            throw fault;
        }

        IEnumerable<EntryRecord> source = Entries.TryGetValue(request.TreeId, out var entries) ? entries.Values : [];
        if (request.IndexName is { } index && request.Tag is { } tag)
        {
            var tagged = Members.TryGetValue((index, tag), out var members)
                ? members.Where(member => member.TreeId == request.TreeId).Select(member => member.Key).ToHashSet(StringComparer.Ordinal)
                : [];
            source = source.Where(entry => tagged.Contains(entry.Key));
        }
        else
        {
            source = source.Where(entry =>
                (request.StartInclusive is null || string.CompareOrdinal(entry.Key, request.StartInclusive) >= 0)
                && (request.EndExclusive is null || string.CompareOrdinal(entry.Key, request.EndExclusive) < 0));
        }

        var (items, next) = Page(source.ToList(), request.ContinuationToken, request.PageSize);
        return Task.FromResult(new EntryScanResponse { TreeId = request.TreeId, Entries = items, ContinuationToken = next });
    }

    /// <inheritdoc />
    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(GetEntryAsync) + ":" + request.Key);
        ThrowIfFaulted(nameof(GetEntryAsync));
        var found = Entries.TryGetValue(request.TreeId, out var entries) && entries.TryGetValue(request.Key, out var entry) ? entry : null;
        return Task.FromResult(new EntryGetResponse
        {
            TreeId = request.TreeId,
            Key = request.Key,
            Status = found is null ? StateQueryStatus.KeyNotFound : StateQueryStatus.Found,
            Entry = found,
        });
    }

    /// <inheritdoc />
    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(GetEntryHistoryAsync) + ":" + (request.ContinuationToken ?? "first"));
        ThrowIfFaulted(nameof(GetEntryHistoryAsync));
        var revisions = History.TryGetValue((request.TreeId, request.Key), out var found) ? found : [];
        IEnumerable<EntryRevisionRecord> ordered = revisions.Where(revision => request.ToHlc is not { } to || revision.Hlc <= to);
        if (request.Reverse)
        {
            ordered = ordered.Reverse();
        }

        var (items, next) = Page(ordered.ToList(), request.ContinuationToken, request.Limit);
        return Task.FromResult(new EntryHistoryResponse
        {
            TreeId = request.TreeId,
            Key = request.Key,
            Status = revisions.Count == 0 ? StateQueryStatus.KeyNotFound : StateQueryStatus.Found,
            Revisions = items,
            ContinuationToken = next,
        });
    }

    /// <inheritdoc />
    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(CancelScanAsync));
        return Task.FromResult(new EntryScanCancelResponse());
    }

    /// <inheritdoc />
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(GetMetricsSnapshotAsync));
        ThrowIfFaulted(nameof(GetMetricsSnapshotAsync));
        var trees = request.TreeIds is { } ids ? ids.Where(Metrics.ContainsKey).Select(id => Metrics[id]).ToList() : Metrics.Values.ToList();
        return Task.FromResult(new TreeMetricsSnapshot { Trees = trees });
    }

    /// <inheritdoc />
    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public Task<DeadLetterCountResponse> GetDeadLetterCountAsync(DeadLetterCountRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(GetDeadLetterCountAsync));
        ThrowIfFaulted(nameof(GetDeadLetterCountAsync));
        return Task.FromResult(new DeadLetterCountResponse
        {
            TreeId = request.TreeId,
            Count = DeadLetters.TryGetValue(request.TreeId, out var letters) ? letters.Count : 0,
        });
    }

    /// <inheritdoc />
    public Task<DeadLetterQueuePage> ListDeadLettersAsync(DeadLetterQueueRequest request, CancellationToken cancellationToken = default)
    {
        Record(nameof(ListDeadLettersAsync) + ":" + (request.PageToken ?? "first"));
        ThrowIfFaulted(nameof(ListDeadLettersAsync));
        var letters = DeadLetters.TryGetValue(request.TreeId, out var found) ? found : [];
        var (items, next) = Page(letters, request.PageToken, request.PageSize);
        return Task.FromResult(new DeadLetterQueuePage { Entries = items, NextPageToken = next });
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(
        StateObserveRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Record(nameof(ObserveChangesAsync));
        ObserveRequests.Add(request);
        var feed = Channel.CreateUnbounded<StateChangeNotification>();
        Feeds.Add(feed);
        ThrowIfFaulted(nameof(ObserveChangesAsync));
        Interlocked.Increment(ref _observerCount);
        try
        {
            await foreach (var change in feed.Reader.ReadAllAsync(cancellationToken))
            {
                yield return change;
            }
        }
        finally
        {
            Interlocked.Decrement(ref _observerCount);
        }
    }

    /// <inheritdoc />
    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    /// <summary>A change notification for <paramref name="key"/>.</summary>
    public static StateChangeNotification Change(string tree, string key, StateChangeKind kind = StateChangeKind.Set, long ticks = 639_000_000_000_000_000) =>
        new() { TreeId = tree, Key = key, Kind = kind, Hlc = new HybridLogicalClock { WallClockTicks = ticks }, Position = "p" + ticks.ToString(CultureInfo.InvariantCulture) };

    private static (List<T> Items, string? Next) Page<T>(IReadOnlyList<T> items, string? token, int pageSize)
    {
        var offset = token is null ? 0 : int.Parse(token, CultureInfo.InvariantCulture);
        var size = pageSize <= 0 ? 100 : pageSize;
        var page = items.Skip(offset).Take(size).ToList();
        var next = offset + size < items.Count ? (offset + size).ToString(CultureInfo.InvariantCulture) : null;
        return (page, next);
    }

    private List<string> TagsOf(string? index) =>
        [.. Members.Keys.Where(key => key.Index == index).Select(key => key.Tag).Distinct(StringComparer.Ordinal).OrderBy(tag => tag, StringComparer.Ordinal)];

    private void Record(string call)
    {
        lock (Calls)
        {
            Calls.Add(call);
        }
    }

    private void ThrowIfFaulted(string call)
    {
        if (Fault?.Invoke(call) is { } exception)
        {
            throw exception;
        }
    }
}
