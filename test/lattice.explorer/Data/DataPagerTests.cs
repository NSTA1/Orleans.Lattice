using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class DataPagerTests
{
    [Test]
    public async Task ResetAsync_LoadsFirstPage()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 3);
        var pager = new DataPager(reader);

        await pager.ResetAsync("tree-1", pageSize: 1);

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(0));
            Assert.That(KeysOf(pager), Is.EqualTo(new[] { "k0" }));
            Assert.That(pager.CanGoPrevious, Is.False);
            Assert.That(pager.CanGoNext, Is.True);
            Assert.That(reader.ScanCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task NextAsync_AdvancesFrontierByCallingReader()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 3);
        var pager = new DataPager(reader);
        await pager.ResetAsync("tree-1", pageSize: 1);

        await pager.NextAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(1));
            Assert.That(KeysOf(pager), Is.EqualTo(new[] { "k1" }));
            Assert.That(reader.ScanCalls, Is.EqualTo(2));
        });
    }

    // Regression for the "occasional" Data-tab InvalidArgument: the snapshot
    // cursor is forward-only, so navigating Next -> Prev -> Next must serve the
    // revisited page from cache instead of replaying the (now consumed/closed)
    // continuation token, which the server rejects with InvalidArgument.
    [Test]
    public async Task NextAfterPrevious_ServesFromCache_WithoutReplayingCursor()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 2);
        var pager = new DataPager(reader);

        await pager.ResetAsync("tree-1", pageSize: 1); // page 0, opens cursor
        await pager.NextAsync();                        // page 1, drains + closes cursor
        pager.Previous();                               // back to page 0 (cache)

        var scansBeforeRevisit = reader.ScanCalls;

        // With the old token-replay paging this threw the server's
        // "continuation token is invalid or has expired" ArgumentException.
        Assert.DoesNotThrowAsync(async () => await pager.NextAsync());

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(1));
            Assert.That(KeysOf(pager), Is.EqualTo(new[] { "k1" }));
            Assert.That(reader.ScanCalls, Is.EqualTo(scansBeforeRevisit), "revisiting a cached page must not call the reader");
        });
    }

    [Test]
    public async Task FullBackAndForthTraversal_NeverReplaysCursorAndPreservesOrder()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 3);
        var pager = new DataPager(reader);

        await pager.ResetAsync("tree-1", pageSize: 1);
        await pager.NextAsync(); // page 1
        await pager.NextAsync(); // page 2 (frontier, last)
        Assert.That(reader.ScanCalls, Is.EqualTo(3));
        Assert.That(pager.CanGoNext, Is.False);

        pager.Previous(); // page 1
        pager.Previous(); // page 0
        Assert.That(pager.PageIndex, Is.EqualTo(0));

        await pager.NextAsync(); // page 1 (cache)
        await pager.NextAsync(); // page 2 (cache)

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(2));
            Assert.That(KeysOf(pager), Is.EqualTo(new[] { "k2" }));
            Assert.That(reader.ScanCalls, Is.EqualTo(3), "no extra reader calls for cached pages");
        });
    }

    [Test]
    public async Task NextAsync_AtDrainedFrontier_IsNoOp()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 1);
        var pager = new DataPager(reader);
        await pager.ResetAsync("tree-1", pageSize: 1);

        await pager.NextAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(0));
            Assert.That(pager.CanGoNext, Is.False);
            Assert.That(reader.ScanCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ResetAsync_AfterFailure_LeavesPreviousPagesIntact()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 2);
        var pager = new DataPager(reader);
        await pager.ResetAsync("tree-1", pageSize: 1);

        reader.FailNextScan = true;
        Assert.ThrowsAsync<InvalidOperationException>(async () => await pager.ResetAsync("tree-1", pageSize: 1));

        Assert.Multiple(() =>
        {
            Assert.That(KeysOf(pager), Is.EqualTo(new[] { "k0" }), "a failed reset must not discard the visible page");
            Assert.That(reader.CancelCalls, Is.EqualTo(0), "a failed reset must not release the still-valid cursor");
        });
    }

    [Test]
    public async Task ResetAsync_ReleasesThePreviousSnapshotCursor()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 3);
        var pager = new DataPager(reader);

        await pager.ResetAsync("tree-1", pageSize: 1); // opens cursor-1 (still live)
        await pager.ResetAsync("tree-1", pageSize: 1); // opens cursor-2, releases cursor-1

        Assert.Multiple(() =>
        {
            Assert.That(reader.CancelCalls, Is.EqualTo(1), "the superseded snapshot's cursor must be released");
            Assert.That(reader.LastCancelToken, Is.EqualTo("cursor-1"));
            Assert.That(pager.CanGoNext, Is.True, "the new snapshot remains usable");
        });
    }

    [Test]
    public async Task ResetAsync_AfterDrainedScan_DoesNotCancel()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 1);
        var pager = new DataPager(reader);

        await pager.ResetAsync("tree-1", pageSize: 1); // single page: cursor already drained/closed
        await pager.ResetAsync("tree-1", pageSize: 1);

        Assert.That(reader.CancelCalls, Is.EqualTo(0), "a drained snapshot has no open cursor to release");
    }

    [Test]
    public async Task CloseAsync_ReleasesTheLiveCursor_AndIsIdempotent()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 3);
        var pager = new DataPager(reader);
        await pager.ResetAsync("tree-1", pageSize: 1); // cursor-1 live

        await pager.CloseAsync();
        await pager.CloseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reader.CancelCalls, Is.EqualTo(1), "close releases the cursor once, then is a no-op");
            Assert.That(reader.LastCancelToken, Is.EqualTo("cursor-1"));
        });
    }

    [Test]
    public async Task CloseAsync_AfterDrain_IsNoOp()
    {
        var reader = new ForwardOnlyCursorReader(pageCount: 1);
        var pager = new DataPager(reader);
        await pager.ResetAsync("tree-1", pageSize: 1); // single page: drained, no live cursor

        await pager.CloseAsync();

        Assert.That(reader.CancelCalls, Is.EqualTo(0), "a drained snapshot has no open cursor to release");
    }

    private static string[] KeysOf(DataPager pager) => pager.Current.Entries.Select(e => e.Key).ToArray();

    /// <summary>
    /// Emulates the state-API snapshot cursor: a single forward-only cursor that
    /// advances one page per call and is closed once drained. A fresh scan (null
    /// token) opens a new cursor; replaying a stale or closed token is rejected,
    /// exactly as the server does (ArgumentException -> gRPC InvalidArgument).
    /// </summary>
    private sealed class ForwardOnlyCursorReader(int pageCount) : IDataReader
    {
        private int _cursorGeneration;
        private string? _liveToken;
        private int _position;

        public int ScanCalls { get; private set; }

        public int CancelCalls { get; private set; }

        public string? LastCancelToken { get; private set; }

        public bool FailNextScan { get; set; }

        public Task<DataPage> ScanAsync(
            string treeId,
            int pageSize,
            string? continuationToken = null,
            TagFilter? tagFilter = null,
            string? keyPrefix = null,
            CancellationToken cancellationToken = default)
        {
            ScanCalls++;

            if (FailNextScan)
            {
                FailNextScan = false;
                throw new InvalidOperationException("scan failed");
            }

            if (string.IsNullOrEmpty(continuationToken))
            {
                _cursorGeneration++;
                _liveToken = $"cursor-{_cursorGeneration}";
                _position = 0;
            }
            else if (continuationToken != _liveToken)
            {
                throw new ArgumentException("The continuation token is invalid or has expired.");
            }

            var entry = new DataEntry { Key = $"k{_position}" };
            _position++;

            var hasMore = _position < pageCount;
            var token = hasMore ? _liveToken : null;
            if (!hasMore)
            {
                _liveToken = null; // server closes a drained cursor
            }

            return Task.FromResult(new DataPage
            {
                Entries = new[] { entry },
                ContinuationToken = token,
            });
        }

        public Task<DataEntry?> GetEntryAsync(string treeId, string key, CancellationToken cancellationToken = default)
            => Task.FromResult<DataEntry?>(null);

        public Task CancelScanAsync(string treeId, string? continuationToken, CancellationToken cancellationToken = default)
        {
            CancelCalls++;
            LastCancelToken = continuationToken;

            // Emulate the server closing the named cursor: a later replay of this
            // token is then rejected. Cancelling a stale token is a tolerated
            // no-op (it never matches the live cursor).
            if (!string.IsNullOrEmpty(continuationToken) && continuationToken == _liveToken)
            {
                _liveToken = null;
            }

            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<string>> ListTagIndexesForTreeAsync(string treeId, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
    }
}
