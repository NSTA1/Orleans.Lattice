using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Coverage for <see cref="BackupCatalogPager"/>: the forward-token pager that
/// backs the paged Existing Backups list. It caches visited pages, advances the
/// frontier only when a new page is needed, and serves an already-visited page
/// from the cache when navigating back.
/// </summary>
[TestFixture]
public sealed class BackupCatalogPagerTests
{
    [Test]
    public async Task Reset_loads_the_first_page_with_the_filter()
    {
        var reader = new ScriptedReader();
        reader.Add(null, Page(new[] { "a", "b" }, next: "t1"));
        var filter = new BackupCatalogFilter { NamePrefix = "a" };

        var pager = new BackupCatalogPager(reader);
        await pager.ResetAsync(2, filter);

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(0));
            Assert.That(pager.Current.Entries.Select(e => e.Id), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(pager.CanGoNext, Is.True);
            Assert.That(pager.CanGoPrevious, Is.False);
            Assert.That(reader.LastFilter, Is.SameAs(filter));
        });
    }

    [Test]
    public async Task Next_advances_the_frontier_then_previous_serves_from_cache()
    {
        var reader = new ScriptedReader();
        reader.Add(null, Page(new[] { "a" }, next: "t1"));
        reader.Add("t1", Page(new[] { "b" }, next: null));

        var pager = new BackupCatalogPager(reader);
        await pager.ResetAsync(1, BackupCatalogFilter.None);

        await pager.NextAsync();
        Assert.That(pager.Current.Entries.Select(e => e.Id), Is.EqualTo(new[] { "b" }));
        Assert.That(pager.PageIndex, Is.EqualTo(1));
        Assert.That(pager.CanGoNext, Is.False, "the second page has no continuation");

        pager.Previous();
        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(0));
            Assert.That(pager.Current.Entries.Select(e => e.Id), Is.EqualTo(new[] { "a" }));

            // Going back and forward again must NOT re-read the frontier page.
            Assert.That(reader.LoadCount, Is.EqualTo(2));
        });

        await pager.NextAsync();
        Assert.That(pager.PageIndex, Is.EqualTo(1));
        Assert.That(reader.LoadCount, Is.EqualTo(2), "a cached page is served without a reader call");
    }

    [Test]
    public async Task Next_on_the_last_page_is_a_no_op()
    {
        var reader = new ScriptedReader();
        reader.Add(null, Page(new[] { "a" }, next: null));

        var pager = new BackupCatalogPager(reader);
        await pager.ResetAsync(1, BackupCatalogFilter.None);

        await pager.NextAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pager.PageIndex, Is.EqualTo(0));
            Assert.That(reader.LoadCount, Is.EqualTo(1));
        });
    }

    private static BackupListView Page(IEnumerable<string> ids, string? next) => new()
    {
        Status = BackupOperationStatus.Succeeded,
        Entries = ids.Select(id => SampleBackup.Manifest(id)).ToList(),
        NextPageToken = next,
    };

    // A reader that returns a scripted page per requested continuation token.
    private sealed class ScriptedReader : IBackupCatalogReader
    {
        private readonly Dictionary<string, BackupListView> _pages = new();

        public int LoadCount { get; private set; }
        public BackupCatalogFilter? LastFilter { get; private set; }

        public void Add(string? token, BackupListView page) => _pages[token ?? string.Empty] = page;

        public Task<BackupListView> LoadPageAsync(int pageSize = 0, string? pageToken = null, BackupCatalogFilter? filter = null, CancellationToken cancellationToken = default)
        {
            LoadCount++;
            LastFilter = filter;
            return Task.FromResult(_pages[pageToken ?? string.Empty]);
        }

        public Task<BackupCatalogSummary> LoadSummaryAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupChainDescription?> DescribeAsync(string backupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupOperationResult> TriggerFullAsync(string name, BackupScopeSelector scope, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupOperationResult> TriggerSetAsync(string name, IReadOnlyList<BackupScopeSelector> scopes, bool crossTreeConsistent, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupOperationResult> TriggerIncrementalAsync(string name, BackupScopeSelector scope, string baseBackupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupOperationResult> RestoreAsync(string backupId, string targetTreeId, LatticeRestoreMode mode = LatticeRestoreMode.InPlace, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupOperationResult> DeleteAsync(string backupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupOperationResult> ScheduleAsync(BackupScopeSelector scope, bool incremental, TimeSpan interval, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
