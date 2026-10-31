using System.Runtime.CompilerServices;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for <see cref="BackupCatalogIndexQuery"/>'s degenerate arms - the
/// branches an ordinary listing never takes because the index view the happy
/// path reads is always well formed and always caught up. Each one here is
/// driven from a raw index view that emits index keys and rows directly, rather
/// than from encoded manifests, because the encoder cannot produce a malformed
/// key or a repeated backup id by construction.
/// </summary>
public sealed partial class BackupCatalogIndexQueryTests
{
    private const char Sep = BackupConstants.KeySeparator;

    [Test]
    public async Task A_lagging_index_is_answered_from_the_catalog_so_no_backup_is_missing()
    {
        // Issue #5450: an index still draining a burst (or rebuilding after it fell
        // off the catalog's write-ahead log) lacks a backup the catalog already
        // holds. When the read-your-writes catch-up times out the listing is served
        // from the authoritative catalog, so the just-captured backup is listed.
        var indexed = Manifest("indexed", DateTimeOffset.UnixEpoch.AddHours(1));
        var unindexed = Manifest("unindexed", DateTimeOffset.UnixEpoch.AddHours(2));
        var catalog = new FakeCatalog(indexed, unindexed);
        var view = new RawIndexView(headWaitThrowsTimeout: true, Row(indexed));

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.Multiple(() =>
        {
            Assert.That(view.HeadWaits, Is.EqualTo(1), "the head wait really was attempted");
            Assert.That(view.Scans, Is.Zero, "a lagging index is not read");
            Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "unindexed", "indexed" }));
        });
    }

    [Test]
    public async Task A_caught_up_index_is_served_without_a_catalog_scan()
    {
        var live = Manifest("live", DateTimeOffset.UnixEpoch.AddHours(1));
        var catalog = new FakeCatalog(live);
        var view = new RawIndexView(headWaitThrowsTimeout: false, Row(live));

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.Multiple(() =>
        {
            Assert.That(view.Scans, Is.EqualTo(1), "the caught-up index serves the listing");
            Assert.That(catalog.ListCalls, Is.Zero, "the catalog is read per row, never scanned");
            Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "live" }));
        });
    }

    [Test]
    public async Task A_group_carrying_the_same_backup_twice_lists_it_once()
    {
        // A stale index generation can leave two rows for one backup inside a
        // single logical group. The per-group de-duplication collapses them, so
        // the manifest is read and listed once rather than repeated in the row.
        var live = Manifest("live", DateTimeOffset.UnixEpoch.AddHours(1));
        var catalog = new FakeCatalog(live);
        var key = BackupCatalogIndexKey.Encode(live);
        var view = new RawIndexView(
            headWaitThrowsTimeout: false,
            (key, IndexRow(live)),
            (key + "-dup", IndexRow(live)));

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "live" }));
            Assert.That(catalog.GetCalls, Is.EqualTo(1),
                "the duplicate row is skipped before the authoritative catalog read, not after it");
        });
    }

    [Test]
    public async Task Kind_filter_rejects_a_full_backup_whether_or_not_an_increment_builds_on_it()
    {
        // "adhoc" is nobody's base and "base" carries an increment; both are full,
        // so the incremental kind filter is what drops each of them.
        var catalog = new FakeCatalog(
            Manifest("adhoc", DateTimeOffset.UnixEpoch.AddHours(1)),
            Manifest("base", DateTimeOffset.UnixEpoch.AddHours(2)),
            Manifest("inc", DateTimeOffset.UnixEpoch.AddHours(3), kind: BackupKind.Incremental, baseBackupId: "base"));

        var page = await QueryAsync(catalog, Request(kind: BackupKind.Incremental));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "inc" }),
            "both full backups are dropped by the kind filter");
    }

    [Test]
    public async Task Created_prefix_filter_matches_the_earliest_member_of_a_set()
    {
        // A set's members carry distinct capture times. The created filter is
        // evaluated against the earliest of them, so a set whose first-scanned
        // member falls outside the prefix still matches on an earlier sibling.
        var setCreated = new DateTimeOffset(2024, 3, 10, 0, 0, 0, TimeSpan.Zero);
        var early = new DateTimeOffset(2024, 3, 9, 23, 0, 0, TimeSpan.Zero);
        var late = new DateTimeOffset(2024, 3, 11, 1, 0, 0, TimeSpan.Zero);

        // Index order within the group is by backup id, so "a" is scanned first
        // and carries the later time; "b" is the earlier sibling the fold finds.
        var catalog = new FakeCatalog(
            Manifest("a", late, setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated),
            Manifest("b", early, setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated));

        var matched = await QueryAsync(catalog, Request(createdPrefix: "2024-03-09"));
        var unmatched = await QueryAsync(catalog, Request(createdPrefix: "2024-03-11"));

        Assert.Multiple(() =>
        {
            Assert.That(matched.Entries.Select(e => e.Id), Is.EquivalentTo(new[] { "a", "b" }),
                "the earliest member's rendering is what the prefix is tested against");
            Assert.That(unmatched.Entries, Is.Empty,
                "the later member's rendering is not what the prefix is tested against");
        });
    }

    [Test]
    public async Task An_index_key_with_no_separator_is_its_own_group_key()
    {
        // A key shape no encoder produces, but one a corrupt or older-generation
        // index row can hold. It must form a group of its own rather than fault
        // the scan or merge into a neighbour.
        var lone = Manifest("lone", DateTimeOffset.UnixEpoch.AddHours(2));
        var other = Manifest("other", DateTimeOffset.UnixEpoch.AddHours(1));
        var catalog = new FakeCatalog(lone, other);
        var view = new RawIndexView(
            headWaitThrowsTimeout: false,
            ("0000malformedkey", IndexRow(lone)),
            (BackupCatalogIndexKey.Encode(other), IndexRow(other)));

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "lone", "other" }),
            "a separator-free key groups alone and is still listed");
    }

    [Test]
    public async Task An_index_key_with_one_separator_is_its_own_group_key()
    {
        // The encoder writes two separators ({ticks}|{group}|{backup}); a key
        // truncated to one has no group/backup boundary to cut at, so the whole
        // key is the group key.
        var truncated = Manifest("truncated", DateTimeOffset.UnixEpoch.AddHours(2));
        var other = Manifest("other", DateTimeOffset.UnixEpoch.AddHours(1));
        var catalog = new FakeCatalog(truncated, other);
        var view = new RawIndexView(
            headWaitThrowsTimeout: false,
            ("0000" + Sep + "truncated", IndexRow(truncated)),
            (BackupCatalogIndexKey.Encode(other), IndexRow(other)));

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "truncated", "other" }),
            "a single-separator key groups alone and is still listed");
    }

    private static (string Key, byte[] Row) Row(BackupManifest manifest) =>
        (BackupCatalogIndexKey.Encode(manifest), IndexRow(manifest));

    private static byte[] IndexRow(BackupManifest manifest) =>
        JsonLatticeSerializer<BackupCatalogIndexRow>.Default.Serialize(new BackupCatalogIndexRow
        {
            BackupId = manifest.Id,
            Name = manifest.Name,
            Kind = manifest.Kind,
            TreeId = manifest.Scope.TreeId,
            CreatedAtUtc = manifest.CreatedAtUtc,
            SetId = manifest.SetId,
            SetName = manifest.SetName,
            BaseBackupId = manifest.BaseBackupId,
        });

    // An index view over verbatim (key, row) pairs, so a test can express a key
    // shape - malformed, truncated, or repeated - that BackupCatalogIndexKey
    // cannot encode. Pairs are streamed in the order given.
    private sealed class RawIndexView(bool headWaitThrowsTimeout, params (string Key, byte[] Row)[] entries)
        : ILatticeView
    {
        private readonly List<KeyValuePair<string, byte[]>> _entries =
            entries.Select(e => new KeyValuePair<string, byte[]>(e.Key, e.Row)).ToList();

        public int HeadWaits { get; private set; }

        public int Scans { get; private set; }

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(
            string? startInclusive = null,
            string? endExclusive = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Scans++;
            foreach (var entry in _entries)
            {
                if (startInclusive is not null && string.CompareOrdinal(entry.Key, startInclusive) < 0)
                {
                    continue;
                }

                yield return entry;
            }

            await Task.CompletedTask;
        }

        public Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            HeadWaits++;
            return headWaitThrowsTimeout
                ? Task.FromException(new TimeoutException("the index view did not reach the catalog head"))
                : Task.CompletedTask;
        }

        public string ViewName => BackupConstants.CatalogIndexView;
        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<int> CountAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<long> GetLagAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task RebuildAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> ReconcileAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<ViewDigest> ComputeDigestAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
