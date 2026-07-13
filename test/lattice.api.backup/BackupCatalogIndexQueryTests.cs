using System.Runtime.CompilerServices;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="BackupCatalogIndexQuery"/>: the filtered,
/// newest-first, set-aware, paged catalog listing. The deterministic full-scan
/// path (no index view) drives the ordering, filtering, grouping, paging and
/// authorization cases; a compact fake index view drives the liveness drop that
/// only the view path can exhibit.
/// </summary>
[TestFixture]
public sealed class BackupCatalogIndexQueryTests
{
    private static readonly Func<BackupScopeSelector, CancellationToken, ValueTask<bool>> AllowAll =
        (_, _) => new ValueTask<bool>(true);

    private static BackupManifest Manifest(
        string id,
        DateTimeOffset createdAtUtc,
        string tree = "orders",
        string? name = null,
        BackupKind kind = BackupKind.Full,
        string? setId = null,
        string? setName = null,
        DateTimeOffset? setCreatedAtUtc = null,
        string? baseBackupId = null)
    {
        var manifest = new BackupManifest(
            id: id,
            name: name ?? id,
            createdAtUtc: createdAtUtc,
            kind: kind,
            scope: BackupScopeSelector.WholeTree(tree),
            consistencyCut: new BackupConsistencyCut(1, 1),
            topology: new BackupTopologySnapshot(1, 4096, new[] { "d0" }),
            structuralDigest: "digest",
            keyDescriptors: Array.Empty<BackupKeyDescriptor>(),
            contentDescriptors: Array.Empty<BackupContentDescriptor>(),
            provenance: Array.Empty<BackupOriginProvenance>(),
            baseBackupId: baseBackupId);

        return manifest with { SetId = setId, SetName = setName, SetCreatedAtUtc = setCreatedAtUtc };
    }

    private static async Task<BackupCatalogPage> QueryAsync(
        FakeCatalog catalog,
        BackupCatalogRequest request,
        int pageSize = 10,
        Func<BackupScopeSelector, CancellationToken, ValueTask<bool>>? auth = null,
        ILatticeViewFactory? viewFactory = null)
    {
        var query = new BackupCatalogIndexQuery(catalog, viewFactory);
        return await query.QueryAsync(request, pageSize, auth ?? AllowAll, CancellationToken.None);
    }

    private static BackupCatalogRequest Request(
        int pageSize = 0,
        string? token = null,
        BackupKind? kind = null,
        string? namePrefix = null,
        string? treeId = null,
        string? createdPrefix = null) => new()
    {
        PageSize = pageSize,
        PageToken = token,
        OrderByCreatedDescending = true,
        Kind = kind,
        NamePrefix = namePrefix,
        TreeId = treeId,
        CreatedPrefix = createdPrefix,
    };

    [Test]
    public async Task Returns_backups_newest_first()
    {
        var catalog = new FakeCatalog(
            Manifest("a", DateTimeOffset.UnixEpoch.AddHours(1)),
            Manifest("b", DateTimeOffset.UnixEpoch.AddHours(3)),
            Manifest("c", DateTimeOffset.UnixEpoch.AddHours(2)));

        var page = await QueryAsync(catalog, Request());

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "b", "c", "a" }));
    }

    [Test]
    public async Task Set_members_stay_adjacent_and_are_ordered_by_set_time()
    {
        var setCreated = DateTimeOffset.UnixEpoch.AddHours(2);
        var catalog = new FakeCatalog(
            Manifest("standalone", DateTimeOffset.UnixEpoch.AddHours(5)),
            Manifest("m1", DateTimeOffset.UnixEpoch.AddHours(2), tree: "orders", setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated),
            Manifest("m2", DateTimeOffset.UnixEpoch.AddHours(9), tree: "customers", setId: "set-1", setName: "nightly", setCreatedAtUtc: setCreated));

        var page = await QueryAsync(catalog, Request());

        // The standalone (created at +5h) sorts before the set (set time +2h), and
        // the two members come back together.
        var ids = page.Entries.Select(e => e.Id).ToList();
        Assert.That(ids[0], Is.EqualTo("standalone"));
        Assert.That(ids.Skip(1), Is.EquivalentTo(new[] { "m1", "m2" }));
    }

    [Test]
    public async Task Kind_filter_matches_only_the_requested_kind()
    {
        var catalog = new FakeCatalog(
            Manifest("full", DateTimeOffset.UnixEpoch.AddHours(1)),
            Manifest("inc", DateTimeOffset.UnixEpoch.AddHours(2), kind: BackupKind.Incremental, baseBackupId: "full"));

        var page = await QueryAsync(catalog, Request(kind: BackupKind.Incremental));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "inc" }));
    }

    [Test]
    public async Task Name_prefix_filter_is_case_insensitive_starts_with()
    {
        var catalog = new FakeCatalog(
            Manifest("a", DateTimeOffset.UnixEpoch.AddHours(1), name: "Nightly-EU"),
            Manifest("b", DateTimeOffset.UnixEpoch.AddHours(2), name: "adhoc"));

        var page = await QueryAsync(catalog, Request(namePrefix: "night"));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task Scope_filter_matches_the_tree()
    {
        var catalog = new FakeCatalog(
            Manifest("a", DateTimeOffset.UnixEpoch.AddHours(1), tree: "orders"),
            Manifest("b", DateTimeOffset.UnixEpoch.AddHours(2), tree: "customers"));

        var page = await QueryAsync(catalog, Request(treeId: "customers"));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "b" }));
    }

    [Test]
    public async Task Created_prefix_filter_matches_the_rendered_timestamp()
    {
        var jan = new DateTimeOffset(2024, 1, 5, 6, 0, 0, TimeSpan.Zero);
        var feb = new DateTimeOffset(2024, 2, 5, 6, 0, 0, TimeSpan.Zero);
        var catalog = new FakeCatalog(
            Manifest("jan", jan),
            Manifest("feb", feb));

        var page = await QueryAsync(catalog, Request(createdPrefix: "2024-02"));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "feb" }));
    }

    [Test]
    public async Task Paging_returns_a_cursor_and_resumes_without_overlap()
    {
        var catalog = new FakeCatalog(
            Enumerable.Range(0, 5)
                .Select(i => Manifest($"b{i}", DateTimeOffset.UnixEpoch.AddHours(i)))
                .ToArray());

        var first = await QueryAsync(catalog, Request(), pageSize: 2);
        Assert.That(first.Entries.Select(e => e.Id), Is.EqualTo(new[] { "b4", "b3" }));
        Assert.That(first.NextPageToken, Is.Not.Null);

        var second = await QueryAsync(catalog, Request(token: first.NextPageToken), pageSize: 2);
        Assert.That(second.Entries.Select(e => e.Id), Is.EqualTo(new[] { "b2", "b1" }));
        Assert.That(second.NextPageToken, Is.Not.Null);

        var third = await QueryAsync(catalog, Request(token: second.NextPageToken), pageSize: 2);
        Assert.That(third.Entries.Select(e => e.Id), Is.EqualTo(new[] { "b0" }));
        Assert.That(third.NextPageToken, Is.Null, "the final page carries no continuation");
    }

    [Test]
    public async Task Unauthorized_scopes_are_hidden()
    {
        var catalog = new FakeCatalog(
            Manifest("a", DateTimeOffset.UnixEpoch.AddHours(1), tree: "orders"),
            Manifest("b", DateTimeOffset.UnixEpoch.AddHours(2), tree: "secret"));

        var page = await QueryAsync(
            catalog,
            Request(),
            auth: (scope, _) => new ValueTask<bool>(scope.TreeId != "secret"));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task A_stale_index_row_whose_backup_was_deleted_is_dropped()
    {
        // The index still carries "ghost" but the authoritative catalog no longer
        // holds it; the liveness read must drop it so no phantom backup surfaces.
        var live = Manifest("live", DateTimeOffset.UnixEpoch.AddHours(1));
        var ghost = Manifest("ghost", DateTimeOffset.UnixEpoch.AddHours(2));

        var catalog = new FakeCatalog(live); // ghost absent from the catalog
        var view = new FakeIndexView(live, ghost); // but present in the index

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "live" }));
    }

    [Test]
    public async Task Duplicate_index_rows_for_one_backup_are_listed_once()
    {
        // An older index generation could leave two rows for the same content-
        // addressed backup (a re-capture that re-keyed by capture time before the
        // registration became idempotent). Both rows resolve to the one live
        // manifest; the query must surface the backup exactly once, not once per
        // orphaned index row.
        var older = Manifest("dup", DateTimeOffset.UnixEpoch.AddHours(1));
        var newer = Manifest("dup", DateTimeOffset.UnixEpoch.AddHours(5));

        var catalog = new FakeCatalog(newer); // one live manifest per id
        var view = new FakeIndexView(older, newer); // but two index rows for it

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "dup" }));
    }

    [Test]
    public async Task Incremental_chain_collapses_to_its_tip_full_scan()
    {
        // full "base" <- inc "i1" <- inc "i2" (the tip). Only the tip is listed;
        // the base and the mid-chain increment are folded behind it.
        var catalog = new FakeCatalog(
            Manifest("base", DateTimeOffset.UnixEpoch.AddHours(1)),
            Manifest("i1", DateTimeOffset.UnixEpoch.AddHours(2), kind: BackupKind.Incremental, baseBackupId: "base"),
            Manifest("i2", DateTimeOffset.UnixEpoch.AddHours(3), kind: BackupKind.Incremental, baseBackupId: "i1"));

        var page = await QueryAsync(catalog, Request());

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "i2" }));
    }

    [Test]
    public async Task Incremental_chain_collapses_to_its_tip_via_index()
    {
        var baseFull = Manifest("base", DateTimeOffset.UnixEpoch.AddHours(1));
        var i1 = Manifest("i1", DateTimeOffset.UnixEpoch.AddHours(2), kind: BackupKind.Incremental, baseBackupId: "base");
        var i2 = Manifest("i2", DateTimeOffset.UnixEpoch.AddHours(3), kind: BackupKind.Incremental, baseBackupId: "i1");

        var catalog = new FakeCatalog(baseFull, i1, i2);
        var view = new FakeIndexView(baseFull, i1, i2);

        var page = await QueryAsync(catalog, Request(), viewFactory: new FakeViewFactory(view));

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "i2" }));
    }

    [Test]
    public async Task A_full_backup_with_no_increments_is_still_listed()
    {
        var catalog = new FakeCatalog(
            Manifest("lonely-full", DateTimeOffset.UnixEpoch.AddHours(1)));

        var page = await QueryAsync(catalog, Request());

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "lonely-full" }));
    }

    [Test]
    public async Task Chain_tip_lists_alongside_unrelated_backups_newest_first()
    {
        // A standalone full, plus a chain base<-inc. The listing shows the
        // standalone and the chain tip only, newest-first, and never the folded
        // ancestors even though they carry distinct capture times.
        var catalog = new FakeCatalog(
            Manifest("standalone", DateTimeOffset.UnixEpoch.AddHours(10)),
            Manifest("base", DateTimeOffset.UnixEpoch.AddHours(1)),
            Manifest("tip", DateTimeOffset.UnixEpoch.AddHours(5), kind: BackupKind.Incremental, baseBackupId: "base"));

        var page = await QueryAsync(catalog, Request());

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { "standalone", "tip" }));
    }

    private sealed class FakeCatalog(params BackupManifest[] manifests) : ILatticeBackupCatalogStore
    {
        private readonly List<BackupManifest> _manifests = manifests.ToList();

        public Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_manifests.FirstOrDefault(m => m.Id == backupId));

        public Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<BackupManifest> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var manifest in _manifests.OrderBy(m => m.Id, StringComparer.Ordinal))
            {
                yield return manifest;
            }

            await Task.CompletedTask;
        }
    }

    // A minimal read-only index view that streams the encoded index entries for a
    // set of manifests in key order. Only the members the query uses are real.
    private sealed class FakeIndexView : ILatticeView
    {
        private readonly List<KeyValuePair<string, byte[]>> _entries;

        public FakeIndexView(params BackupManifest[] manifests)
        {
            _entries = manifests
                .Select(m => new KeyValuePair<string, byte[]>(
                    BackupCatalogIndexKey.Encode(m),
                    JsonLatticeSerializer<BackupCatalogIndexRow>.Default.Serialize(new BackupCatalogIndexRow
                    {
                        BackupId = m.Id,
                        Name = m.Name,
                        Kind = m.Kind,
                        TreeId = m.Scope.TreeId,
                        CreatedAtUtc = m.CreatedAtUtc,
                        SetId = m.SetId,
                        SetName = m.SetName,
                        BaseBackupId = m.BaseBackupId,
                    })))
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .ToList();
        }

        public async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(
            string? startInclusive = null,
            string? endExclusive = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
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

        public Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

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

    private sealed class FakeViewFactory(ILatticeView view) : ILatticeViewFactory
    {
        public ILatticeView Create(ILattice source, string viewName, LatticeViewDefinition definition) => view;
        public Task<ILatticeView?> GetAsync(string viewName, CancellationToken cancellationToken = default) => Task.FromResult<ILatticeView?>(view);
        public Task DeleteAsync(string viewName, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
