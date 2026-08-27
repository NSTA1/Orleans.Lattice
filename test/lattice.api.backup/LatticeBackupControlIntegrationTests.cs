using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// End-to-end coverage for the <see cref="ILatticeBackupControl"/> facade over a
/// live single-silo cluster: it drives a full backup, an incremental, a
/// cursor-resumable listing, a describe-chain, a safe delete, and a restore that
/// reproduces the captured values; and it fails closed - writing nothing - when
/// the backup authorization gate denies the caller.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlIntegrationTests
{
    private const string Source = "orders";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new ApiBackupClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Full end-to-end drive ------------------------------------------

    [Test]
    public async Task Facade_drives_full_incremental_list_and_restore_end_to_end()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var full = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        await source.SetAsync("k3", Bytes("v3"));
        var incremental = await _fixture.Control.CreateIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "incr", BackupScopeSelector.WholeTree(Source), full.BackupId));

        var page = await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest());
        var listedIds = page.Entries.Select(e => e.Id).ToList();

        const string target = "orders-restored";
        var result = await _fixture.Control.RestoreBackupAsync(
            new LatticeRestoreRequest(incremental.BackupId, target));

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);

        Assert.Multiple(() =>
        {
            Assert.That(listedIds, Does.Contain(full.BackupId));
            Assert.That(listedIds, Does.Contain(incremental.BackupId));
            Assert.That(result.TargetTreeId, Is.EqualTo(target));
        });

        Assert.Multiple(() =>
        {
            Assert.That(Str(restored.GetAsync("k1").Result!), Is.EqualTo("v1"));
            Assert.That(Str(restored.GetAsync("k2").Result!), Is.EqualTo("v2"));
            Assert.That(Str(restored.GetAsync("k3").Result!), Is.EqualTo("v3"));
        });
    }

    // ---- Permission fail-closed -----------------------------------------

    [Test]
    public async Task CreateBackupAsync_denied_permission_fails_closed_and_writes_nothing()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no backup grant"), membership: null));

        Assert.That(
            async () => await denying.CreateBackupAsync(
                new LatticeBackupCaptureRequest("denied", BackupScopeSelector.WholeTree(Source))),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        // Fail-closed: nothing was registered in the catalog.
        Assert.That(await CountCatalogAsync(), Is.Zero);
    }

    [Test]
    public async Task RestoreBackupAsync_denied_permission_fails_closed_and_writes_nothing()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        const string target = "orders-denied";
        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no restore grant"), membership: null));

        Assert.That(
            async () => await denying.RestoreBackupAsync(new LatticeRestoreRequest(backup.BackupId, target)),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());

        var restored = _fixture.GrainFactory.GetGrain<ILattice>(target);
        Assert.That(restored.GetAsync("k1").Result, Is.Null);
    }

    // ---- Cursor-resumable listing ---------------------------------------

    [Test]
    public async Task ListBackupsAsync_is_cursor_resumable_and_visits_every_backup_once()
    {
        await _fixture.InitializeAsync();

        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 5; i++)
        {
            var treeId = $"tree-{i}";
            var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
            await tree.SetAsync("k", Bytes($"v{i}"));
            var backup = await _fixture.Control.CreateBackupAsync(
                new LatticeBackupCaptureRequest($"b{i}", BackupScopeSelector.WholeTree(treeId)));
            expected.Add(backup.BackupId);
        }

        var seen = new List<string>();
        string? token = null;
        do
        {
            var page = await _fixture.Control.ListBackupsAsync(
                new BackupCatalogRequest { PageSize = 2, PageToken = token });
            Assert.That(page.Entries, Has.Count.LessThanOrEqualTo(2));
            seen.AddRange(page.Entries.Select(e => e.Id));
            token = page.NextPageToken;
        }
        while (token is not null);

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.Unique);
            Assert.That(new HashSet<string>(seen, StringComparer.Ordinal), Is.EquivalentTo(expected));
        });
    }

    [Test]
    public async Task StreamBackupsAsync_yields_every_backup_in_id_order()
    {
        await _fixture.InitializeAsync();

        var expected = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var treeId = $"stream-{i}";
            var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
            await tree.SetAsync("k", Bytes($"v{i}"));
            var backup = await _fixture.Control.CreateBackupAsync(
                new LatticeBackupCaptureRequest($"s{i}", BackupScopeSelector.WholeTree(treeId)));
            expected.Add(backup.BackupId);
        }

        var streamed = new List<string>();
        await foreach (var manifest in _fixture.Control.StreamBackupsAsync())
        {
            streamed.Add(manifest.Id);
        }

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Is.EquivalentTo(expected));
            Assert.That(streamed, Is.Ordered.Using((IComparer<string>)StringComparer.Ordinal));
        });
    }

    // ---- Describe chain -------------------------------------------------

    [Test]
    public async Task DescribeBackupAsync_returns_the_base_first_chain_for_a_base_and_increment()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var full = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        await source.SetAsync("k2", Bytes("v2"));
        var incremental = await _fixture.Control.CreateIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "incr", BackupScopeSelector.WholeTree(Source), full.BackupId));

        var description = await _fixture.Control.DescribeBackupAsync(incremental.BackupId);

        Assert.That(description, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(description!.Manifest.Id, Is.EqualTo(incremental.BackupId));
            Assert.That(description.ChainBackupIds, Is.EqualTo(new[] { full.BackupId, incremental.BackupId }));
        });
    }

    [Test]
    public async Task DescribeBackupAsync_returns_null_for_an_unknown_backup()
    {
        await _fixture.InitializeAsync();

        var description = await _fixture.Control.DescribeBackupAsync(
            "0000000000000000000000000000000000000000000000000000000000000000");

        Assert.That(description, Is.Null);
    }

    // ---- Delete ---------------------------------------------------------

    [Test]
    public async Task DeleteBackupAsync_removes_a_backup_from_the_listing()
    {
        await _fixture.InitializeAsync();

        var treeA = _fixture.GrainFactory.GetGrain<ILattice>("tree-a");
        await treeA.SetAsync("k", Bytes("a"));
        var backupA = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("a", BackupScopeSelector.WholeTree("tree-a")));

        var treeB = _fixture.GrainFactory.GetGrain<ILattice>("tree-b");
        await treeB.SetAsync("k", Bytes("b"));
        var backupB = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("b", BackupScopeSelector.WholeTree("tree-b")));

        var deleted = await _fixture.Control.DeleteBackupAsync(backupA.BackupId);
        var deletedAgain = await _fixture.Control.DeleteBackupAsync(backupA.BackupId);

        var page = await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest());
        var listedIds = page.Entries.Select(e => e.Id).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(deletedAgain, Is.False);
            Assert.That(listedIds, Does.Not.Contain(backupA.BackupId));
            Assert.That(listedIds, Does.Contain(backupB.BackupId));
        });
    }

    // ---- Artifact export ------------------------------------------------

    [Test]
    public async Task ExportArtifactAsync_streams_an_owned_artifact_chunk_wise()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        var artifactId = backup.Manifest.ContentDescriptors.Single().ArtifactId;
        long exportedBytes = 0;
        await foreach (var chunk in _fixture.Control.ExportArtifactAsync(backup.BackupId, artifactId))
        {
            exportedBytes += chunk.Length;
        }

        Assert.That(exportedBytes, Is.GreaterThan(0));
    }

    [Test]
    public async Task ExportArtifactAsync_throws_for_an_artifact_the_backup_does_not_own()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var backup = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        Assert.That(
            async () =>
            {
                await foreach (var _ in _fixture.Control.ExportArtifactAsync(backup.BackupId, "not-an-artifact"))
                {
                }
            },
            Throws.InstanceOf<KeyNotFoundException>());
    }

    // ---- Argument guards ------------------------------------------------

    [Test]
    public async Task CreateBackupAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.CreateBackupAsync(null!),
            Throws.ArgumentNullException);
    }

    // ---- Backup-set capture ---------------------------------------------

    [Test]
    public async Task CreateBackupSetAsync_captures_a_member_per_tree_under_one_set_manifest()
    {
        await _fixture.InitializeAsync();
        var treeA = _fixture.GrainFactory.GetGrain<ILattice>("set-tree-a");
        await treeA.SetAsync("k", Bytes("a"));
        var treeB = _fixture.GrainFactory.GetGrain<ILattice>("set-tree-b");
        await treeB.SetAsync("k", Bytes("b"));

        var set = await _fixture.Control.CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest(
                "nightly-set",
                new[]
                {
                    BackupScopeSelector.WholeTree("set-tree-a"),
                    BackupScopeSelector.WholeTree("set-tree-b"),
                },
                crossTreeConsistent: true));

        var page = await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest());
        var listedIds = page.Entries.Select(e => e.Id).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(set.Members, Has.Count.EqualTo(2));
            Assert.That(set.SetManifest.MemberBackupIds, Has.Count.EqualTo(2));
            Assert.That(set.SetManifest.CrossTreeConsistent, Is.True);
            foreach (var member in set.Members)
            {
                Assert.That(listedIds, Does.Contain(member.BackupId));
            }
        });
    }

    [Test]
    public async Task CreateBackupSetAsync_set_id_matches_what_ListBackupsAsync_reports_for_every_member()
    {
        await _fixture.InitializeAsync();
        await _fixture.GrainFactory.GetGrain<ILattice>("row-solo").SetAsync("k", Bytes("s"));
        await _fixture.GrainFactory.GetGrain<ILattice>("row-a").SetAsync("k", Bytes("a"));
        await _fixture.GrainFactory.GetGrain<ILattice>("row-b").SetAsync("k", Bytes("b"));

        var solo = await _fixture.Control.CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest(
                "row-solo-set", new[] { BackupScopeSelector.WholeTree("row-solo") }));
        var pair = await _fixture.Control.CreateBackupSetAsync(
            new LatticeBackupSetCaptureRequest(
                "row-pair-set",
                new[]
                {
                    BackupScopeSelector.WholeTree("row-a"),
                    BackupScopeSelector.WholeTree("row-b"),
                }));

        var rows = (await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest()))
            .Entries.ToDictionary(e => e.Id, e => e.SetId);

        // The whole point of the fix: the create response and the catalog rows a
        // remote consumer groups by must agree. A one-scope capture reports no set
        // id and its row carries none; a multi-scope capture reports an id every
        // member row carries. Grouping by a reported id can therefore never come
        // back empty.
        Assert.Multiple(() =>
        {
            Assert.That(solo.SetManifest.SetId, Is.Null,
                "a one-scope capture must not report a set id no catalog row carries");
            Assert.That(rows[solo.Members[0].BackupId], Is.EqualTo(solo.SetManifest.SetId));

            Assert.That(pair.SetManifest.SetId, Is.Not.Null);
            foreach (var member in pair.Members)
            {
                Assert.That(rows[member.BackupId], Is.EqualTo(pair.SetManifest.SetId));
            }
        });

        var grouped = (await _fixture.Control.ListBackupsAsync(new BackupCatalogRequest()))
            .Entries.Where(e => e.SetId is not null && e.SetId == pair.SetManifest.SetId)
            .Select(e => e.Id)
            .ToList();
        Assert.That(grouped, Is.EquivalentTo(pair.Members.Select(m => m.BackupId)),
            "grouping catalog rows by a reported set id resolves exactly the set's members");
    }

    [Test]
    public async Task CreateBackupSetAsync_denied_permission_fails_closed()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no backup grant"), membership: null));

        Assert.That(
            async () => await denying.CreateBackupSetAsync(
                new LatticeBackupSetCaptureRequest(
                    "denied-set",
                    new[] { BackupScopeSelector.WholeTree(Source) })),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task CreateBackupSetAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.CreateBackupSetAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ListBackupsAsync_null_request_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.ListBackupsAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task DescribeBackupAsync_empty_id_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.DescribeBackupAsync(string.Empty),
            Throws.ArgumentException);
    }

    // ---- Helpers --------------------------------------------------------

    private async Task<int> CountCatalogAsync()
    {
        var count = 0;
        await foreach (var _ in _fixture.Catalog.ListAsync())
        {
            count++;
        }

        return count;
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] b) => Encoding.UTF8.GetString(b);

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
