using System.Runtime.CompilerServices;
using NSubstitute;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Retention-pass tests for <see cref="BackupSchedulerGrain"/> against
/// in-memory stores: the fence that keeps a pass from deleting a base an
/// in-flight incremental capture builds on (doomed row removed first, one sink
/// scan, the depended-on row restored, then the sink manifest and then the
/// artifacts), and the activation-local guard that runs one pass at a time
/// without wedging the scope after a fault (issue #5357).
/// </summary>
public sealed partial class BackupSchedulerGrainUnitTests
{
    private static readonly LatticeBackupScheduleOptions KeepLastOne = new()
    {
        RetentionEnabled = true,
        RetentionKeepLast = 1,
    };

    private static BackupManifest RetentionManifest(
        string id, int ageDays, BackupKind kind = BackupKind.Full, string? baseId = null) =>
        BackupManifestModelTests.Sample(id, kind, baseId) with
        {
            CreatedAtUtc = DateTimeOffset.UtcNow.AddDays(-ageDays),
            ContentDescriptors =
            [
                new BackupContentDescriptor("art-" + id, "hash", 10, 1, TestScope),
            ],
        };

    [Test]
    public async Task PruneAsync_removes_the_doomed_row_before_it_scans_the_sink_then_deletes_manifest_then_artifacts()
    {
        var log = new List<string>();
        var old = RetentionManifest("old", 2);
        var newest = RetentionManifest("newest", 0);
        var catalog = new RecordingCatalog(log, old, newest);
        var sink = RecordingSink(log, () => [old, newest]);

        var report = await CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne).PruneAsync(TestScope);

        Assert.Multiple(() =>
        {
            Assert.That(report.PrunedBackupIds, Is.EqualTo(new[] { "old" }));
            Assert.That(
                log,
                Is.EqualTo(new[] { "remove:old", "list", "delete-manifest:old", "delete-artifact:art-old" }));
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "newest" }));
        });
    }

    [Test]
    public async Task PruneAsync_keeps_and_restores_a_doomed_base_an_in_flight_increment_builds_on()
    {
        // The increment has written its sink manifest but not yet registered its
        // catalog row, so the pass's listing does not hold it and its keep set
        // dooms the base. The sink scan finds it, and the base is kept.
        var log = new List<string>();
        var baseManifest = RetentionManifest("base", 2);
        var newest = RetentionManifest("newest", 1);
        var inFlight = RetentionManifest("in-flight", 0, BackupKind.Incremental, "base");
        var catalog = new RecordingCatalog(log, baseManifest, newest);
        var sink = RecordingSink(log, () => [baseManifest, newest, inFlight]);

        var report = await CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne).PruneAsync(TestScope);

        Assert.Multiple(() =>
        {
            Assert.That(report.PrunedBackupIds, Is.Empty);
            Assert.That(log, Is.EqualTo(new[] { "remove:base", "list", "register:base" }));
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "base", "newest" }));
        });
    }

    [Test]
    public async Task PruneAsync_deletes_a_doomed_chain_tip_first()
    {
        // The listing holds the base before its increment. Deleting in listing
        // order would leave the doomed increment's sink manifest without its base
        // for as long as the pass is interrupted between the two deletes.
        var log = new List<string>();
        var b0 = RetentionManifest("b0", 3);
        var i1 = RetentionManifest("i1", 2, BackupKind.Incremental, "b0");
        var newest = RetentionManifest("newest", 0);
        var catalog = new RecordingCatalog(log, b0, i1, newest);
        var sink = RecordingSink(log, () => [b0, i1, newest]);

        var report = await CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne).PruneAsync(TestScope);

        Assert.Multiple(() =>
        {
            Assert.That(report.PrunedBackupIds, Is.EquivalentTo(new[] { "b0", "i1" }));
            Assert.That(
                log.Where(e => e.StartsWith("delete-manifest:", StringComparison.Ordinal)),
                Is.EqualTo(new[] { "delete-manifest:i1", "delete-manifest:b0" }));
        });
    }

    [Test]
    public async Task A_retention_pass_is_skipped_while_another_is_in_flight()
    {
        // The first pass is held at its first row removal, after it has listed and
        // planned. A second pass started then lists the same doomed manifest and,
        // unguarded, would remove, scan and delete it alongside the first.
        var log = new List<string>();
        var old = RetentionManifest("old", 2);
        var newest = RetentionManifest("newest", 0);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holds = 1;
        var catalog = new RecordingCatalog(log, old, newest)
        {
            OnRemove = async _ =>
            {
                if (Interlocked.Decrement(ref holds) == 0)
                {
                    entered.TrySetResult();
                    await release.Task;
                }
            },
        };
        var sink = RecordingSink(log, () => [old, newest]);
        var grain = CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne);

        var first = grain.PruneAsync(TestScope);
        await entered.Task;
        var second = await grain.PruneAsync(TestScope);
        release.SetResult();
        var firstReport = await first;

        Assert.Multiple(() =>
        {
            Assert.That(second.PrunedBackupIds, Is.Empty, "the overlapping pass must be skipped");
            Assert.That(firstReport.PrunedBackupIds, Is.EqualTo(new[] { "old" }));
            Assert.That(log.Count(e => e == "remove:old"), Is.EqualTo(1), "only one pass may remove, scan and delete");
            Assert.That(log.Count(e => e == "list"), Is.EqualTo(1), "only one pass may remove, scan and delete");
        });
    }

    [Test]
    public async Task A_retention_pass_plans_from_a_listing_taken_under_the_one_pass_guard()
    {
        // The held pass lists the catalog and is held there. A second pass then runs
        // and, if the held pass's listing was taken outside the guard, finds the
        // guard free and deletes "old" whole. An increment on "old" writes its sink
        // manifest after that pass's scan. Released, a held pass planning from its
        // stale listing would find that increment in its scan and restore the row of
        // "old", whose manifest and artifacts are already gone: the catalog would
        // list a backup that cannot be restored, and the increment's base re-check
        // would then pass against that row (issue #5357's fence, reopened).
        var log = new List<string>();
        var old = RetentionManifest("old", 2);
        var newest = RetentionManifest("newest", 0);
        var inFlight = RetentionManifest("in-flight", 0, BackupKind.Incremental, "old");
        var sinkManifests = new Dictionary<string, BackupManifest>(StringComparer.Ordinal)
        {
            [old.Id] = old,
            [newest.Id] = newest,
        };
        var listed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holds = 1;
        var catalog = new RecordingCatalog(log, old, newest)
        {
            OnListed = async () =>
            {
                if (Interlocked.Decrement(ref holds) == 0)
                {
                    listed.TrySetResult();
                    await release.Task;
                }
            },
        };
        var sink = RecordingSink(log, () => [.. sinkManifests.Values], deleteManifest: id =>
            Task.FromResult(sinkManifests.Remove(id)));
        var grain = CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne);

        var held = grain.PruneAsync(TestScope);
        await listed.Task;
        await grain.PruneAsync(TestScope);
        sinkManifests[inFlight.Id] = inFlight;
        release.SetResult();
        await held;

        foreach (var id in catalog.Ids)
        {
            Assert.That(sinkManifests, Does.ContainKey(id), $"catalog row '{id}' has no sink manifest");
        }
    }

    [Test]
    public async Task A_faulted_retention_pass_does_not_wedge_the_scope()
    {
        var log = new List<string>();
        var old = RetentionManifest("old", 2);
        var newest = RetentionManifest("newest", 0);
        var catalog = new RecordingCatalog(log, old, newest);
        var faults = 1;
        var sink = RecordingSink(log, () => [old, newest], deleteManifest: _ =>
            Interlocked.Decrement(ref faults) >= 0
                ? throw new InvalidOperationException("simulated sink fault")
                : Task.FromResult(true));
        var grain = CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne);

        await Assert.ThatAsync(async () => await grain.PruneAsync(TestScope), Throws.InvalidOperationException);

        // The faulted pass removed the row and stopped; the row is re-listed (as a
        // catalog rebuild would) and the next pass completes the prune.
        await catalog.RegisterAsync(old);
        var report = await grain.PruneAsync(TestScope);

        Assert.That(report.PrunedBackupIds, Is.EqualTo(new[] { "old" }));
    }

    private static ILatticeBackupSink RecordingSink(
        List<string> log,
        Func<BackupManifest[]> manifests,
        Func<string, Task<bool>>? deleteManifest = null)
    {
        var sink = Substitute.For<ILatticeBackupSink>();
        sink.ListManifestsAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            log.Add("list");
            return ToAsync(manifests());
        });
        sink.DeleteManifestAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(ci =>
        {
            var id = ci.ArgAt<string>(0);
            log.Add("delete-manifest:" + id);
            return deleteManifest?.Invoke(id) ?? Task.FromResult(true);
        });
        sink.DeleteArtifactAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(ci =>
        {
            log.Add("delete-artifact:" + ci.ArgAt<string>(0));
            return Task.FromResult(true);
        });
        return sink;
    }

    private static async IAsyncEnumerable<BackupManifest> ToAsync(BackupManifest[] manifests)
    {
        foreach (var manifest in manifests)
        {
            await Task.Yield();
            yield return manifest;
        }
    }

    private sealed class RecordingCatalog(List<string> log, params BackupManifest[] manifests) : ILatticeBackupCatalogStore
    {
        private readonly Dictionary<string, BackupManifest> _rows = manifests.ToDictionary(m => m.Id);

        public IReadOnlyCollection<string> Ids => _rows.Keys;

        public Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
        {
            log.Add("register:" + manifest.Id);
            _rows[manifest.Id] = manifest;
            return Task.CompletedTask;
        }

        public Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.GetValueOrDefault(backupId));

        public Func<string, Task>? OnRemove { get; init; }

        public Func<Task>? OnListed { get; init; }

        public async Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)
        {
            log.Add("remove:" + backupId);
            if (OnRemove is { } hook)
            {
                await hook(backupId);
            }

            return _rows.Remove(backupId);
        }

        public async IAsyncEnumerable<BackupManifest> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var snapshot = _rows.Values.ToList();
            if (OnListed is { } hook)
            {
                await hook();
            }

            foreach (var manifest in snapshot)
            {
                await Task.Yield();
                yield return manifest;
            }
        }
    }
}
