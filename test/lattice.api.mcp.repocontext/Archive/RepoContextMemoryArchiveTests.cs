using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Archive;

/// <summary>
/// End-to-end tests for the durable-memory archive (issue #2601, acceptance criteria
/// 1 and 2): export the memory tree to a directory outside the store, then restore it
/// into a store that came up empty - the state a destroyed data volume leaves behind -
/// and confirm the records are back.
/// <para>
/// The hardening tests matter as much as the round trip. An archive is a recovery
/// mechanism, so a failure mode that damages the archive is a second way to lose the
/// data it was added to protect. The two guarded against here are an export
/// interrupted part-way through its write, and an export that runs against a store
/// that has come up empty and would otherwise overwrite a good archive with nothing.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextMemoryArchiveTests
{
    private string archiveDirectory = string.Empty;

    private static string MemoryTreeName => $"repocontext-archive-{Guid.NewGuid():N}";

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    [SetUp]
    public void CreateArchiveDirectory()
    {
        archiveDirectory = Path.Combine(
            Path.GetTempPath(), "lattice-memory-archive-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(archiveDirectory);
    }

    [TearDown]
    public void RemoveArchiveDirectory()
    {
        try
        {
            if (Directory.Exists(archiveDirectory))
            {
                Directory.Delete(archiveDirectory, recursive: true);
            }
        }
        catch (IOException)
        {
        }
    }

    private RepoContextMemoryArchiveOptions Options(
        RepoContextMemoryArchiveRestoreMode restoreMode = RepoContextMemoryArchiveRestoreMode.Auto)
        => new() { Directory = archiveDirectory, RestoreMode = restoreMode };

    private string SnapshotPath => Path.Combine(
        archiveDirectory, RepoContextMemoryArchive.SnapshotFileName);

    private string PreviousSnapshotPath => Path.Combine(
        archiveDirectory, RepoContextMemoryArchive.PreviousSnapshotFileName);

    /// <summary>
    /// Seeds memory through <see cref="RepoContextMemoryCodec.Accessor"/> - the path
    /// production writes through - rather than a raw <c>SetAsync</c>.
    /// <para>
    /// This is load-bearing and it is why issue #2641's sibling defect stayed hidden.
    /// A raw <c>SetAsync</c> stores the record un-enveloped; the capture path stores it
    /// inside an <see cref="MvRegister"/> blob. A round-trip suite that seeds raw
    /// therefore exercises a route production never takes, and certifies it while
    /// reporting coverage of the one it does. Every export, import, and read below has
    /// to see the shape a real store holds or it is testing a different system.
    /// </para>
    /// </summary>
    private static async Task SeedMemoryAsync(
        ILattice tree, Serializer serializer, string repoId, params string[] ids)
    {
        foreach (var id in ids)
        {
            var record = new MemoryRecord
            {
                RepoId = repoId,
                Topic = "gotchas",
                Id = id,
                Kind = MemoryKind.Note,
                Title = RepoContextValues.Lww($"title-{id}", Clock(1_000)),
                Body = RepoContextValues.Lww($"body-{id}", Clock(1_000)),
            };

            await RepoContextMemoryCodec
                .Accessor(tree, RepoContextKeys.Memory(repoId, "gotchas", id))
                .SetAsync("local", serializer.SerializeToArray(record));
        }
    }

    /// <summary>
    /// Reads memory back through the supported decoder, which unwraps the register the
    /// capture path writes. Deserializing the stored bytes directly decodes the leading
    /// ASCII of the JSON envelope as a type reference and fails identically for every
    /// record, whether the archive is perfect or shredded.
    /// </summary>
    private static async Task<MemoryRecord?> ReadMemoryAsync(
        ILattice tree, Serializer serializer, string repoId, string id)
    {
        var bytes = await tree.GetAsync(RepoContextKeys.Memory(repoId, "gotchas", id));
        return RepoContextMemoryCodec.Fold(bytes, serializer);
    }

    [Test]
    public async Task Memory_exported_from_one_store_is_restored_into_an_empty_one()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var archive = new RepoContextMemoryArchive(Options());

        var live = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(live, serializer, "acme", "g1", "g2", "g3");

        var export = await archive.ExportAsync(
            live, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(export.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written));
            Assert.That(export.RecordCount, Is.EqualTo(3));
            Assert.That(File.Exists(SnapshotPath), Is.True);
        });

        // A fresh tree stands in for the store a destroyed data volume leaves behind:
        // same container, same archive on the host path, no memory at all.
        var empty = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        var restored = await archive.RestoreAsync(
            empty, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restored.Restored, Is.True);
            Assert.That(restored.RecordsRead, Is.EqualTo(3));
            Assert.That(restored.SourcePath, Is.EqualTo(SnapshotPath));
        });

        var record = await ReadMemoryAsync(empty, serializer, "acme", "g2");
        Assert.That(record, Is.Not.Null);
        Assert.That(RepoContextValues.ReadString(record!.Body), Is.EqualTo("body-g2"));
    }

    [Test]
    public async Task Auto_restore_declines_when_the_store_already_holds_memory()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var archive = new RepoContextMemoryArchive(Options());

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "g1");
        await archive.ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);

        var occupied = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(occupied, serializer, "other", "x1");

        var restore = await archive.RestoreAsync(
            occupied, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restore.Restored, Is.False);
            Assert.That(restore.Outcome, Is.EqualTo(RepoContextMemoryRestoreOutcome.NothingToRestore));
            Assert.That(restore.Reason, Does.Contain("already holds"));

            // The decline must say WHY the store was left alone, not merely that it
            // was. A store can be non-empty because it is in use or because a previous
            // restore died partway, and only the second is wreckage worth overwriting
            // (issue #2641) - so the reason has to name the marker it consulted.
            Assert.That(restore.Reason, Does.Contain("restore-state marker"));
            Assert.That(
                restore.RecordsInStore, Is.EqualTo(1),
                "The decline must report what the store actually holds rather than zero.");
        });
    }

    [Test]
    public async Task Always_restore_merges_into_a_store_that_already_holds_memory()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var archive = new RepoContextMemoryArchive(
            Options(RepoContextMemoryArchiveRestoreMode.Always));

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "archived");
        await archive.ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);

        var target = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName + "-target");
        await SeedMemoryAsync(target, serializer, "acme", "live");

        var restore = await archive.RestoreAsync(
            target, serializer, TestContext.CurrentContext.CancellationToken);

        var archived = await ReadMemoryAsync(target, serializer, "acme", "archived");
        var stillLive = await ReadMemoryAsync(target, serializer, "acme", "live");

        Assert.Multiple(() =>
        {
            Assert.That(restore.Restored, Is.True);
            Assert.That(archived, Is.Not.Null, "the archived record arrived");
            Assert.That(
                stillLive,
                Is.Not.Null,
                "and the live record was not displaced - the import is a CRDT join, not an overwrite");
        });
    }

    [Test]
    public async Task Restore_is_skipped_entirely_when_the_mode_is_off()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "g1");
        await new RepoContextMemoryArchive(Options())
            .ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);

        var offArchive = new RepoContextMemoryArchive(Options(RepoContextMemoryArchiveRestoreMode.Off));
        var empty = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName + "-empty");

        var restore = await offArchive.RestoreAsync(
            empty, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restore.Restored, Is.False);
            Assert.That(restore.Reason, Does.Contain("Off").IgnoreCase);
        });
    }

    [Test]
    public async Task Restoring_with_no_archive_on_disk_reports_rather_than_throws()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();

        var restore = await new RepoContextMemoryArchive(Options()).RestoreAsync(
            harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName),
            serializer,
            TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restore.Restored, Is.False);
            Assert.That(restore.Reason, Does.Contain("no archived snapshot"));
        });
    }

    /// <summary>
    /// The hardening test that matters most. An export killed part-way through its
    /// write - by a shutdown budget running out, by SIGKILL, by a full disk - must not
    /// be able to damage the archive it is replacing, because the archive is the only
    /// remaining copy of state that rebuilds from nothing.
    /// </summary>
    [Test]
    public async Task An_export_interrupted_mid_write_leaves_the_previous_archive_importable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "g1", "g2", "g3");

        var good = new RepoContextMemoryArchive(Options());
        var first = await good.ExportAsync(
            source, serializer, TestContext.CurrentContext.CancellationToken);
        Assert.That(first.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written));

        var goodBytes = await File.ReadAllBytesAsync(
            SnapshotPath, TestContext.CurrentContext.CancellationToken);

        // Now export again through a stream that dies a few bytes in.
        var breaking = new RepoContextMemoryArchive(Options())
        {
            OpenForWrite = path => new FailingStream(
                new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None),
                failAfterBytes: 16),
        };

        await SeedMemoryAsync(source, serializer, "acme", "g4");
        var interrupted = await breaking.ExportAsync(
            source, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.That(
            interrupted.Outcome,
            Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Incomplete),
            "the interruption is reported, not swallowed");

        Assert.Multiple(() =>
        {
            Assert.That(File.Exists(SnapshotPath), Is.True, "the archive still exists");
            Assert.That(
                File.ReadAllBytes(SnapshotPath),
                Is.EqualTo(goodBytes),
                "and is byte-for-byte the one that was there before - the write was never in place");
            Assert.That(
                Directory.GetFiles(archiveDirectory, "*" + RepoContextMemoryArchive.TemporaryExtension),
                Is.Empty,
                "the partial file was cleaned up rather than left to be mistaken for a snapshot");
        });

        // The surviving archive is not merely present, it still imports.
        var empty = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName + "-recovered");
        var restored = await good.RestoreAsync(
            empty, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restored.Restored, Is.True);
            Assert.That(restored.RecordsRead, Is.EqualTo(3));
        });
        Assert.That(await ReadMemoryAsync(empty, serializer, "acme", "g2"), Is.Not.Null);
    }

    [Test]
    public async Task An_export_rotates_the_prior_generation_rather_than_discarding_it()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var archive = new RepoContextMemoryArchive(Options());

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "g1");
        await archive.ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);
        var firstGeneration = await File.ReadAllBytesAsync(
            SnapshotPath, TestContext.CurrentContext.CancellationToken);

        await SeedMemoryAsync(source, serializer, "acme", "g2");
        var second = await archive.ExportAsync(
            source, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(second.RecordCount, Is.EqualTo(2));
            Assert.That(File.Exists(PreviousSnapshotPath), Is.True);
            Assert.That(File.ReadAllBytes(PreviousSnapshotPath), Is.EqualTo(firstGeneration));
        });
    }

    [Test]
    public async Task Restore_falls_back_to_the_previous_generation_when_the_current_one_is_corrupt()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var archive = new RepoContextMemoryArchive(Options());

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "g1");
        await archive.ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);
        await SeedMemoryAsync(source, serializer, "acme", "g2");
        await archive.ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);

        // Damage the current snapshot the way a torn write or a truncated copy would.
        await File.WriteAllBytesAsync(
            SnapshotPath, [1, 2, 3, 4, 5, 6, 7, 8, 9], TestContext.CurrentContext.CancellationToken);

        var empty = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName + "-empty");
        var restore = await archive.RestoreAsync(
            empty, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restore.Restored, Is.True);
            Assert.That(restore.SourcePath, Is.EqualTo(PreviousSnapshotPath));
            Assert.That(restore.RecordsRead, Is.EqualTo(1));
        });
    }

    /// <summary>
    /// The sequence this whole feature exists for is a wipe followed by a restart, and
    /// in that sequence the store is empty while the archive is good. An export that
    /// ran first and wrote its empty view over the archive would complete the loss
    /// rather than prevent it.
    /// </summary>
    [Test]
    public async Task An_empty_export_is_refused_rather_than_written_over_a_non_empty_archive()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var archive = new RepoContextMemoryArchive(Options());

        var source = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName);
        await SeedMemoryAsync(source, serializer, "acme", "g1", "g2");
        await archive.ExportAsync(source, serializer, TestContext.CurrentContext.CancellationToken);
        var goodBytes = await File.ReadAllBytesAsync(
            SnapshotPath, TestContext.CurrentContext.CancellationToken);

        var wiped = harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName + "-wiped");
        var refused = await archive.ExportAsync(
            wiped, serializer, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(
                refused.Outcome,
                Is.EqualTo(RepoContextMemoryArchiveExportOutcome.RefusedEmptyOverNonEmpty));
            Assert.That(refused.Reason, Does.Contain("left intact"));
            Assert.That(File.ReadAllBytes(SnapshotPath), Is.EqualTo(goodBytes));
            Assert.That(
                Directory.GetFiles(archiveDirectory, "*" + RepoContextMemoryArchive.TemporaryExtension),
                Is.Empty);
        });
    }

    [Test]
    public async Task An_empty_export_is_written_when_there_is_nothing_to_protect()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();

        var export = await new RepoContextMemoryArchive(Options()).ExportAsync(
            harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName),
            serializer,
            TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(export.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written));
            Assert.That(export.RecordCount, Is.Zero);
            Assert.That(File.Exists(SnapshotPath), Is.True);
        });
    }

    [Test]
    public async Task The_archive_directory_is_created_when_it_does_not_exist()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();

        Directory.Delete(archiveDirectory, recursive: true);
        Assert.That(Directory.Exists(archiveDirectory), Is.False);

        var export = await new RepoContextMemoryArchive(Options()).ExportAsync(
            harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName),
            serializer,
            TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(export.Outcome, Is.EqualTo(RepoContextMemoryArchiveExportOutcome.Written));
            Assert.That(File.Exists(SnapshotPath), Is.True);
        });
    }

    [Test]
    public async Task Stale_temporaries_from_an_earlier_interruption_are_swept()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();

        var stale = Path.Combine(
            archiveDirectory,
            RepoContextMemoryArchive.SnapshotFileName + ".deadbeef" + RepoContextMemoryArchive.TemporaryExtension);
        await File.WriteAllTextAsync(stale, "partial", TestContext.CurrentContext.CancellationToken);

        await new RepoContextMemoryArchive(Options()).ExportAsync(
            harness.GrainFactory.GetGrain<ILattice>(MemoryTreeName),
            serializer,
            TestContext.CurrentContext.CancellationToken);

        Assert.That(File.Exists(stale), Is.False);
    }

    [Test]
    public void Export_and_restore_reject_a_null_tree_or_serializer()
    {
        var archive = new RepoContextMemoryArchive(Options());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await archive.ExportAsync(null!, null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await archive.RestoreAsync(null!, null!),
                Throws.ArgumentNullException);
        });
    }

    /// <summary>
    /// A write stream that fails once a threshold of bytes has passed through it, so a
    /// test can interrupt an export part-way through without killing the process.
    /// </summary>
    private sealed class FailingStream(Stream inner, int failAfterBytes) : Stream
    {
        private long written;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
            => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Guard(buffer.Length);
            inner.Write(buffer);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Guard(buffer.Length);
            return inner.WriteAsync(buffer, cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }

        private void Guard(int count)
        {
            written += count;
            if (written > failAfterBytes)
            {
                throw new IOException("the simulated device ran out of space mid-write");
            }
        }
    }
}
