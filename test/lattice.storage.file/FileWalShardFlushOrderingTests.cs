using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;
using IoEvent = Orleans.Lattice.Storage.File.Tests.RecordingFileWalFileSystem.IoEvent;
using IoKind = Orleans.Lattice.Storage.File.Tests.RecordingFileWalFileSystem.IoKind;
using IoTarget = Orleans.Lattice.Storage.File.Tests.RecordingFileWalFileSystem.IoTarget;

namespace Orleans.Lattice.Storage.File.Tests;

/// <summary>
/// Pins the ordering between a physical flush (<c>Flush(true)</c>) and the
/// acknowledgement of every durable <see cref="FileWalStorageProvider"/>
/// operation (issue #3462).
/// <para>
/// Every other durability test in the repository proves round-trip survival
/// across a graceful, same-process reopen, and all of them pass identically
/// with <see cref="FileWalStorageOptions.FlushToDisk"/> off, because unsynced
/// bytes are still in the OS page cache when the provider is rebuilt. This
/// fixture instead drives the provider through
/// <see cref="RecordingFileWalFileSystem"/>, which records every write, flush,
/// and replace and can hold or fault the next <c>Flush(true)</c>. All
/// synchronisation is barrier-based: an operation runs off the test thread and
/// the test waits for whichever comes first, the held flush or the
/// acknowledgement, so a regression that acknowledges without fsync reddens
/// immediately rather than timing out.
/// </para>
/// </summary>
[TestFixture]
public sealed class FileWalShardFlushOrderingTests
{
    private const string TreeId = "tree-flush-ordering";

    private ServiceProvider _services = null!;
    private Serializer<WalRecord> _serializer = null!;
    private OrleansBinaryWalRecordEncoder _encoder = null!;
    private string _root = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<WalRecord>>();
        _encoder = new OrleansBinaryWalRecordEncoder(_serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "lattice-file-wal-flush-ordering", Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(_root);
    }

    [TearDown]
    public void TearDown()
    {
        try
        {
            if (System.IO.Directory.Exists(_root))
            {
                System.IO.Directory.Delete(_root, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup; a leaked temp directory does not fail the test.
        }
    }

    private FileWalStorageProvider CreateProvider(IFileWalFileSystem fileSystem, bool flushToDisk = true)
    {
        var options = Options.Create(new FileWalStorageOptions
        {
            RootDirectory = _root,
            FlushToDisk = flushToDisk,
        });
        return new FileWalStorageProvider(options, _serializer, GcWalReadPressureGovernor.Instance, fileSystem);
    }

    private FileWalStorageProvider CreatePhysicalProvider() =>
        CreateProvider(PhysicalFileWalFileSystem.Instance);

    private static WalEntry Entry(long offset, byte tag) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = "k" + offset,
            Value = new[] { tag },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private static WalEntry[] Entries(long first, int count)
    {
        var entries = new WalEntry[count];
        for (var i = 0; i < count; i++)
        {
            entries[i] = Entry(first + i, (byte)(first + i));
        }

        return entries;
    }

    private (ArraySegment<byte>[] Segments, long[] Offsets) Encoded(long first, int count)
    {
        var segments = new ArraySegment<byte>[count];
        var offsets = new long[count];
        for (var i = 0; i < count; i++)
        {
            var record = new WalRecord
            {
                TreeId = TreeId,
                Op = MutationKind.Set,
                Key = "e" + (first + i),
                Value = new[] { (byte)i },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            };
            var writer = new System.Buffers.ArrayBufferWriter<byte>();
            _encoder.Encode(in record, writer);
            segments[i] = new ArraySegment<byte>(writer.WrittenMemory.ToArray());
            offsets[i] = first + i;
        }

        return (segments, offsets);
    }

    private static async Task<long[]> ReadOffsetsAsync(FileWalStorageProvider provider)
    {
        var offsets = new List<long>();
        await foreach (var entry in provider.ReadAsync(TreeId, 0, -1L, 1024, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }

        return offsets.ToArray();
    }

    /// <summary>
    /// Runs <paramref name="operation"/> off the test thread with a hold armed
    /// on the next <c>Flush(true)</c>, and asserts that the operation cannot
    /// complete while that flush is held. Returns the events recorded by the
    /// time the operation completed, plus the index at which the operation
    /// started, so the caller can assert ordering over just its own events.
    /// </summary>
    private static async Task<(IReadOnlyList<IoEvent> Events, int Start)> RunWithHeldFlushAsync(
        RecordingFileWalFileSystem fileSystem,
        Func<Task> operation,
        string what)
    {
        var start = fileSystem.Events.Count;
        var hold = fileSystem.ArmHold();
        var op = Task.Run(operation);
        try
        {
            var first = await Task.WhenAny(hold.Entered, op);
            if (first == op)
            {
                await op;
                Assert.Fail(
                    $"{what} was acknowledged without ever issuing Flush(true). With FlushToDisk on, the task "
                    + "must not complete before the bytes are forced to the device.");
            }

            Assert.That(
                op.IsCompleted,
                Is.False,
                $"{what} completed while its Flush(true) was still held: it acknowledged before fsync.");
        }
        finally
        {
            hold.Release();
        }

        await op;
        return (fileSystem.Events, start);
    }

    private static int LastIndexOf(IReadOnlyList<IoEvent> events, int start, IoKind kind, IoTarget target)
    {
        for (var i = events.Count - 1; i >= start; i--)
        {
            if (events[i].Kind == kind && events[i].Target == target)
            {
                return i;
            }
        }

        return -1;
    }

    private static void AssertNoWriteFollowsTheLastPhysicalFlush(
        IReadOnlyList<IoEvent> events,
        int start,
        IoTarget target,
        string what)
    {
        var lastFlush = LastIndexOf(events, start, IoKind.FlushToDisk, target);
        var lastWrite = LastIndexOf(events, start, IoKind.Write, target);
        Assert.Multiple(() =>
        {
            Assert.That(lastWrite, Is.GreaterThanOrEqualTo(start), $"{what} must have written to the {target}.");
            Assert.That(
                lastFlush,
                Is.GreaterThan(lastWrite),
                $"{what}: a write to the {target} followed its last Flush(true), so those bytes were acknowledged "
                + "without being forced to the device.");
        });
    }

    [Test]
    public async Task AppendBatchAsync_acknowledges_only_after_the_batch_is_flushed_to_disk()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using var provider = CreateProvider(fileSystem);

        var (events, start) = await RunWithHeldFlushAsync(
            fileSystem,
            () => provider.AppendBatchAsync(TreeId, 0, Entries(0, 3), CancellationToken.None),
            "AppendBatchAsync");

        AssertNoWriteFollowsTheLastPhysicalFlush(events, start, IoTarget.Log, "AppendBatchAsync");
        Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task AppendEncodedBatchAsync_acknowledges_only_after_the_batch_is_flushed_to_disk()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using var provider = CreateProvider(fileSystem);
        var (segments, offsets) = Encoded(0, 3);

        var (events, start) = await RunWithHeldFlushAsync(
            fileSystem,
            () => provider.AppendEncodedBatchAsync(TreeId, 0, segments, offsets, _encoder, CancellationToken.None),
            "AppendEncodedBatchAsync");

        AssertNoWriteFollowsTheLastPhysicalFlush(events, start, IoTarget.Log, "AppendEncodedBatchAsync");
        Assert.That(await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None), Is.EqualTo(2L));
    }

    [Test]
    public async Task TrimAsync_acknowledges_only_after_its_marker_is_flushed_to_disk()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using var provider = CreateProvider(fileSystem);
        await provider.AppendBatchAsync(TreeId, 0, Entries(0, 4), CancellationToken.None);

        var (events, start) = await RunWithHeldFlushAsync(
            fileSystem,
            () => provider.TrimAsync(TreeId, 0, 1L, CancellationToken.None),
            "TrimAsync");

        AssertNoWriteFollowsTheLastPhysicalFlush(events, start, IoTarget.Log, "TrimAsync");
        Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 2L, 3L }));
    }

    [Test]
    public async Task ReconcileAsync_flushes_the_compaction_target_to_disk_before_replacing_the_log()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using var provider = CreateProvider(fileSystem);
        await provider.AppendBatchAsync(TreeId, 0, Entries(0, 10), CancellationToken.None);

        // The default 64 KiB minimum-dead-bytes floor keeps this tiny trim from
        // compacting inline, so the dead bytes are still there for reconcile.
        await provider.TrimAsync(TreeId, 0, 4L, CancellationToken.None);

        var start = fileSystem.Events.Count;
        var hold = fileSystem.ArmHold();
        var op = Task.Run(() => provider.ReconcileAsync(TreeId, 0, CancellationToken.None));
        try
        {
            var first = await Task.WhenAny(hold.Entered, op);
            if (first == op)
            {
                await op;
                Assert.Fail(
                    "ReconcileAsync compacted without ever issuing Flush(true) on the compaction target. Replacing "
                    + "wal.log with an unsynced file loses every retained entry on power loss.");
            }

            var whileHeld = fileSystem.Events;
            Assert.That(
                LastIndexOf(whileHeld, start, IoKind.Replace, IoTarget.Log),
                Is.EqualTo(-1),
                "wal.log was replaced while the compaction target's Flush(true) was still held.");
        }
        finally
        {
            hold.Release();
        }

        await op;

        var events = fileSystem.Events;
        var targetFlush = LastIndexOf(events, start, IoKind.FlushToDisk, IoTarget.CompactionTarget);
        var replace = LastIndexOf(events, start, IoKind.Replace, IoTarget.Log);
        Assert.Multiple(() =>
        {
            Assert.That(replace, Is.GreaterThanOrEqualTo(start), "Reconcile must have compacted and replaced wal.log.");
            Assert.That(
                targetFlush,
                Is.GreaterThanOrEqualTo(start).And.LessThan(replace),
                "The compaction target must be flushed to disk before it replaces wal.log.");
        });
        AssertNoWriteFollowsTheLastPhysicalFlush(events, start, IoTarget.CompactionTarget, "Compaction");
        Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 5L, 6L, 7L, 8L, 9L }));
    }

    [Test]
    public async Task With_FlushToDisk_off_no_operation_issues_a_physical_flush_and_the_hold_never_engages()
    {
        // Anti-vacuity control for every hold test above: if the recorder were
        // blind to Flush(true), or engaged on something else, this would redden.
        var fileSystem = new RecordingFileWalFileSystem();
        using var provider = CreateProvider(fileSystem, flushToDisk: false);
        var (segments, offsets) = Encoded(10, 2);

        var hold = fileSystem.ArmHold();
        var op = Task.Run(async () =>
        {
            await provider.AppendBatchAsync(TreeId, 0, Entries(0, 10), CancellationToken.None);
            await provider.AppendEncodedBatchAsync(TreeId, 0, segments, offsets, _encoder, CancellationToken.None);
            await provider.TrimAsync(TreeId, 0, 4L, CancellationToken.None);
            await provider.ReconcileAsync(TreeId, 0, CancellationToken.None);
        });
        try
        {
            var first = await Task.WhenAny(hold.Entered, op);
            Assert.That(first, Is.SameAs(op), "With FlushToDisk off, no operation may issue Flush(true).");
            await op;
        }
        finally
        {
            hold.Release();
        }

        var events = fileSystem.Events;
        Assert.Multiple(() =>
        {
            Assert.That(events.Count(e => e.Kind == IoKind.FlushToDisk), Is.Zero);
            Assert.That(events.Count(e => e.Kind == IoKind.Flush), Is.GreaterThanOrEqualTo(4),
                "Every append, trim, and compaction must still push its bytes to the OS.");
            Assert.That(events.Any(e => e.Kind == IoKind.Replace), Is.True,
                "The control must exercise the compaction path too, or it proves nothing about it.");
            Assert.That(hold.Entered.IsCompleted, Is.False);
        });
    }

    [Test]
    public async Task A_failed_fsync_on_append_faults_the_task_and_the_batch_is_not_resurrected_by_a_reopen()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using (var provider = CreateProvider(fileSystem))
        {
            fileSystem.ArmFault();

            Assert.That(
                async () => await provider.AppendBatchAsync(TreeId, 0, Entries(0, 3), CancellationToken.None),
                Throws.InstanceOf<IOException>(),
                "A failed fsync must fault the append rather than acknowledge it.");

            var inProcessOffsets = await ReadOffsetsAsync(provider);
            var inProcessHighest = await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
            Assert.Multiple(() =>
            {
                Assert.That(inProcessOffsets, Is.Empty);
                Assert.That(inProcessHighest, Is.EqualTo(-1L));
            });
        }

        using var reopened = CreatePhysicalProvider();
        var recoveredOffsets = await ReadOffsetsAsync(reopened);
        var recoveredHighest = await reopened.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(
                recoveredOffsets,
                Is.Empty,
                "The batch whose fsync failed was reported as failed, so recovery must not roll it forward.");
            Assert.That(
                recoveredHighest,
                Is.EqualTo(-1L),
                "A resurrected failed batch would advance the recovered watermark.");
        });
    }

    [Test]
    public async Task A_failed_fsync_on_append_rolls_back_only_the_failed_batch_and_the_shard_keeps_working()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using (var provider = CreateProvider(fileSystem))
        {
            await provider.AppendBatchAsync(TreeId, 0, Entries(0, 2), CancellationToken.None);

            fileSystem.ArmFault();
            Assert.That(
                async () => await provider.AppendBatchAsync(TreeId, 0, Entries(2, 2), CancellationToken.None),
                Throws.InstanceOf<IOException>());

            // The caller retries the same offsets, exactly as a WAL grain would.
            var (segments, offsets) = Encoded(2, 3);
            await provider.AppendEncodedBatchAsync(TreeId, 0, segments, offsets, _encoder, CancellationToken.None);
        }

        using var reopened = CreatePhysicalProvider();
        var recoveredOffsets = await ReadOffsetsAsync(reopened);
        var recoveredHighest = await reopened.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(recoveredOffsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L }));
            Assert.That(recoveredHighest, Is.EqualTo(4L));
        });
    }

    [Test]
    public async Task A_failed_fsync_on_trim_faults_the_task_and_the_trim_is_not_resurrected_by_a_reopen()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using (var provider = CreateProvider(fileSystem))
        {
            await provider.AppendBatchAsync(TreeId, 0, Entries(0, 4), CancellationToken.None);

            fileSystem.ArmFault();
            Assert.That(
                async () => await provider.TrimAsync(TreeId, 0, 2L, CancellationToken.None),
                Throws.InstanceOf<IOException>());

            Assert.That(await ReadOffsetsAsync(provider), Is.EqualTo(new[] { 0L, 1L, 2L, 3L }));
        }

        using var reopened = CreatePhysicalProvider();
        Assert.That(
            await ReadOffsetsAsync(reopened),
            Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
            "The trim whose fsync failed was reported as failed, so recovery must not apply its marker.");
    }

    [Test]
    public async Task A_failed_rollback_after_a_failed_fsync_fail_stops_the_shard()
    {
        var fileSystem = new RecordingFileWalFileSystem();
        using var provider = CreateProvider(fileSystem);
        await provider.AppendBatchAsync(TreeId, 0, Entries(0, 1), CancellationToken.None);

        // The first fault is the batch's own fsync; the second is the fsync of
        // the truncation that rolls it back.
        fileSystem.ArmFault(count: 2);
        Assert.That(
            async () => await provider.AppendBatchAsync(TreeId, 0, Entries(1, 2), CancellationToken.None),
            Throws.InstanceOf<IOException>());

        var readFailure = Assert.ThrowsAsync<IOException>(
            async () => await provider.GetHighestOffsetAsync(TreeId, 0, CancellationToken.None));
        var appendFailure = Assert.ThrowsAsync<IOException>(
            async () => await provider.AppendBatchAsync(TreeId, 0, Entries(1, 2), CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(readFailure!.Message, Does.Contain("fail-stopped"));
            Assert.That(readFailure.InnerException, Is.InstanceOf<IOException>(),
                "The fail-stop must carry the rollback failure that caused it.");
            Assert.That(appendFailure!.Message, Does.Contain("fail-stopped"));
        });
    }

    [Test]
    public void Constructor_rejects_a_null_file_system()
    {
        var options = Options.Create(new FileWalStorageOptions { RootDirectory = _root });

        Assert.That(
            () => new FileWalStorageProvider(options, _serializer, GcWalReadPressureGovernor.Instance, null!),
            Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName)).EqualTo("fileSystem"));
    }
}
