using System.Reflection;
using System.Text;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The import pipeline (issue #5365) over fake streaming facades: an upsert writes every
/// readable line and lists each refused one with its line; a seed is refused by its
/// pre-flight pass, with the line, before anything is written; Stop ends within one batch
/// and reports exactly what was written; a G2 export re-imports identically.
/// </summary>
[TestFixture]
public sealed class DataImportRunTests
{
    [Test]
    public async Task An_upsert_writes_every_readable_line_and_lists_each_refusal_with_its_line()
    {
        var data = FakeStreamingData.Create();
        using var run = Upsert(data, "{\"id\":\"a\"}\nnot json\n{\"id\":\"deny-1\"}\n{\"id\":\"b\"}\n");

        await run.RunAsync(CancellationToken.None);

        var refused = run.Rejections.Shown;
        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Succeeded));
            Assert.That(FakeStreamingData.Of(data).Written, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(run.EntriesCommitted, Is.EqualTo(2));
            Assert.That(run.Rejections.Count, Is.EqualTo(2));
            Assert.That(refused.Select(r => (r.Line, r.Key, r.Code)), Is.EquivalentTo(new[]
            {
                ((long?)2, (string?)null, DataImportRejection.FormatCode),
                (3, "deny-1", DataEntryRejection.DeniedCode),
            }));
        });
    }

    [Test]
    public async Task A_seed_whose_keys_do_not_ascend_is_refused_with_the_line_and_writes_nothing()
    {
        var admin = FakeStreamingAdmin.Create();
        using var run = Seed(admin, "{\"id\":\"a\"}\n{\"id\":\"c\"}\n{\"id\":\"b\"}\n");

        await run.RunAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Failed));
            Assert.That(run.Failure, Does.StartWith("Line 3: the key \"b\" does not sort after \"c\" on line 2."));
            Assert.That(run.Failure, Does.Contain("nothing was written"));
            Assert.That(FakeStreamingAdmin.Of(admin).Loaded, Is.Null, "the load must never start");
        });
    }

    [Test]
    public async Task A_seed_with_an_unreadable_line_is_refused_with_the_line_and_writes_nothing()
    {
        var admin = FakeStreamingAdmin.Create();
        using var run = Seed(admin, "{\"id\":\"a\"}\n{oops\n");

        await run.RunAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(run.Failure, Does.StartWith("Line 2: The line is not one JSON value"));
            Assert.That(FakeStreamingAdmin.Of(admin).Loaded, Is.Null);
        });
    }

    [Test]
    public async Task A_seed_checks_the_whole_file_then_streams_it_and_reports_the_tree()
    {
        var admin = FakeStreamingAdmin.Create();
        var lines = string.Concat(Enumerable.Range(0, 2_500).Select(i => $"{{\"id\":\"k{i:D5}\"}}\n"));
        using var run = Seed(admin, lines);

        await run.RunAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Succeeded), run.Failure);
            Assert.That(run.EntriesChecked, Is.EqualTo(2_500));
            Assert.That(run.EntriesCommitted, Is.EqualTo(2_500));
            Assert.That(run.TreeKeys, Is.EqualTo(2_500));
            Assert.That(FakeStreamingAdmin.Of(admin).Loaded!.Count, Is.EqualTo(2_500));
            Assert.That(FakeStreamingAdmin.Of(admin).ChunkSize, Is.EqualTo(DataImportRun.SeedChunkSize));
        });
    }

    [Test]
    public async Task Stop_ends_the_import_within_one_batch_and_reports_exactly_what_was_written()
    {
        var data = FakeStreamingData.Create();
        DataImportRun? run = null;
        FakeStreamingData.Of(data).AfterBatch = batch =>
        {
            if (batch == 0)
            {
                run!.Stop();
            }
        };
        var lines = string.Concat(Enumerable.Range(0, 5_000).Select(i => $"{{\"id\":\"k{i}\"}}\n"));
        run = Upsert(data, lines);

        await run.RunAsync(CancellationToken.None);

        var written = FakeStreamingData.Of(data).Written.Count;
        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Stopped));
            Assert.That(run.EntriesCommitted, Is.EqualTo(written), "the count must be exactly what landed");
            Assert.That(written, Is.LessThanOrEqualTo(2 * DataImportRun.UpsertBatchSize), "no more than the batch in flight after the stop");
        });
        run.Dispose();
    }

    [Test]
    public async Task Ending_the_circuit_cancels_the_import()
    {
        var data = FakeStreamingData.Create();
        using var circuit = new CancellationTokenSource();
        FakeStreamingData.Of(data).AfterBatch = _ => circuit.Cancel();
        var lines = string.Concat(Enumerable.Range(0, 5_000).Select(i => $"{{\"id\":\"k{i}\"}}\n"));
        using var run = Upsert(data, lines);

        await run.RunAsync(circuit.Token);

        Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Cancelled));
    }

    [Test]
    public async Task A_fault_that_is_not_a_refusal_fails_the_import_with_a_plain_reason()
    {
        var data = FakeStreamingData.Create();
        FakeStreamingData.Of(data).Fault = new LatticeQuotaExceededException("full");
        using var run = Upsert(data, "{\"id\":\"a\"}\n");

        await run.RunAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Failed));
            Assert.That(run.Failure, Does.Contain("reached a limit"));
        });
    }

    [Test]
    public async Task A_lattice_export_reimports_into_an_empty_tree_identically()
    {
        var source = new SortedDictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["order/000017"] = Encoding.UTF8.GetBytes("{\"total\":-3}"),
            ["order/000101"] = [0, 1, 2, 255],
            ["order/000233"] = Encoding.UTF8.GetBytes("plain text"),
            ["z"] = [],
        };
        var file = new MemoryStream();
        using (var encoder = new DataExportEncoder(DataExportFormat.Ndjson))
        {
            foreach (var (key, value) in source)
            {
                file.Write(encoder.Entry(key, value).Span);
            }

            file.Write(encoder.Trailer(new(DataExportEncoder.FormatId, source.Count, true, ExplorerDownloadRefusal.None, null)).Span);
        }

        var bytes = file.ToArray();
        var admin = FakeStreamingAdmin.Create();
        using var run = new DataImportRun("copy", new DataImportSettings { Format = DataImportFormat.LatticeExport, Mode = DataImportMode.Seed },
            () => new MemoryStream(bytes), bytes.Length, data: null, admin);

        await run.RunAsync(CancellationToken.None);

        var loaded = FakeStreamingAdmin.Of(admin).Loaded!;
        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Succeeded), run.Failure);
            Assert.That(run.Note, Is.Null);
            Assert.That(loaded.Select(entry => entry.Key), Is.EqualTo(source.Keys));
            Assert.That(loaded.Select(entry => entry.Value), Is.EqualTo(source.Values));
        });
    }

    [Test]
    public void An_import_runs_once_and_needs_the_facade_for_its_mode()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new DataImportRun("t", new DataImportSettings { KeyTemplate = "/id", Mode = DataImportMode.Seed }, () => Stream.Null, 0, FakeStreamingData.Create(), null),
                Throws.ArgumentException);
            Assert.That(() => new DataImportRun("t", new DataImportSettings { KeyTemplate = "/id" }, () => Stream.Null, 0, null, FakeStreamingAdmin.Create()),
                Throws.ArgumentException);
        });
    }

    [Test]
    public async Task The_rejection_log_keeps_the_first_page_in_memory_and_every_refusal_on_disk()
    {
        using var log = new DataImportRejectionLog();
        for (var i = 0; i < DataImportRejectionLog.ShownLimit + 5; i++)
        {
            log.Add(new DataImportRejection(i + 1, "k" + i, "denied", "no"));
        }

        using var reader = new StreamReader(log.OpenRead());
        var lines = (await reader.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        var path = log.FilePath!;

        Assert.Multiple(() =>
        {
            Assert.That(log.Count, Is.EqualTo(DataImportRejectionLog.ShownLimit + 5));
            Assert.That(log.Shown, Has.Count.EqualTo(DataImportRejectionLog.ShownLimit));
            Assert.That(lines, Has.Length.EqualTo(DataImportRejectionLog.ShownLimit + 5));
            Assert.That(lines[0], Is.EqualTo("{\"line\":1,\"key\":\"k0\",\"code\":\"denied\",\"reason\":\"no\"}"));
        });

        reader.Dispose();
        log.Dispose();
        Assert.That(File.Exists(path), Is.False, "the list of refused lines is deleted with the import");
    }

    private static DataImportRun Upsert(ILatticeDataApi data, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return new DataImportRun("orders", new DataImportSettings { KeyTemplate = "/id" }, () => new MemoryStream(bytes), bytes.Length, data, admin: null, channelCapacity: 16);
    }

    private static DataImportRun Seed(ILatticeTreeAdmin admin, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return new DataImportRun("orders", new DataImportSettings { KeyTemplate = "/id", Mode = DataImportMode.Seed }, () => new MemoryStream(bytes), bytes.Length, data: null, admin);
    }

    /// <summary>
    /// A streaming Data API: writes each batch, refusing every key that starts with
    /// <c>deny</c> as the facade would, and reports each batch.
    /// </summary>
    public class FakeStreamingData : DispatchProxy
    {
        internal List<string> Written { get; } = [];

        internal Action<int>? AfterBatch { get; set; }

        internal Exception? Fault { get; set; }

        internal static ILatticeDataApi Create() => Create<ILatticeDataApi, FakeStreamingData>();

        internal static FakeStreamingData Of(ILatticeDataApi api) => (FakeStreamingData)(object)api;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            targetMethod!.Name == nameof(ILatticeDataApi.SetManyStreamAsync)
                ? RunAsync((string)args![0]!, (IAsyncEnumerable<DataEntry>)args[1]!, (IProgress<DataSetManyStreamBatch>?)args[2], (DataSetManyStreamOptions?)args[3], (CancellationToken)args[4]!)
                : throw new NotSupportedException(targetMethod.Name);

        private async Task<DataSetManyStreamResult> RunAsync(string treeId, IAsyncEnumerable<DataEntry> entries, IProgress<DataSetManyStreamBatch>? progress, DataSetManyStreamOptions? options, CancellationToken cancellationToken)
        {
            var size = options?.BatchSize ?? DataSetManyStreamOptions.DefaultBatchSize;
            var batch = new List<DataEntry>(size);
            long ordinal = 0;
            var index = 0;
            await foreach (var entry in entries.WithCancellation(cancellationToken))
            {
                batch.Add(entry);
                if (batch.Count == size)
                {
                    Write(batch, ref ordinal, ref index);
                }
            }

            if (batch.Count > 0)
            {
                Write(batch, ref ordinal, ref index);
            }

            return new DataSetManyStreamResult { TreeId = treeId, EntryCount = ordinal, CommittedCount = Written.Count, RejectedCount = 0, BatchCount = index };

            void Write(List<DataEntry> entries, ref long first, ref int batchIndex)
            {
                if (Fault is { } fault)
                {
                    throw fault;
                }

                var rejections = new List<DataEntryRejection>();
                for (var i = 0; i < entries.Count; i++)
                {
                    if (entries[i].Key.StartsWith("deny", StringComparison.Ordinal))
                    {
                        rejections.Add(new DataEntryRejection { Ordinal = first + i, Key = entries[i].Key, Code = DataEntryRejection.DeniedCode, Reason = "no" });
                    }
                    else
                    {
                        Written.Add(entries[i].Key);
                    }
                }

                progress?.Report(new DataSetManyStreamBatch
                {
                    TreeId = treeId,
                    BatchIndex = batchIndex,
                    FirstOrdinal = first,
                    EntryCount = entries.Count,
                    CommittedCount = entries.Count - rejections.Count,
                    Rejections = rejections,
                });
                AfterBatch?.Invoke(batchIndex);
                first += entries.Count;
                batchIndex++;
                entries.Clear();
            }
        }
    }

    /// <summary>A streaming tree-administration API: loads the entries in chunks and acknowledges each.</summary>
    public class FakeStreamingAdmin : DispatchProxy
    {
        internal List<DataEntry>? Loaded { get; private set; }

        internal int ChunkSize { get; private set; }

        internal static ILatticeTreeAdmin Create() => Create<ILatticeTreeAdmin, FakeStreamingAdmin>();

        internal static FakeStreamingAdmin Of(ILatticeTreeAdmin admin) => (FakeStreamingAdmin)(object)admin;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            targetMethod!.Name == nameof(ILatticeTreeAdmin.BulkLoadStreamAsync)
                ? LoadAsync((string)args![0]!, (string)args[1]!, (IAsyncEnumerable<DataEntry>)args[2]!, (IProgress<TreeBulkLoadChunkAck>?)args[3], (TreeBulkLoadStreamOptions?)args[4], (CancellationToken)args[5]!)
                : throw new NotSupportedException(targetMethod.Name);

        private async Task<TreeBulkLoadResult> LoadAsync(string treeId, string operationId, IAsyncEnumerable<DataEntry> entries, IProgress<TreeBulkLoadChunkAck>? progress, TreeBulkLoadStreamOptions? options, CancellationToken cancellationToken)
        {
            ChunkSize = options?.ChunkSize ?? TreeBulkLoadStreamOptions.DefaultChunkSize;
            Loaded = [];
            long chunk = 0;
            var inChunk = 0;
            await foreach (var entry in entries.WithCancellation(cancellationToken))
            {
                Loaded.Add(entry);
                if (++inChunk == ChunkSize)
                {
                    progress?.Report(Ack(chunk++, inChunk));
                    inChunk = 0;
                }
            }

            if (inChunk > 0)
            {
                progress?.Report(Ack(chunk, inChunk));
            }

            return new TreeBulkLoadResult { TreeId = treeId, OperationId = operationId, TotalLiveKeys = Loaded.Count };

            TreeBulkLoadChunkAck Ack(long index, int count) => new()
            {
                TreeId = treeId,
                OperationId = operationId,
                ChunkIndex = index,
                AcceptedEntryCount = count,
                NextChunkIndex = index + 1,
            };
        }
    }
}
