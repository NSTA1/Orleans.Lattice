using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// One import of a file into a tree (issue #5365), streamed at every hop: the file is read a
/// buffer at a time, parsed a line at a time into a bounded channel, and drained from that
/// channel into the streaming facade member for the mode -
/// <see cref="ILatticeTreeAdmin.BulkLoadStreamAsync"/> to seed an empty tree, or
/// <see cref="ILatticeDataApi.SetManyStreamAsync"/> to write into any tree. Memory is
/// bounded by the channel and the facade's batch, never by the file.
/// </summary>
/// <remarks>
/// <para>
/// <b>Seed.</b> A pre-flight pass reads the whole file first and refuses it, with the line,
/// on the first line that does not parse or whose key does not sort after the one before;
/// nothing is written unless the whole file is valid. The load pass then reads it again.
/// </para>
/// <para>
/// <b>Upsert.</b> A line that does not parse, and an entry the cluster refuses, is counted
/// and recorded in <see cref="Rejections"/> with its line, and the import reads on.
/// </para>
/// <para>
/// <b>Stop.</b> <see cref="Stop"/> ends the import gracefully: no entry is read after it, the
/// batch or chunk already in flight completes, and the counts report exactly what was
/// written. Cancelling the token passed to <see cref="RunAsync"/> - the circuit ending -
/// abandons the import instead, recorded as <see cref="DataImportPhase.Cancelled"/>.
/// </para>
/// </remarks>
internal sealed class DataImportRun : IDisposable
{
    /// <summary>How many parsed entries may wait between the reader and the writer.</summary>
    internal const int DefaultChannelCapacity = 1024;

    /// <summary>The entries per chunk a seed streams.</summary>
    internal const int SeedChunkSize = 1_000;

    /// <summary>The entries per batch an upsert streams.</summary>
    internal const int UpsertBatchSize = 256;

    private readonly string _treeId;
    private readonly DataImportSettings _settings;
    private readonly Func<Stream> _openFile;
    private readonly ILatticeDataApi? _data;
    private readonly ILatticeTreeAdmin? _admin;
    private readonly int _channelCapacity;
    private readonly int _maxLineBytes;
    private readonly Stopwatch _clock = new();
    private DataImportCountingStream? _file;
    private DataImportReader? _reader;
    private long _committed;
    private long _yielded;
    private long _checked;
    private int _phase;
    private volatile bool _stopRequested;
    private string? _failure;
    private string? _note;
    private long? _treeKeys;

    /// <summary>Prepares an import.</summary>
    /// <param name="treeId">The tree to write, as the facades name it under the caller's tenant.</param>
    /// <param name="settings">What to read and how; must be valid.</param>
    /// <param name="openFile">Opens the file from its start; called once per pass.</param>
    /// <param name="fileBytes">The file's size in bytes.</param>
    /// <param name="data">The Data API, for an upsert.</param>
    /// <param name="admin">The tree-administration API, for a seed.</param>
    /// <param name="channelCapacity">How many parsed entries may wait to be written.</param>
    /// <param name="maxLineBytes">The longest line or CSV record to read.</param>
    public DataImportRun(
        string treeId,
        DataImportSettings settings,
        Func<Stream> openFile,
        long fileBytes,
        ILatticeDataApi? data,
        ILatticeTreeAdmin? admin,
        int channelCapacity = DefaultChannelCapacity,
        int maxLineBytes = DataImportReader.DefaultMaxLineBytes)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(openFile);
        ArgumentOutOfRangeException.ThrowIfNegative(fileBytes);
        ArgumentOutOfRangeException.ThrowIfLessThan(channelCapacity, 1);
        if (settings.Mode == DataImportMode.Seed && admin is null)
        {
            throw new ArgumentException("A seed needs the tree-administration API.", nameof(admin));
        }

        if (settings.Mode == DataImportMode.Upsert && data is null)
        {
            throw new ArgumentException("An upsert needs the Data API.", nameof(data));
        }

        _treeId = treeId;
        _settings = settings;
        _openFile = openFile;
        FileBytes = fileBytes;
        _data = data;
        _admin = admin;
        _channelCapacity = channelCapacity;
        _maxLineBytes = maxLineBytes;
    }

    /// <summary>Raised, on the import's own thread, whenever its progress or phase changes.</summary>
    public event Action? Changed;

    /// <summary>The tree the import writes.</summary>
    public string TreeId => _treeId;

    /// <summary>What the import reads and how.</summary>
    public DataImportSettings Settings => _settings;

    /// <summary>Where the import is.</summary>
    public DataImportPhase Phase => (DataImportPhase)Volatile.Read(ref _phase);

    /// <summary>Whether the import has ended.</summary>
    public bool IsFinished => Phase is DataImportPhase.Succeeded or DataImportPhase.Stopped or DataImportPhase.Failed or DataImportPhase.Cancelled;

    /// <summary>The file's size in bytes.</summary>
    public long FileBytes { get; }

    /// <summary>The bytes of the file read by the current pass.</summary>
    public long BytesRead => _file?.BytesRead ?? 0;

    /// <summary>The lines read by the current pass.</summary>
    public long LinesRead => _reader?.LinesRead ?? 0;

    /// <summary>The entries the pre-flight pass found valid (a seed only).</summary>
    public long EntriesChecked => Interlocked.Read(ref _checked);

    /// <summary>The entries written so far, exactly: each is durable.</summary>
    public long EntriesCommitted => Interlocked.Read(ref _committed);

    /// <summary>Every line the import refused.</summary>
    public DataImportRejectionLog Rejections { get; } = new();

    /// <summary>Why the import failed, or <see langword="null"/>.</summary>
    public string? Failure => Volatile.Read(ref _failure);

    /// <summary>A caveat about a finished import, such as an export file with no trailer, or <see langword="null"/>.</summary>
    public string? Note => Volatile.Read(ref _note);

    /// <summary>The tree's live key count a seed's commit reported, or <see langword="null"/>.</summary>
    public long? TreeKeys => _treeKeys;

    /// <summary>How long the import has run.</summary>
    public TimeSpan Elapsed => _clock.Elapsed;

    /// <summary>Entries written per second, over the run so far.</summary>
    public double EntriesPerSecond => _clock.Elapsed.TotalSeconds is > 0.5 and var seconds ? EntriesCommitted / seconds : 0;

    /// <summary>Whether <see cref="Stop"/> has been asked for.</summary>
    public bool StopRequested => _stopRequested;

    /// <summary>Asks the import to stop after the batch or chunk in flight.</summary>
    public void Stop()
    {
        _stopRequested = true;
        Changed?.Invoke();
    }

    /// <summary>Runs the import to its end.</summary>
    /// <param name="cancellationToken">Abandons the import: its circuit ended.</param>
    /// <returns>A task that completes when the import has ended; it never faults.</returns>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _phase, (int)DataImportPhase.Checking, (int)DataImportPhase.Ready) != (int)DataImportPhase.Ready)
        {
            throw new InvalidOperationException("An import runs once.");
        }

        _clock.Start();
        try
        {
            if (_settings.Mode == DataImportMode.Seed)
            {
                if (!await CheckAsync(cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                SetPhase(DataImportPhase.Importing);
                await SeedAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                SetPhase(DataImportPhase.Importing);
                await UpsertAsync(cancellationToken).ConfigureAwait(false);
            }

            if (_settings.Format == DataImportFormat.LatticeExport && _reader is { } reader)
            {
                Volatile.Write(ref _note, !reader.TrailerSeen
                    ? "The file has no trailer line, so the export it came from may have been cut short."
                    : !reader.TrailerComplete ? "The file's trailer says the export it came from was not complete." : null);
            }

            SetPhase(_stopRequested ? DataImportPhase.Stopped : DataImportPhase.Succeeded);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            SetPhase(DataImportPhase.Cancelled);
        }
        catch (Exception exception)
        {
            Fail(Describe(exception));
        }
        finally
        {
            _clock.Stop();
            Changed?.Invoke();
        }
    }

    /// <summary>The import as an operation status, for <c>LtOperationProgress</c>.</summary>
    /// <returns>The status.</returns>
    public LatticeOperationStatus ToStatus()
    {
        var phase = Phase;
        return new LatticeOperationStatus
        {
            OperationId = "import",
            Kind = "data.import",
            Scope = new LatticeOperationScope { TenantId = string.Empty, TreeIds = [_treeId] },
            State = phase switch
            {
                DataImportPhase.Ready => LatticeOperationState.Queued,
                DataImportPhase.Checking or DataImportPhase.Importing => LatticeOperationState.Running,
                DataImportPhase.Succeeded => LatticeOperationState.Succeeded,
                DataImportPhase.Failed => LatticeOperationState.Failed,
                _ => LatticeOperationState.Cancelled,
            },
            Phase = phase == DataImportPhase.Checking ? "Checking" : "Importing",
            PhaseIndex = _settings.Mode == DataImportMode.Seed ? (phase == DataImportPhase.Checking ? 0 : 1) : null,
            PhaseCount = _settings.Mode == DataImportMode.Seed ? 2 : null,
            CompletedUnits = BytesRead,
            TotalUnits = FileBytes > 0 ? FileBytes : null,
            UnitName = "bytes",
            FailureReason = Failure,
            CancelRequested = _stopRequested,
        };
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _file?.Dispose();
        Rejections.Dispose();
    }

    /// <summary>A plain sentence for a fault that ended an import.</summary>
    /// <param name="exception">The fault.</param>
    internal string Describe(Exception exception) => exception switch
    {
        DataImportFileException file => file.Message,
        BulkLoadOrderException order => $"The key \"{order.OffendingKey}\" does not sort after \"{order.PrecedingKey}\". A seed needs strictly ascending keys.",
        TreeNotEmptyException => "The tree is not empty, so it cannot be seeded. Import with Upsert to add to a tree that holds keys.",
        InvalidOperationException when _settings.Mode == DataImportMode.Seed =>
            "The cluster refused to seed the tree. A seed needs an empty tree; import with Upsert to add to one that holds keys.",
        LatticeQuotaExceededException or InvalidOperationException => "The cluster refused more writes: the tree, or its tenant, reached a limit.",
        _ => DataErrors.Describe(exception, _settings.Mode == DataImportMode.Seed ? "seed this tree" : "write to this tree"),
    };

    private async Task<bool> CheckAsync(CancellationToken cancellationToken)
    {
        var (file, reader) = Open();
        string? previousKey = null;
        long previousLine = 0;
        await using (file.ConfigureAwait(false))
        {
            await foreach (var record in reader.ReadAsync(file, cancellationToken).ConfigureAwait(false))
            {
                if (_stopRequested)
                {
                    SetPhase(DataImportPhase.Stopped);
                    return false;
                }

                if (record.Error is { } error)
                {
                    Fail(Invariant($"Line {record.Line:N0}: {error} A seed writes nothing unless every line is valid, and nothing was written."));
                    return false;
                }

                var key = record.Entry!.Key;
                if (previousKey is not null && string.CompareOrdinal(key, previousKey) <= 0)
                {
                    Fail(Invariant($"Line {record.Line:N0}: the key \"{key}\" does not sort after \"{previousKey}\" on line {previousLine:N0}. A seed needs strictly ascending keys, so nothing was written."));
                    return false;
                }

                previousKey = key;
                previousLine = record.Line;
                if (Interlocked.Increment(ref _checked) % 4096 == 0)
                {
                    Changed?.Invoke();
                }
            }
        }

        if (EntriesChecked == 0)
        {
            Fail("The file holds no entries, so nothing was written.");
            return false;
        }

        return true;
    }

    private async Task SeedAsync(CancellationToken cancellationToken)
    {
        var (file, reader) = Open();
        await using (file.ConfigureAwait(false))
        {
            var channel = NewChannel();
            using var reading = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var producer = ProduceAsync(file, reader, channel.Writer, lines: null, seed: true, reading.Token);
            var progress = new Sink<TreeBulkLoadChunkAck>(ack =>
            {
                // Every chunk but the last is full, so the entries through an acknowledged
                // chunk are exact even when an acknowledgement is re-sent after a resume.
                var through = Math.Min((ack.ChunkIndex + 1) * SeedChunkSize, Interlocked.Read(ref _yielded));
                InterlockedMax(ref _committed, through);
                Changed?.Invoke();
            });

            try
            {
                var result = await _admin!.BulkLoadStreamAsync(
                    _treeId,
                    Guid.NewGuid().ToString("N"),
                    UntilStopped(channel.Reader, cancellationToken),
                    progress,
                    new TreeBulkLoadStreamOptions { ChunkSize = SeedChunkSize },
                    cancellationToken).ConfigureAwait(false);

                // The commit answered, so every entry handed to the load is durable.
                _treeKeys = result.TotalLiveKeys;
                Interlocked.Exchange(ref _committed, Interlocked.Read(ref _yielded));
            }
            finally
            {
                await DrainAsync(channel, producer, reading).ConfigureAwait(false);
            }
        }
    }

    private async Task UpsertAsync(CancellationToken cancellationToken)
    {
        var (file, reader) = Open();
        await using (file.ConfigureAwait(false))
        {
            var channel = NewChannel();
            using var reading = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var lines = new ConcurrentQueue<(long Ordinal, long Line)>();
            var producer = ProduceAsync(file, reader, channel.Writer, lines, seed: false, reading.Token);
            var progress = new Sink<DataSetManyStreamBatch>(batch =>
            {
                var end = batch.FirstOrdinal + batch.EntryCount;
                Dictionary<long, long>? lineOf = batch.Rejections.Count > 0 ? new(batch.Rejections.Count) : null;
                while (lines.TryPeek(out var head) && head.Ordinal < end)
                {
                    lines.TryDequeue(out head);
                    lineOf?.TryAdd(head.Ordinal, head.Line);
                }

                foreach (var refused in batch.Rejections)
                {
                    long? line = lineOf!.TryGetValue(refused.Ordinal, out var known) ? known : null;
                    Rejections.Add(new DataImportRejection(line, refused.Key, refused.Code, refused.Reason));
                }

                Interlocked.Add(ref _committed, batch.CommittedCount);
                Changed?.Invoke();
            });

            try
            {
                await _data!.SetManyStreamAsync(
                    _treeId,
                    UntilStopped(channel.Reader, cancellationToken),
                    progress,
                    new DataSetManyStreamOptions { BatchSize = UpsertBatchSize },
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await DrainAsync(channel, producer, reading).ConfigureAwait(false);
            }
        }
    }

    private Task ProduceAsync(
        Stream file,
        DataImportReader reader,
        ChannelWriter<DataEntry> writer,
        ConcurrentQueue<(long Ordinal, long Line)>? lines,
        bool seed,
        CancellationToken cancellationToken) => Task.Run(async () =>
    {
        try
        {
            long ordinal = 0;
            await foreach (var record in reader.ReadAsync(file, cancellationToken).ConfigureAwait(false))
            {
                if (_stopRequested)
                {
                    break;
                }

                if (record.Error is { } error)
                {
                    if (seed)
                    {
                        throw new DataImportFileException(record.Line, Invariant(
                            $"Line {record.Line:N0}: {error} The file changed after it was checked, so the seed stopped there."));
                    }

                    Rejections.Add(new DataImportRejection(record.Line, null, DataImportRejection.FormatCode, error));
                    Changed?.Invoke();
                    continue;
                }

                lines?.Enqueue((ordinal++, record.Line));
                await writer.WriteAsync(record.Entry!, cancellationToken).ConfigureAwait(false);
            }

            writer.TryComplete();
        }
        catch (Exception exception)
        {
            writer.TryComplete(exception);
        }
    }, CancellationToken.None);

    /// <summary>
    /// The channel's entries until <see cref="Stop"/> is asked for: the writer then receives
    /// nothing more, so it finishes the batch or chunk it holds and reports exactly that.
    /// </summary>
    private async IAsyncEnumerable<DataEntry> UntilStopped(ChannelReader<DataEntry> reader, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (!_stopRequested && reader.TryRead(out var entry))
            {
                Interlocked.Increment(ref _yielded);
                yield return entry;
            }

            if (_stopRequested)
            {
                yield break;
            }
        }
    }

    private static async Task DrainAsync(Channel<DataEntry> channel, Task producer, CancellationTokenSource reading)
    {
        // The writer has finished with the channel: stop the reader, and empty the channel so
        // a reader waiting for room wakes to the cancellation.
        await reading.CancelAsync().ConfigureAwait(false);
        channel.Writer.TryComplete();
        while (channel.Reader.TryRead(out _))
        {
        }

        await producer.ConfigureAwait(false);
    }

    private Channel<DataEntry> NewChannel() => Channel.CreateBounded<DataEntry>(new BoundedChannelOptions(_channelCapacity)
    {
        SingleReader = true,
        SingleWriter = true,
        FullMode = BoundedChannelFullMode.Wait,
    });

    private (DataImportCountingStream File, DataImportReader Reader) Open()
    {
        _file?.Dispose();
        var file = new DataImportCountingStream(_openFile());
        var reader = new DataImportReader(_settings, _maxLineBytes);
        _file = file;
        _reader = reader;
        Changed?.Invoke();
        return (file, reader);
    }

    private void Fail(string reason)
    {
        Volatile.Write(ref _failure, reason);
        SetPhase(DataImportPhase.Failed);
    }

    private void SetPhase(DataImportPhase phase)
    {
        Volatile.Write(ref _phase, (int)phase);
        Changed?.Invoke();
    }

    private static void InterlockedMax(ref long target, long value)
    {
        var current = Interlocked.Read(ref target);
        while (value > current)
        {
            var seen = Interlocked.CompareExchange(ref target, value, current);
            if (seen == current)
            {
                return;
            }

            current = seen;
        }
    }

    private static string Invariant(FormattableString text) => text.ToString(CultureInfo.InvariantCulture);

    /// <summary>An <see cref="IProgress{T}"/> that reports on the reporting thread, with no synchronization context.</summary>
    private sealed class Sink<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
