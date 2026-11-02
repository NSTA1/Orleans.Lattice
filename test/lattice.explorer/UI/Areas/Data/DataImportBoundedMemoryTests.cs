using System.Reflection;
using System.Text;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The bounded-memory proof for the Explorer's hop of an import (issue #5365): a generated
/// 200 MB NDJSON file - produced on the fly, so the test itself holds none of it - is read,
/// parsed and streamed through the import pipeline into a facade that keeps nothing, and the
/// memory the process retains, measured after a full collection while the import runs,
/// never grows by more than 64 MB. A hop that buffered the file, or a channel that did not
/// push back, would retain hundreds of megabytes and fail.
/// </summary>
/// <remarks>
/// The facade hops beyond the Explorer (the gRPC client and the server's batch-at-a-time
/// handler) are bounded by their own windows; the out-of-process proof that measures the
/// silo too is the journeys' harness, which the epic's F1 estate provides.
/// </remarks>
[TestFixture]
public sealed class DataImportBoundedMemoryTests
{
    private const long FileBytes = 200L * 1024 * 1024;
    private const long CeilingBytes = 64L * 1024 * 1024;

    [Test]
    public async Task A_200_MB_upsert_retains_less_than_64_MB_above_its_baseline()
    {
        var data = CountingData.Create();
        var sink = CountingData.Of(data);
        using var run = new DataImportRun(
            "orders",
            new DataImportSettings { KeyTemplate = "order/{/id}" },
            () => new GeneratedNdjson(FileBytes),
            FileBytes,
            data,
            admin: null);

        var baseline = GC.GetTotalMemory(forceFullCollection: true);
        long peak = 0;
        sink.AfterBatch = batch =>
        {
            if (batch % 400 == 0)
            {
                peak = Math.Max(peak, GC.GetTotalMemory(forceFullCollection: true) - baseline);
            }
        };

        await run.RunAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(run.Phase, Is.EqualTo(DataImportPhase.Succeeded), run.Failure);
            Assert.That(run.BytesRead, Is.EqualTo(FileBytes), "the whole file must have been read");
            Assert.That(sink.Entries, Is.EqualTo(run.EntriesCommitted));
            Assert.That(sink.Batches, Is.GreaterThan(400), "the memory was sampled while the import ran");
            Assert.That(peak, Is.LessThan(CeilingBytes),
                $"the import retained {peak / (1024 * 1024)} MB above its baseline while reading a {FileBytes / (1024 * 1024)} MB file");
        });
    }

    /// <summary>An NDJSON file of about 200-byte lines, generated as it is read.</summary>
    private sealed class GeneratedNdjson(long length) : Stream
    {
        private static readonly string Payload = new('x', 160);
        private byte[] _line = [];
        private int _lineOffset;
        private long _position;
        private long _next;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => length;

        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            var written = 0;
            while (written < buffer.Length && _position < length)
            {
                if (_lineOffset >= _line.Length)
                {
                    _line = Encoding.UTF8.GetBytes($"{{\"id\":\"{_next++:D9}\",\"payload\":\"{Payload}\"}}\n");
                    _lineOffset = 0;
                }

                var take = (int)Math.Min(Math.Min(buffer.Length - written, _line.Length - _lineOffset), length - _position);
                _line.AsSpan(_lineOffset, take).CopyTo(buffer[written..]);
                _lineOffset += take;
                written += take;
                _position += take;
            }

            return written;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(Read(buffer.Span));

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>A streaming Data API that counts what it is sent and keeps none of it.</summary>
    public class CountingData : DispatchProxy
    {
        internal long Entries { get; private set; }

        internal int Batches { get; private set; }

        internal Action<int>? AfterBatch { get; set; }

        internal static ILatticeDataApi Create() => Create<ILatticeDataApi, CountingData>();

        internal static CountingData Of(ILatticeDataApi api) => (CountingData)(object)api;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            targetMethod!.Name == nameof(ILatticeDataApi.SetManyStreamAsync)
                ? RunAsync((string)args![0]!, (IAsyncEnumerable<DataEntry>)args[1]!, (IProgress<DataSetManyStreamBatch>?)args[2], (DataSetManyStreamOptions?)args[3], (CancellationToken)args[4]!)
                : throw new NotSupportedException(targetMethod.Name);

        private async Task<DataSetManyStreamResult> RunAsync(string treeId, IAsyncEnumerable<DataEntry> entries, IProgress<DataSetManyStreamBatch>? progress, DataSetManyStreamOptions? options, CancellationToken cancellationToken)
        {
            var size = options?.BatchSize ?? DataSetManyStreamOptions.DefaultBatchSize;
            var inBatch = 0;
            await foreach (var _ in entries.WithCancellation(cancellationToken))
            {
                if (++inBatch == size)
                {
                    Report(inBatch);
                    inBatch = 0;
                }
            }

            if (inBatch > 0)
            {
                Report(inBatch);
            }

            return new DataSetManyStreamResult { TreeId = treeId, EntryCount = Entries, CommittedCount = Entries, RejectedCount = 0, BatchCount = Batches };

            void Report(int count)
            {
                progress?.Report(new DataSetManyStreamBatch
                {
                    TreeId = treeId,
                    BatchIndex = Batches,
                    FirstOrdinal = Entries,
                    EntryCount = count,
                    CommittedCount = count,
                });
                Entries += count;
                AfterBatch?.Invoke(Batches);
                Batches++;
            }
        }
    }
}
