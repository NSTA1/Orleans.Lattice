using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// <see cref="ITelemetrySink"/> implementation that writes per-vehicle telemetry into an
/// <c>Orleans.Lattice</c> tree. Implements the §3 contract in <c>benchmark/benchmark-scenarios.md</c>:
/// the producer's <c>PublishTelemetryAsync</c> path is a non-blocking channel write; a
/// long-running drain task (started as an <see cref="IHostedService"/>) batches drained samples
/// into <c>SetAsync</c> calls off the <c>VehicleGrain</c> turn.
/// </summary>
/// <remarks>
/// <para>
/// Discrete events (<see cref="VehicleEvent"/>) are deliberately discarded. The benchmark plan
/// keeps the events feed off the hot path — it's lower-volume, lower-value for the sustained
/// write benchmark, and the design seam (<see cref="ITelemetrySink"/>) intentionally allows it.
/// </para>
/// </remarks>
public sealed class LatticeSink : ITelemetrySink, IHostedService, IAsyncDisposable
{
    private readonly IGrainFactory _grainFactory;
    private readonly LatticeSinkOptions _options;
    private readonly ILogger<LatticeSink> _logger;
    private readonly Channel<VehicleTelemetryEvent> _channel;
    private readonly Func<VehicleTelemetryEvent, byte[]> _serializer;
    private readonly string[] _regions;
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _drainTask;

    /// <summary>
    /// Initializes a new instance of the <see cref="LatticeSink"/> class.
    /// </summary>
    public LatticeSink(
        IGrainFactory grainFactory,
        IOptions<LatticeSinkOptions> options,
        ILogger<LatticeSink> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _options = options.Value;
        _logger = logger;
        _serializer = _options.Serializer ?? DefaultSerializer;
        _regions = (_options.Regions ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (_regions.Length == 0)
            _regions = ["eu-west"];

        var capacity = _options.ChannelCapacity > 0 ? _options.ChannelCapacity : LatticeSinkOptions.DefaultChannelCapacity;
        var fullMode = _options.DropOnFull ? BoundedChannelFullMode.DropOldest : BoundedChannelFullMode.Wait;
        _channel = Channel.CreateBounded<VehicleTelemetryEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = fullMode,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
    }

    /// <inheritdoc />
    public ValueTask PublishTelemetryAsync(VehicleTelemetryEvent telemetry, CancellationToken cancellationToken = default)
    {
        var start = Stopwatch.GetTimestamp();
        if (_channel.Writer.TryWrite(telemetry))
        {
            LatticeSinkMetrics.Published.Add(1);
            LatticeSinkMetrics.QueueDepth.Add(1);
            LatticeSinkMetrics.InlinePublishDurationMs.Record(GetElapsedMs(start));
            return ValueTask.CompletedTask;
        }

        if (_options.DropOnFull)
        {
            LatticeSinkMetrics.Dropped.Add(1);
            LatticeSinkMetrics.InlinePublishDurationMs.Record(GetElapsedMs(start));
            return ValueTask.CompletedTask;
        }

        // BoundedChannelFullMode.Wait — fall back to async write so we apply backpressure.
        return WaitWriteAsync(telemetry, cancellationToken, start);

        async ValueTask WaitWriteAsync(VehicleTelemetryEvent t, CancellationToken ct, long s)
        {
            try
            {
                await _channel.Writer.WriteAsync(t, ct).ConfigureAwait(false);
                LatticeSinkMetrics.Published.Add(1);
                LatticeSinkMetrics.QueueDepth.Add(1);
            }
            catch (OperationCanceledException)
            {
                LatticeSinkMetrics.Dropped.Add(1);
            }
            finally
            {
                LatticeSinkMetrics.InlinePublishDurationMs.Record(GetElapsedMs(s));
            }
        }
    }

    /// <inheritdoc />
    public ValueTask PublishEventAsync(VehicleEvent vehicleEvent, CancellationToken cancellationToken = default)
        => ValueTask.CompletedTask;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _drainTask ??= Task.Run(() => DrainLoopAsync(_shutdown.Token), CancellationToken.None);
        _logger.LogInformation(
            "LatticeSink started: tree={TreeId}, keyShape={KeyShape}, batchSize={BatchSize}, flushInterval={FlushInterval}",
            _options.TreeId, _options.KeyShape, _options.BatchSize, _options.FlushInterval);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _channel.Writer.TryComplete();
        if (_drainTask is null) return;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_options.ShutdownDrainTimeout);
        try
        {
            await _drainTask.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Drain didn't complete within the budget — record a bulk drop count and move on.
            var remaining = _channel.Reader.Count;
            if (remaining > 0)
                LatticeSinkMetrics.DroppedOnShutdown.Add(remaining);
            _logger.LogWarning(
                "LatticeSink shutdown drain exceeded {Timeout}; {Remaining} samples discarded.",
                _options.ShutdownDrainTimeout, remaining);
            await _shutdown.CancelAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _shutdown.Dispose();
    }

    // ─── Drain loop ──────────────────────────────────────────────────────────

    private async Task DrainLoopAsync(CancellationToken cancellationToken)
    {
        var lattice = _grainFactory.GetGrain<ILattice>(_options.TreeId);
        var batch = new List<KeyValuePair<string, byte[]>>(_options.BatchSize);
        var nextFlush = DateTime.UtcNow + _options.FlushInterval;

        try
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_channel.Reader.TryRead(out var telemetry))
                {
                    var key = BuildKey(telemetry);
                    var value = _serializer(telemetry);
                    batch.Add(new KeyValuePair<string, byte[]>(key, value));
                    LatticeSinkMetrics.QueueDepth.Add(-1);

                    if (batch.Count >= _options.BatchSize)
                    {
                        await FlushAsync(lattice, batch, cancellationToken).ConfigureAwait(false);
                        nextFlush = DateTime.UtcNow + _options.FlushInterval;
                    }
                }

                if (batch.Count > 0 && DateTime.UtcNow >= nextFlush)
                {
                    await FlushAsync(lattice, batch, cancellationToken).ConfigureAwait(false);
                    nextFlush = DateTime.UtcNow + _options.FlushInterval;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "LatticeSink drain loop failed unexpectedly.");
        }

        if (batch.Count > 0)
        {
            try
            {
                await FlushAsync(lattice, batch, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LatticeSinkMetrics.FlushErrors.Add(batch.Count);
                _logger.LogWarning(ex, "LatticeSink final flush of {Count} samples failed.", batch.Count);
            }
        }
    }

    private async Task FlushAsync(ILattice lattice, List<KeyValuePair<string, byte[]>> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0) return;

        var size = batch.Count;
        var start = Stopwatch.GetTimestamp();
        try
        {
            if (_options.KeyShape == KeyShape.EventLogTimestamped && _options.EventLogTtl is { } ttl)
            {
                foreach (var entry in batch)
                    await lattice.SetAsync(entry.Key, entry.Value, ttl, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                foreach (var entry in batch)
                    await lattice.SetAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            LatticeSinkMetrics.FlushErrors.Add(size);
            throw;
        }
        catch (Exception ex)
        {
            LatticeSinkMetrics.FlushErrors.Add(size);
            _logger.LogWarning(ex, "LatticeSink flush of {Count} samples failed.", size);
        }
        finally
        {
            LatticeSinkMetrics.FlushBatchSize.Record(size);
            LatticeSinkMetrics.FlushDurationMs.Record(GetElapsedMs(start));
            batch.Clear();
        }
    }

    // ─── Key shape ───────────────────────────────────────────────────────────

    private string BuildKey(VehicleTelemetryEvent telemetry) => _options.KeyShape switch
    {
        KeyShape.RegionPrefixedVehicleId => $"{PickRegion(telemetry.VehicleId)}/{telemetry.VehicleId:N}",
        KeyShape.EventLogTimestamped => $"{telemetry.VehicleId:N}/{telemetry.TimestampUtc:yyyyMMddTHHmmssfffZ}",
        _ => telemetry.VehicleId.ToString("N"),
    };

    private string PickRegion(Guid vehicleId)
    {
        // Stable per-vehicle bucketing so a vehicle always lands in the same region for the run,
        // with the first region oversubscribed to <see cref="LatticeSinkOptions.HotRegionShare"/>.
        // The hash uses the high 32 bits of the Guid so two vehicles with similar timestamps
        // don't collide.
        Span<byte> bytes = stackalloc byte[16];
        vehicleId.TryWriteBytes(bytes);
        var hash = (uint)(bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24));
        var fraction = hash / (double)uint.MaxValue;
        if (_regions.Length == 1 || fraction < _options.HotRegionShare)
            return _regions[0];

        // Distribute the remaining (1 - HotRegionShare) of vehicles uniformly across the rest.
        var rest = _regions.Length - 1;
        var idx = 1 + (int)(hash % (uint)rest);
        return _regions[idx];
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static double GetElapsedMs(long start) =>
        (Stopwatch.GetTimestamp() - start) * 1000.0 / Stopwatch.Frequency;

    private static byte[] DefaultSerializer(VehicleTelemetryEvent telemetry)
    {
        // Hand-rolled UTF-8 JSON encoder — one allocation per call (the byte[]). Avoids taking
        // a System.Text.Json dependency on the abstractions package and keeps the serialized
        // shape deterministic across runs (no reflection-based property ordering).
        var buffer = new ArrayBufferWriter<byte>(initialCapacity: 192);
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true }))
        {
            writer.WriteStartObject();
            writer.WriteString("vehicleId"u8, telemetry.VehicleId);
            writer.WriteString("timestampUtc"u8, telemetry.TimestampUtc);
            writer.WriteString("fromCityId"u8, telemetry.FromCityId);
            writer.WriteString("toCityId"u8, telemetry.ToCityId);
            writer.WriteNumber("segmentProgressKm"u8, telemetry.SegmentProgressKm);
            writer.WriteNumber("segmentLengthKm"u8, telemetry.SegmentLengthKm);
            writer.WriteNumber("speedKph"u8, telemetry.SpeedKph);
            writer.WriteNumber("fuelLitres"u8, telemetry.FuelLitres);
            writer.WriteNumber("fuelCapacityLitres"u8, telemetry.FuelCapacityLitres);
            writer.WriteNumber("status"u8, (int)telemetry.Status);
            writer.WriteEndObject();
            writer.Flush();
        }
        return buffer.WrittenSpan.ToArray();
    }
}

/// <summary>Internal helper — lightweight expanding buffer for <see cref="Utf8JsonWriter"/>.</summary>
internal sealed class ArrayBufferWriter<T> : System.Buffers.IBufferWriter<T>
{
    private T[] _buffer;
    private int _index;

    public ArrayBufferWriter(int initialCapacity)
    {
        _buffer = new T[Math.Max(initialCapacity, 16)];
        _index = 0;
    }

    public ReadOnlySpan<T> WrittenSpan => new(_buffer, 0, _index);

    public void Advance(int count) => _index += count;

    public Memory<T> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_index);
    }

    public Span<T> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_index);
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint <= 0) sizeHint = 64;
        var available = _buffer.Length - _index;
        if (available >= sizeHint) return;
        var newSize = Math.Max(_buffer.Length * 2, _index + sizeHint);
        Array.Resize(ref _buffer, newSize);
    }
}

internal static class Utf8JsonReadOnlySpanExtensions
{
    // Local helper consts: the JSON keys are written as `u8` literals above but Utf8JsonWriter
    // accepts ReadOnlySpan<byte> for property names natively, so no extra plumbing is required.
}
