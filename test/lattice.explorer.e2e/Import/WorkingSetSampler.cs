using System.Diagnostics;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// Samples the working set of a fixed set of estate processes on a short interval: a
/// baseline taken before an import, then the peak each process reaches while it runs. Each
/// process is read by its id, so a sample costs one <see cref="Process.Refresh"/> and no
/// handle is kept between samples. A process that serves <c>/metrics</c> also has its live
/// managed heap read, about once a second, as evidence beside the working set.
/// </summary>
internal sealed class WorkingSetSampler : IAsyncDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);

    private readonly IReadOnlyList<MeasuredProcess> _processes;
    private readonly long[] _baseline;
    private readonly long[] _peak;
    private readonly long?[] _heapBaseline;
    private readonly long?[] _heapPeak;
    private readonly ManagedHeapReader _heap = new();
    private readonly CancellationTokenSource _stop = new();
    private Task? _sampling;
    private int _samples;

    private WorkingSetSampler(IReadOnlyList<MeasuredProcess> processes)
    {
        _processes = processes;
        _baseline = new long[processes.Count];
        _peak = new long[processes.Count];
        _heapBaseline = new long?[processes.Count];
        _heapPeak = new long?[processes.Count];
    }

    /// <summary>
    /// Takes each process's baseline - the mean of several readings over a couple of seconds,
    /// so one reading caught mid-collection does not set it - and starts sampling for peaks.
    /// </summary>
    /// <param name="processes">The processes to watch.</param>
    /// <returns>The running sampler.</returns>
    public static async Task<WorkingSetSampler> StartAsync(IReadOnlyList<MeasuredProcess> processes)
    {
        var sampler = new WorkingSetSampler(processes);
        const int Readings = 8;
        var totals = new long[processes.Count];
        for (var reading = 0; reading < Readings; reading++)
        {
            for (var i = 0; i < processes.Count; i++)
            {
                totals[i] += Read(processes[i].ProcessId);
            }

            await Task.Delay(Interval);
        }

        for (var i = 0; i < processes.Count; i++)
        {
            sampler._baseline[i] = totals[i] / Readings;
            sampler._peak[i] = sampler._baseline[i];
            sampler._heapBaseline[i] = await sampler.ReadHeapAsync(processes[i]);
            sampler._heapPeak[i] = sampler._heapBaseline[i];
        }

        sampler._sampling = sampler.SampleAsync(sampler._stop.Token);
        return sampler;
    }

    /// <summary>Stops sampling and returns each process's baseline and peak.</summary>
    /// <returns>One reading per process, in the order given.</returns>
    public async Task<IReadOnlyList<WorkingSetGrowth>> StopAsync()
    {
        await _stop.CancelAsync();
        if (_sampling is not null)
        {
            await _sampling;
        }

        return [.. _processes.Select((process, i) => new WorkingSetGrowth(process.Name, _baseline[i], _peak[i], _samples)
        {
            HeapBaseline = _heapBaseline[i],
            HeapPeak = _heapPeak[i],
        })];
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        if (_sampling is not null)
        {
            await _sampling;
        }

        _stop.Dispose();
        _heap.Dispose();
    }

    private async Task SampleAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            for (var i = 0; i < _processes.Count; i++)
            {
                _peak[i] = Math.Max(_peak[i], Read(_processes[i].ProcessId));
            }

            _samples++;
            if (_samples % 4 == 0)
            {
                for (var i = 0; i < _processes.Count; i++)
                {
                    if (await ReadHeapAsync(_processes[i]) is { } heap)
                    {
                        _heapPeak[i] = Math.Max(_heapPeak[i] ?? 0, heap);
                    }
                }
            }

            try
            {
                await Task.Delay(Interval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private Task<long?> ReadHeapAsync(MeasuredProcess process) =>
        process.MetricsPort is { } port ? _heap.ReadAsync(port) : Task.FromResult<long?>(null);

    private static long Read(int processId)
    {
        using var process = Process.GetProcessById(processId);
        process.Refresh();
        return process.WorkingSet64;
    }
}
