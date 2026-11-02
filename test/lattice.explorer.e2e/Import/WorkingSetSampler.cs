using System.Diagnostics;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// Watches the memory of a fixed set of estate processes across one import (#5475). Before
/// it, each process's memory is settled - an aggressive full collection that also returns
/// free memory to the operating system - so the baseline does not depend on what the
/// garbage collector happened to have done last. While it runs, the working set is sampled
/// every 250 ms and, in parallel, the live heap is read about once a second through a forced
/// full collection. After it, each process is settled again and read once more.
/// </summary>
internal sealed class WorkingSetSampler : IAsyncDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan LiveInterval = TimeSpan.FromSeconds(1);

    private readonly IReadOnlyList<MeasuredProcess> _processes;
    private readonly Func<MeasuredProcess, bool, Task<JourneyMemoryReading>> _probe;
    private readonly long[] _baseline;
    private readonly long[] _peak;
    private readonly long[] _liveBaseline;
    private readonly long[] _livePeak;
    private readonly string[] _collector;
    private readonly CancellationTokenSource _stop = new();
    private Task? _sampling;
    private Task? _liveSampling;
    private int _samples;
    private int _liveSamples;

    private WorkingSetSampler(IReadOnlyList<MeasuredProcess> processes, Func<MeasuredProcess, bool, Task<JourneyMemoryReading>> probe)
    {
        _processes = processes;
        _probe = probe;
        _baseline = new long[processes.Count];
        _peak = new long[processes.Count];
        _liveBaseline = new long[processes.Count];
        _livePeak = new long[processes.Count];
        _collector = new string[processes.Count];
    }

    /// <summary>
    /// Settles each process's memory, takes its baselines - the live heap the settling
    /// collection found, and the mean of several working-set readings straight after it -
    /// and starts sampling.
    /// </summary>
    /// <param name="processes">The processes to watch.</param>
    /// <param name="probe">Reads a process's memory after a forced collection; <see langword="true"/> settles it.</param>
    /// <returns>The running sampler.</returns>
    public static async Task<WorkingSetSampler> StartAsync(
        IReadOnlyList<MeasuredProcess> processes, Func<MeasuredProcess, bool, Task<JourneyMemoryReading>> probe)
    {
        var sampler = new WorkingSetSampler(processes, probe);
        var settled = await Task.WhenAll(processes.Select(process => probe(process, true)));
        for (var i = 0; i < processes.Count; i++)
        {
            sampler._liveBaseline[i] = settled[i].LiveHeapBytes;
            sampler._livePeak[i] = settled[i].LiveHeapBytes;
            sampler._collector[i] = settled[i].Collector;
        }

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
        }

        sampler._sampling = sampler.SampleWorkingSetsAsync(sampler._stop.Token);
        sampler._liveSampling = sampler.SampleLiveHeapsAsync(sampler._stop.Token);
        return sampler;
    }

    /// <summary>Stops sampling, settles each process once more, and returns every reading.</summary>
    /// <returns>One reading per process, in the order given.</returns>
    public async Task<IReadOnlyList<WorkingSetGrowth>> StopAsync()
    {
        await StopSamplingAsync();
        var settled = await Task.WhenAll(_processes.Select(process => _probe(process, true)));
        return [.. _processes.Select((process, i) => new WorkingSetGrowth(process.Name, _baseline[i], _peak[i], _samples)
        {
            LiveBaseline = _liveBaseline[i],
            LivePeak = _livePeak[i],
            LiveSamples = _liveSamples,
            LiveAfter = settled[i].LiveHeapBytes,
            SettledAfter = Read(process.ProcessId),
            Collector = _collector[i],
        })];
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopSamplingAsync();
        _stop.Dispose();
    }

    private async Task StopSamplingAsync()
    {
        if (!_stop.IsCancellationRequested)
        {
            await _stop.CancelAsync();
        }

        foreach (var sampling in new[] { _sampling, _liveSampling })
        {
            if (sampling is not null)
            {
                await sampling;
            }
        }
    }

    private async Task SampleWorkingSetsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            for (var i = 0; i < _processes.Count; i++)
            {
                _peak[i] = Math.Max(_peak[i], Read(_processes[i].ProcessId));
            }

            _samples++;
            if (!await PauseAsync(Interval, cancellationToken))
            {
                return;
            }
        }
    }

    private async Task SampleLiveHeapsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // The processes are read together, so a forced collection in one does not
            // shift the instant at which the others are read.
            var readings = await Task.WhenAll(_processes.Select(process => _probe(process, false)));
            for (var i = 0; i < _processes.Count; i++)
            {
                _livePeak[i] = Math.Max(_livePeak[i], readings[i].LiveHeapBytes);
            }

            _liveSamples++;
            if (!await PauseAsync(LiveInterval, cancellationToken))
            {
                return;
            }
        }
    }

    private static async Task<bool> PauseAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(interval, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private static long Read(int processId)
    {
        using var process = Process.GetProcessById(processId);
        process.Refresh();
        return process.WorkingSet64;
    }
}
