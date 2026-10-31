using System.Diagnostics;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// Samples the working set of a fixed set of estate processes on a short interval: a
/// baseline taken before an import, then the peak each process reaches while it runs. Each
/// process is read by its id, so a sample costs one <see cref="Process.Refresh"/> and no
/// handle is kept between samples.
/// </summary>
internal sealed class WorkingSetSampler : IAsyncDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);

    private readonly IReadOnlyList<(string Name, int ProcessId)> _processes;
    private readonly long[] _baseline;
    private readonly long[] _peak;
    private readonly CancellationTokenSource _stop = new();
    private Task? _sampling;
    private int _samples;

    private WorkingSetSampler(IReadOnlyList<(string Name, int ProcessId)> processes)
    {
        _processes = processes;
        _baseline = new long[processes.Count];
        _peak = new long[processes.Count];
    }

    /// <summary>
    /// Takes each process's baseline - the mean of several readings over a couple of seconds,
    /// so one reading caught mid-collection does not set it - and starts sampling for peaks.
    /// </summary>
    /// <param name="processes">The processes to watch, by display name and id.</param>
    /// <returns>The running sampler.</returns>
    public static async Task<WorkingSetSampler> StartAsync(IReadOnlyList<(string Name, int ProcessId)> processes)
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

        return [.. _processes.Select((process, i) => new WorkingSetGrowth(process.Name, _baseline[i], _peak[i], _samples))];
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

    private static long Read(int processId)
    {
        using var process = Process.GetProcessById(processId);
        process.Refresh();
        return process.WorkingSet64;
    }
}
