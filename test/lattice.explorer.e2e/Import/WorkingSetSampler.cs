using System.Diagnostics;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// Watches the memory of a fixed set of estate processes across one import (#5475). Before
/// it, each process's memory is settled - an aggressive full collection that also returns
/// free memory to the operating system - so the baseline does not depend on what the
/// garbage collector happened to have done last. While it runs, the working set is sampled
/// every 250 ms and, in parallel, the live heap is read about once a second through a forced
/// full collection. After it, each process is settled again, until its live heap has stopped
/// falling for several settles in a row (#6115), and read once more.
/// </summary>
internal sealed class WorkingSetSampler : IAsyncDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan LiveInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DrainInterval = TimeSpan.FromSeconds(5);

    /// <summary>How far a settled live heap must fall between two settles for the process to count as still draining.</summary>
    internal const long DrainStepBytes = 4L * 1024 * 1024;

    /// <summary>The most re-settles the drain takes after the first: a minute at <see cref="DrainInterval"/>.</summary>
    internal const int DrainRounds = 12;

    /// <summary>
    /// How many consecutive settles in a row must not fall before the drain ends: fifteen
    /// seconds at <see cref="DrainInterval"/>. One quiet round is not enough, because a silo
    /// working through the backlog a slow import queued can hold its heap level between two
    /// batches for longer than one interval (#6115).
    /// </summary>
    internal const int QuietRounds = 3;

    /// <summary>
    /// The most further settles taken while a process's settled live heap is still above what it
    /// may keep: three minutes at <see cref="DrainInterval"/>. A silo releases state a large
    /// upsert queued on its own schedule, and on a slow run that comes later than the quiet
    /// rounds (run 45008970668: west-silo-1 held 102.2 MB above its pre-warm-up heap, level for
    /// fifteen seconds; on the run before, the same silo dropped 31.7 MB after its warm-up had
    /// settled). A process that genuinely keeps state is still read as keeping it once the
    /// rounds run out.
    /// </summary>
    internal const int HoldRounds = 36;

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

    /// <summary>
    /// Stops sampling, settles each process until its live heap has drained, and returns every
    /// reading. A process whose settled live heap is still <see cref="ImportMemoryBudget.KeptCeilingBytes"/> or more
    /// above <paramref name="keptFrom"/> is settled further, at most <see cref="HoldRounds"/>
    /// times, before what it kept is read.
    /// </summary>
    /// <param name="keptFrom">
    /// Each process's settled live heap before the first of the imports being measured, in bytes;
    /// <see langword="null"/> (for the list, or for one process) to measure from this sampler's own baseline.
    /// </param>
    /// <returns>One reading per process, in the order given.</returns>
    public async Task<IReadOnlyList<WorkingSetGrowth>> StopAsync(IReadOnlyList<long?>? keptFrom = null)
    {
        await StopSamplingAsync();
        Func<Task<long[]>> settle = async () => [.. (await Task.WhenAll(_processes.Select(process => _probe(process, true)))).Select(reading => reading.LiveHeapBytes)];
        Func<Task> pause = () => Task.Delay(DrainInterval);
        var settled = await SettleUntilDrainedAsync(settle, pause, DrainRounds);
        var holdAbove = _processes.Select((_, i) => (keptFrom?[i] ?? _liveBaseline[i]) + ImportMemoryBudget.KeptCeilingBytes).ToArray();
        var drained = await HoldUntilUnderAsync(settled, settle, pause, holdAbove, HoldRounds);
        return [.. _processes.Select((process, i) => new WorkingSetGrowth(process.Name, _baseline[i], _peak[i], _samples)
        {
            LiveBaseline = _liveBaseline[i],
            LivePeak = _livePeak[i],
            LiveSamples = _liveSamples,
            LiveAfter = settled[i],
            LiveDrained = drained[i],
            SettledAfter = Read(process.ProcessId),
            Collector = _collector[i],
        })];
    }

    /// <summary>
    /// Settles every process, then settles again after each <paramref name="pause"/> for as long
    /// as any process's live heap has fallen by <see cref="DrainStepBytes"/> or more within the
    /// last <see cref="QuietRounds"/> settles, at most <paramref name="rounds"/> more times, and
    /// returns each process's lowest settled reading (#6115). An import reports success once its
    /// last batch commits, while the silos may still be applying work it queued; a slow run is
    /// read mid-drain, and what is still draining is not what the process keeps. A process that
    /// keeps state does not fall, so it is read after <see cref="QuietRounds"/> more settles.
    /// </summary>
    /// <param name="settle">Settles every process and returns each live heap, in bytes.</param>
    /// <param name="pause">Waits between two settles.</param>
    /// <param name="rounds">The most re-settles after the first.</param>
    /// <returns>Each process's lowest settled live heap, in bytes.</returns>
    internal static async Task<long[]> SettleUntilDrainedAsync(Func<Task<long[]>> settle, Func<Task> pause, int rounds)
    {
        var lowest = await settle();
        var quiet = 0;
        for (var round = 0; round < rounds; round++)
        {
            await pause();
            var next = await settle();
            var draining = false;
            for (var i = 0; i < lowest.Length; i++)
            {
                draining |= next[i] <= lowest[i] - DrainStepBytes;
                lowest[i] = Math.Min(lowest[i], next[i]);
            }

            quiet = draining ? 0 : quiet + 1;
            if (quiet >= QuietRounds)
            {
                break;
            }
        }

        return lowest;
    }

    /// <summary>
    /// Starting from <paramref name="settled"/>, settles every process again after each
    /// <paramref name="pause"/> for as long as any process's lowest live heap is at or above its
    /// <paramref name="holdAbove"/>, at most <paramref name="rounds"/> times, and returns each
    /// process's lowest reading. <paramref name="settled"/> is not changed, so the in-flight
    /// reading it supplies is not stretched by the longer wait.
    /// </summary>
    /// <param name="settled">Each process's lowest settled live heap so far, in bytes.</param>
    /// <param name="settle">Settles every process and returns each live heap, in bytes.</param>
    /// <param name="pause">Waits between two settles.</param>
    /// <param name="holdAbove">Each process's live heap at or above which it is settled further, in bytes.</param>
    /// <param name="rounds">The most further settles.</param>
    /// <returns>Each process's lowest settled live heap, in bytes.</returns>
    internal static async Task<long[]> HoldUntilUnderAsync(
        IReadOnlyList<long> settled, Func<Task<long[]>> settle, Func<Task> pause, IReadOnlyList<long> holdAbove, int rounds)
    {
        var lowest = settled.ToArray();
        for (var round = 0; round < rounds && lowest.Where((heap, i) => heap >= holdAbove[i]).Any(); round++)
        {
            await pause();
            var next = await settle();
            for (var i = 0; i < lowest.Length; i++)
            {
                lowest[i] = Math.Min(lowest[i], next[i]);
            }
        }

        return lowest;
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
