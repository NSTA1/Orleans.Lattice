using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using System.Diagnostics.Tracing;

namespace Orleans.Lattice.Benchmark.Microbench.Profiling;

/// <summary>
/// Opens an EventPipe session against the current process, streams events to
/// a <c>.nettrace</c> file on disk for the duration of the profile window,
/// and on <see cref="Stop"/> post-processes the file with <see cref="TraceLog"/>
/// to attribute managed allocations / CPU samples to their deepest named
/// stack frame and feed them into a <see cref="ProfileAggregator"/>.
/// </summary>
/// <remarks>
/// <para>
/// The session lives in-process, which means it works when the BDN run is
/// configured to use <c>InProcessEmitToolchain</c> (the default for the
/// <c>dry</c> and <c>quick</c> fidelity tiers). The <c>full</c> fidelity tier
/// spawns one child process per <c>[Benchmark]</c>, in which case the parent's
/// EventPipe session would not observe any workload activity; the harness
/// refuses profiling in that mode (see <see cref="BenchmarkProfiler.TryStart"/>).
/// </para>
/// <para>
/// The dump-then-symbolicate pattern (vs. live event-by-event symbol
/// resolution) is the documented robust path with TraceEvent: streaming
/// <see cref="EventPipeEventSource"/> events don't carry resolved stack
/// frames inline, whereas <see cref="TraceLog.OpenOrConvert(string)"/>
/// produces a fully cross-referenced ETLX with managed-method symbols once
/// the session's JIT rundown has flushed.
/// </para>
/// </remarks>
internal sealed class EventPipeProfilerSession : IDisposable
{
    /// <summary>
    /// EventPipe provider name for the .NET runtime's cross-platform CPU
    /// sample profiler. Constant rather than a type reference so we don't
    /// need to chase the parser type across TraceEvent namespace
    /// reorganisations.
    /// </summary>
    private const string SampleProfilerProviderName = "Microsoft-DotNETCore-SampleProfiler";

    private readonly ProfilerOptions _options;
    private readonly ProfileAggregator _aggregator;
    private readonly EventPipeSession _session;
    private readonly string _nettracePath;
    private readonly bool _nettraceIsTemp;
    private readonly Task _pumpTask;
    private int _disposed;

    private EventPipeProfilerSession(
        ProfilerOptions options,
        ProfileAggregator aggregator,
        EventPipeSession session,
        string nettracePath,
        bool nettraceIsTemp)
    {
        _options = options;
        _aggregator = aggregator;
        _session = session;
        _nettracePath = nettracePath;
        _nettraceIsTemp = nettraceIsTemp;
        _pumpTask = Task.Run(() => DumpToFile(_session.EventStream, _nettracePath));
    }

    /// <summary>
    /// Opens an EventPipe session against the current process configured by
    /// <paramref name="options"/>. Returns <see langword="null"/> when the
    /// session could not be opened (e.g. on a platform where EventPipe is
    /// unavailable, or when the diagnostics socket is missing).
    /// </summary>
    public static EventPipeProfilerSession? TryStart(ProfilerOptions options, ProfileAggregator aggregator)
    {
        if (!options.IsEnabled)
        {
            return null;
        }
        ArgumentNullException.ThrowIfNull(aggregator);

        try
        {
            var providers = BuildProviders(options);
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            var client = new DiagnosticsClient(pid);
            // requestRundown:true forces the runtime to emit JIT method-load
            // rundown on Stop so symbols for methods JITted before the
            // session started are still resolvable.
            var session = client.StartEventPipeSession(providers, requestRundown: true);

            // Where to write the live .nettrace blob: user-specified path if
            // given, else a temp file we delete on Stop.
            string nettracePath;
            bool nettraceIsTemp;
            if (!string.IsNullOrWhiteSpace(options.NetTraceOutputPath))
            {
                nettracePath = options.NetTraceOutputPath;
                nettraceIsTemp = false;
                var dir = Path.GetDirectoryName(nettracePath);
                if (!string.IsNullOrEmpty(dir))
                {
                    Directory.CreateDirectory(dir);
                }
            }
            else
            {
                nettracePath = Path.Combine(
                    Path.GetTempPath(),
                    $"orleans-lattice-microbench-{Guid.NewGuid():N}.nettrace");
                nettraceIsTemp = true;
            }

            return new EventPipeProfilerSession(options, aggregator, session, nettracePath, nettraceIsTemp);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[microbench] EventPipe session could not be opened: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Stops the live event session, waits for the dump pump to drain, then
    /// post-processes the resulting <c>.nettrace</c> via
    /// <see cref="TraceLog"/> to feed the aggregator. Safe to call from
    /// inside a <c>[GlobalCleanup]</c> handler. Bounded by
    /// <paramref name="drainTimeout"/>.
    /// </summary>
    public void Stop(TimeSpan drainTimeout)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        try
        {
            _session.Stop();
        }
        catch
        {
            // Stop after Dispose / lost socket is non-fatal.
        }
        try
        {
            _pumpTask.Wait(drainTimeout);
        }
        catch (AggregateException ae) when (ae.InnerException is TaskCanceledException or OperationCanceledException)
        {
            // expected on Stop
        }
        try
        {
            _session.Dispose();
        }
        catch { /* idempotent */ }

        try
        {
            PostProcessTrace();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[microbench] EventPipe post-processing failed: {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            if (_nettraceIsTemp)
            {
                try { File.Delete(_nettracePath); } catch { /* best effort */ }
            }
        }
    }

    /// <inheritdoc/>
    public void Dispose() => Stop(TimeSpan.FromSeconds(30));

    private static void DumpToFile(Stream eventStream, string path)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        eventStream.CopyTo(fs);
    }

    private void PostProcessTrace()
    {
        if (!File.Exists(_nettracePath))
        {
            return;
        }
        // OpenOrConvert lazily produces a sibling .etlx with cross-referenced
        // symbols on first read; subsequent loads of the same .nettrace path
        // reuse the .etlx as a cache. The .etlx sidecar is left next to the
        // .nettrace; if the .nettrace is temp it is cleaned up by the caller
        // and the .etlx becomes orphan, which we also delete.
        var etlxPath = TraceLog.CreateFromEventPipeDataFile(_nettracePath);
        try
        {
            using var traceLog = new TraceLog(etlxPath);
            // Walk the whole event stream once; both allocation and sample
            // handlers fire from the same source so we never double-process.
            var source = traceLog.Events.GetSource();

            if (_options.CapturesAllocations)
            {
                var clrParser = new ClrTraceEventParser(source);
                clrParser.GCSampledObjectAllocation += data =>
                {
                    try
                    {
                        // TotalSizeForTypeSample is the accumulated bytes
                        // since the previous sample of this type, which is
                        // the correct cost to attribute to the caller. The
                        // field is uint on this TraceEvent build, so a zero
                        // value means "no accumulator reported"; we fall
                        // back to a 1-byte placeholder so the frame still
                        // shows up in the top-list.
                        long bytes = (long)data.TotalSizeForTypeSample;
                        if (bytes <= 0)
                        {
                            bytes = 1L;
                        }
                        var (method, module) = ResolveDeepestManagedFrame(data);
                        _aggregator.RecordAllocation(method, module, bytes);
                    }
                    catch
                    {
                        // single bad event must not tank the pass
                    }
                };

                // GCAllocationTick fires every ~100 KB of managed allocation
                // across all threads and is the reliable allocation signal
                // over EventPipe (GCSampledObjectAllocationHigh is documented
                // by the runtime but ships near-empty on .NET 10 EventPipe
                // sessions, presumably because the sampled-allocator path is
                // ETW-only). The 0x1 GC keyword is already enabled by
                // BuildProviders. Attributing the full tick amount to the
                // deepest managed frame is the same shape as the sampled
                // handler above; on a healthy session this dominates.
                clrParser.GCAllocationTick += data =>
                {
                    try
                    {
                        long bytes = data.AllocationAmount64;
                        if (bytes <= 0)
                        {
                            bytes = data.AllocationAmount;
                        }
                        if (bytes <= 0)
                        {
                            return;
                        }
                        var (method, module) = ResolveDeepestManagedFrame(data);
                        _aggregator.RecordAllocation(method, module, bytes);
                    }
                    catch
                    {
                        // single bad event must not tank the pass
                    }
                };
            }

            if (_options.CapturesCpu)
            {
                source.AllEvents += data =>
                {
                    try
                    {
                        if (!string.Equals(data.ProviderName, SampleProfilerProviderName, StringComparison.Ordinal))
                        {
                            return;
                        }
                        if (!data.EventName.Contains("Sample", StringComparison.OrdinalIgnoreCase))
                        {
                            return;
                        }
                        var (method, module) = ResolveDeepestManagedFrame(data);
                        _aggregator.RecordSample(method, module);
                    }
                    catch
                    {
                        // ignore single-event failures
                    }
                };
            }

            source.Process();
        }
        finally
        {
            // Clean up the orphan .etlx if the .nettrace itself is temp.
            if (_nettraceIsTemp)
            {
                try { File.Delete(etlxPath); } catch { /* best effort */ }
            }
        }
    }

    private static List<EventPipeProvider> BuildProviders(ProfilerOptions options)
    {
        var list = new List<EventPipeProvider>(2);

        // GCSampledObjectAllocationHigh: 0x80000000  (sampled allocator)
        // Stack:                          0x40000000  (every event gets a stack)
        // Type:                           0x80        (so the parser knows type names)
        // GC:                             0x1         (delivers AllocationTick too on some runtimes)
        // Loader:                         0x8         (module rundown)
        // Jit + JittedMethodILToNativeMap: 0x10 | 0x20000 (method-load rundown)
        const long allocKeywords = 0x80000000L | 0x40000000L | 0x80L | 0x1L | 0x8L | 0x10L | 0x20000L;
        // Even when only CPU sampling is requested we still need
        // module/JIT rundown so stack symbolication works.
        const long rundownKeywords = 0x40000000L | 0x8L | 0x10L | 0x20000L;

        list.Add(new EventPipeProvider(
            name: ClrTraceEventParser.ProviderName,
            eventLevel: EventLevel.Verbose,
            keywords: options.CapturesAllocations ? allocKeywords : rundownKeywords));

        if (options.CapturesCpu)
        {
            list.Add(new EventPipeProvider(
                name: SampleProfilerProviderName,
                eventLevel: EventLevel.Informational,
                keywords: 0));
        }

        return list;
    }

    /// <summary>
    /// Resolves the most relevant named managed frame on the event's call
    /// stack, returning <c>(method, module)</c>.
    /// <para>
    /// When noise-frame filtering is enabled (the default, see
    /// <see cref="ProfilerOptions.FilterNoiseFrames"/>), the walker skips
    /// measurement-substrate frames (test mocks, the BDN engine, async-builder
    /// plumbing) per <see cref="FrameFilter.IsProductFrame"/> and attributes
    /// the cost to the nearest <em>product</em> frame instead. If the stack
    /// contains only noise / unresolved frames, it falls back to the deepest
    /// named managed frame so the cost is never silently dropped.
    /// </para>
    /// Falls back to <c>("[unknown]", "")</c> only when the stack is empty or
    /// contains no named managed frame at all.
    /// </summary>
    private (string Method, string Module) ResolveDeepestManagedFrame(TraceEvent data)
    {
        TraceCallStack? stack;
        try
        {
            stack = data.CallStack();
        }
        catch
        {
            return ("[unknown]", string.Empty);
        }

        // First named managed frame regardless of product/noise classification;
        // used as the fallback when filtering rejects the entire stack.
        (string Method, string Module)? deepestNamed = null;

        while (stack is not null)
        {
            var address = stack.CodeAddress;
            var fullName = address?.FullMethodName;
            if (!string.IsNullOrEmpty(fullName)
                && !fullName.StartsWith("UNMANAGED_CODE_TIME", StringComparison.Ordinal)
                && !fullName.StartsWith("?", StringComparison.Ordinal))
            {
                var module = address?.ModuleFile?.Name ?? string.Empty;
                deepestNamed ??= (fullName, module);

                if (!_options.FilterNoiseFrames || FrameFilter.IsProductFrame(fullName))
                {
                    return (fullName, module);
                }
            }
            stack = stack.Caller;
        }
        return deepestNamed ?? ("[unknown]", string.Empty);
    }
}
