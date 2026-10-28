using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

public partial class CrossTreeSiblingBoundaryIntegrationTests
{
    private sealed record NaturalPartition(
        int Partition, long Head, long Lowest, long? Watermark, long? ProviderWatermark,
        WalShardSequencedEntry[] Rows, bool CaptureLimitReached);

    private static IServiceProvider NaturalServices(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static void AddNaturalObservations(IServiceCollection services, string cluster)
    {
        var original = services.Single(item => item.ServiceType == typeof(ILatticeWalGc));
        services.AddSingleton(new NaturalObservationTrace(cluster));
        services.AddSingleton<ILoggerProvider>(provider => provider.GetRequiredService<NaturalObservationTrace>());
        services.Replace(ServiceDescriptor.Singleton<ILatticeWalGc>(provider =>
        {
            var inner = original.ImplementationInstance is ILatticeWalGc instance ? instance
                : original.ImplementationFactory is { } factory ? (ILatticeWalGc)factory(provider)
                : (ILatticeWalGc)ActivatorUtilities.CreateInstance(provider,
                    original.ImplementationType ?? throw new InvalidOperationException("The GC descriptor has no implementation."));
            return new ObservedNaturalGc(inner, provider);
        }));
    }

    private IDisposable BeginNaturalObservation(params string[] trees)
    {
        var traces = new[] { NaturalServices(_siteA), NaturalServices(_siteB) }
            .Select(services => services.GetRequiredService<NaturalObservationTrace>()).ToArray();
        foreach (var trace in traces)
        {
            trace.Begin(trees);
        }
        return new NaturalObservationScope(traces);
    }

    private async Task ObserveNaturalBoundaryAsync(string boundary, params string[] trees)
    {
        foreach (var cluster in new[] { _siteA, _siteB })
        {
            var services = NaturalServices(cluster);
            var trace = services.GetRequiredService<NaturalObservationTrace>();
            var factory = cluster.Client;
            foreach (var tree in trees)
            {
                var entry = await factory.GetLatticeRegistry().GetEntryAsync(tree);
                trace.Record("binding", new { boundary, tree, entry?.PhysicalTreeId, entry?.Lineage });
                if (entry is null)
                {
                    continue;
                }
                foreach (var log in new[] { tree, entry.PhysicalTreeId ?? tree }.Distinct(StringComparer.Ordinal))
                {
                    await ObserveNaturalConsumersAsync(services, factory, trace, boundary, log);
                }
                await CaptureNaturalLogAsync(services, factory, trace, boundary, entry.PhysicalTreeId ?? tree);
            }
        }
    }

    private static async Task ObserveNaturalConsumersAsync(
        IServiceProvider services, IGrainFactory factory, NaturalObservationTrace trace, string boundary, string physical)
    {
        var consumers = await factory.GetGrain<IWalOffsetConsumerRegistryGrain>(physical).GetConsumersAsync();
        trace.Record("consumer-registry", new { boundary, physical, consumers = consumers.Select(id => id.ToString()).ToArray() });
        foreach (var consumer in consumers)
        {
            // Never touch an unregistered shipper merely to observe whether it exists.
            var positions = await factory.GetGrain(consumer).AsReference<IWalOffsetConsumer>()
                .GetDurableReadPositionsAsync(physical);
            trace.Record("registered-consumer-positions", new { boundary, physical, consumer = consumer.ToString(), positions });
        }
        var options = services.GetRequiredService<IOptionsMonitor<LatticeOptions>>().Get(physical);
        trace.Record("retention-options", new { boundary, physical, options.WalGcInterval, options.WalRetention });
    }

    private static async Task<NaturalPartition[]> CaptureNaturalLogAsync(
        IServiceProvider services, IGrainFactory factory, NaturalObservationTrace trace, string boundary, string physical)
    {
        var count = await services.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(physical);
        var provider = services.GetRequiredService<IWalStorageProvider>();
        var result = new NaturalPartition[count];
        for (var partition = 0; partition < count; partition++)
        {
            var wal = factory.GetGrain<IWalShardGrain>($"{physical}/{partition}");
            var page = await wal.ReadAsync(0, 256, CancellationToken.None);
            result[partition] = new NaturalPartition(partition,
                await wal.GetNextSequenceAsync(CancellationToken.None),
                await wal.GetLowestRetainedSequenceAsync(CancellationToken.None),
                await wal.GetTrimWatermarkAsync(CancellationToken.None),
                await provider.GetTrimWatermarkAsync(physical, partition, CancellationToken.None),
                page.Entries.ToArray(), page.Entries.Count == 256);
        }
        trace.Record("wal-snapshot-not-atomic", new { boundary, physical, partitions = result });
        return result;
    }

    private sealed class ObservedNaturalGc(ILatticeWalGc inner, IServiceProvider services) : ILatticeWalGc
    {
        public async Task<LatticeWalGcReport> RunOnceAsync(string treeName, CancellationToken cancellationToken = default)
        {
            var trace = services.GetRequiredService<NaturalObservationTrace>();
            if (!trace.Includes(treeName))
            {
                return await inner.RunOnceAsync(treeName, cancellationToken);
            }
            var pass = Guid.NewGuid();
            var factory = services.GetRequiredService<IGrainFactory>();
            trace.Record("gc-enter", new { pass, treeName });
            try
            {
                await ObserveNaturalConsumersAsync(services, factory, trace, $"gc-before:{pass}", treeName);
                await CaptureNaturalLogAsync(services, factory, trace, $"gc-before:{pass}", treeName);
                var report = await inner.RunOnceAsync(treeName, cancellationToken);
                trace.Record("gc-return", new { pass, report });
                await ObserveNaturalConsumersAsync(services, factory, trace, $"gc-after:{pass}", treeName);
                await CaptureNaturalLogAsync(services, factory, trace, $"gc-after:{pass}", treeName);
                return report;
            }
            catch (Exception exception)
            {
                trace.Record("gc-or-observation-failed", new { pass, treeName, error = exception.ToString() });
                throw;
            }
        }
    }

    private sealed class NaturalObservationTrace(string cluster) : ILoggerProvider
    {
        private readonly ConcurrentQueue<string> _events = new();
        private string[] _trees = [];
        private long _sequence;

        public bool Includes(string tree) => Volatile.Read(ref _trees).Any(logical =>
            tree == logical || tree.StartsWith(logical + "/", StringComparison.Ordinal));

        public void Begin(string[] trees)
        {
            _events.Clear();
            Volatile.Write(ref _trees, trees.ToArray());
            Record("observation-begin", new
            {
                trees,
                limitation = "Awaited snapshots perturb scheduling and are not atomic; no registration timestamp is inferred from absence."
            });
        }

        public void Record(string kind, object value)
        {
            var text = JsonSerializer.Serialize(new
            {
                cluster, sequence = Interlocked.Increment(ref _sequence),
                monotonic = Stopwatch.GetTimestamp(), utc = DateTimeOffset.UtcNow, kind, value
            });
            _events.Enqueue(text);
            Console.WriteLine("owner179-natural:" + text);
        }

        public void End()
        {
            Record("observation-end", new { buffered = _events.Count });
            Volatile.Write(ref _trees, []);
            foreach (var item in _events)
            {
                TestContext.WriteLine("owner179-natural-retained:" + item);
            }
        }

        public ILogger CreateLogger(string categoryName) => new NaturalObservationLogger(this, categoryName);
        public void Dispose() { }

        private sealed class NaturalObservationLogger(NaturalObservationTrace trace, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel) || Volatile.Read(ref trace._trees).Length == 0
                    || category != typeof(ReplicationShipperGrain).FullName)
                {
                    return;
                }
                var fields = state is IEnumerable<KeyValuePair<string, object?>> structured
                    ? structured.ToDictionary(pair => pair.Key, pair => pair.Value)
                    : new Dictionary<string, object?>();
                var caller = "UNRESOLVED";
                if (fields.GetValueOrDefault("Requested") is long requested
                    && fields.GetValueOrDefault("FirstRetained") is long first && first > requested
                    && fields.GetValueOrDefault("{OriginalFormat}") is string format
                    && format.Contains("trimmed past the unshipped cursor", StringComparison.Ordinal))
                {
                    // The other MarkReseedRequired callers pass equal coordinates.
                    caller = "TryRefillPartitionAsync:strict-gap-callsite";
                }
                else if (fields.GetValueOrDefault("{OriginalFormat}") is string otherFormat)
                {
                    if (otherFormat.Contains("the source tree's lineage changed to", StringComparison.Ordinal))
                    {
                        caller = "ForceSourceLineageGapAsync:lineage-warning-callsite";
                    }
                    else if (otherFormat.Contains("replay cannot withhold it whole with its effects carried", StringComparison.Ordinal))
                    {
                        caller = "TryWithholdReplayedSagaAsync:purged-saga-warning-callsite";
                    }
                    else if (otherFormat.Contains("was trimmed past saga records withheld for the re-seed", StringComparison.Ordinal))
                    {
                        caller = "MaybeClearReseedAsync:retained-saga-gap-callsite";
                    }
                }
                trace.Record("shipper-warning", new
                {
                    category, eventId = eventId.Id, caller, fields,
                    text = formatter(state, exception), error = exception?.ToString(),
                    stack = new StackTrace().ToString()
                });
            }
        }
    }

    private sealed class NaturalObservationScope(NaturalObservationTrace[] traces) : IDisposable
    {
        public void Dispose()
        {
            foreach (var trace in traces)
            {
                trace.End();
            }
        }
    }
}
