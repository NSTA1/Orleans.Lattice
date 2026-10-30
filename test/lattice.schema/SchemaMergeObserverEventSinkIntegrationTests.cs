using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// End-to-end detectors for the schema merge-result observer's event sink (issue
/// #5242): with <see cref="LatticeSchemaEnforcementOptions.ValidateCrdtMergeResults"/>
/// on, a merge whose result fails the tree's policy is counted on
/// <c>orleans.lattice.leaf.merge_observer_events</c> under the tree's tag and
/// logged once with the policy's reason, and the merge itself is unchanged.
/// </summary>
/// <remarks>
/// The violation is the one a pre-merge delta check cannot catch: two OR-Set adds,
/// each of whose delta satisfies a maximum-length rule, merge into a state that
/// exceeds it. The limit is derived from a control tree that carries no policy, so
/// the test does not depend on the OR-Set serialization's exact byte counts.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class SchemaMergeObserverEventSinkIntegrationTests
{
    private const string MessageMarker = "orleans.lattice.leaf.merge_observer_events";
    private const string ReplicaId = "replica-a";

    private static readonly byte[] First = Enumerable.Repeat((byte)'a', 200).ToArray();
    private static readonly byte[] Second = Enumerable.Repeat((byte)'b', 200).ToArray();

    private static readonly ConcurrentQueue<(LogLevel Level, string Message, IReadOnlyDictionary<string, object?> State)> Logs = new();

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<Configurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private ILatticeSchemaAdmin Admin =>
        _cluster.Silos.OfType<InProcessSiloHandle>().Single().SiloHost.Services.GetRequiredService<ILatticeSchemaAdmin>();

    private ILattice Tree(string treeId) => _cluster.Client.GetGrain<ILattice>(treeId);

    private static List<(LogLevel Level, string Message, IReadOnlyDictionary<string, object?> State)> EventLogs(string treeId) =>
        Logs.Where(e => e.Message.Contains(MessageMarker, StringComparison.Ordinal)
            && Equals(e.State.GetValueOrDefault("TreeId"), treeId)).ToList();

    /// <summary>
    /// The stored length of a two-element OR-Set built from the same adds on a tree
    /// with no policy. A limit one byte below it admits each single-element delta
    /// and rejects the two-element merged state.
    /// </summary>
    private async Task<int> TwoElementStateLengthAsync(string treeId)
    {
        var control = Tree(treeId + "-control");
        await control.OrSet("k").AddAsync(First, ReplicaId);
        await control.OrSet("k").AddAsync(Second, ReplicaId);
        var stored = await control.GetAsync("k");
        Assert.That(stored, Is.Not.Null);
        return stored!.Length;
    }

    private static async Task<long> CountEventsAsync(string treeId, Func<Task> body)
    {
        long count = 0;
        using var listener = MeterListening.StartForInstrument(LatticeMetrics.LeafMergeObserverEvents, l =>
            l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, treeId))
                        Interlocked.Add(ref count, value);
                }
            }));

        await body();
        return Interlocked.Read(ref count);
    }

    [Test]
    public async Task A_merge_result_violation_is_counted_and_logged_once_and_the_merge_is_kept()
    {
        const string treeId = "merge-sink-violation";
        var limit = await TwoElementStateLengthAsync(treeId) - 1;
        await Admin.SetPolicyAsync(treeId, new LatticeSchemaPolicy([LatticeSchemaRule.MaxLength(limit)]));
        var set = Tree(treeId).OrSet("k");

        var events = await CountEventsAsync(treeId, async () =>
        {
            await set.AddAsync(First, ReplicaId);
            await set.AddAsync(Second, ReplicaId);
        });

        var logs = EventLogs(treeId);
        var observed = await set.GetAsync();
        Assert.Multiple(() =>
        {
            Assert.That(events, Is.EqualTo(1),
                "only the second add's merged state exceeds the limit, so exactly one event is counted under the tree tag");
            Assert.That(logs, Has.Count.EqualTo(1), "one violation logs exactly one entry");
            Assert.That(logs.Single().Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(logs.Single().State.GetValueOrDefault("MergeMode"), Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(logs.Single().State.GetValueOrDefault("Reason") as string, Is.Not.Null.And.Not.Empty,
                "the log carries the policy's validation reason that the observer returned");
            Assert.That(observed.Contains(First) && observed.Contains(Second), Is.True,
                "a merge-result violation never blocks or changes the merge");
        });
    }

    [Test]
    public async Task A_compliant_merge_result_is_neither_counted_nor_logged()
    {
        const string treeId = "merge-sink-compliant";
        var limit = await TwoElementStateLengthAsync(treeId);
        await Admin.SetPolicyAsync(treeId, new LatticeSchemaPolicy([LatticeSchemaRule.MaxLength(limit)]));
        var set = Tree(treeId).OrSet("k");

        var events = await CountEventsAsync(treeId, async () =>
        {
            await set.AddAsync(First, ReplicaId);
            await set.AddAsync(Second, ReplicaId);
        });

        Assert.Multiple(() =>
        {
            Assert.That(events, Is.Zero, "a merged state within the limit is not an event");
            Assert.That(EventLogs(treeId), Is.Empty, "a merged state within the limit logs nothing");
        });
    }

    private sealed class Configurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddLatticeSchemaEnforcement(o => o.ValidateCrdtMergeResults = true);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<ILoggerProvider, CapturingLoggerProvider>();
        }
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new CapturingLogger();

        public void Dispose()
        {
            // Nothing to release.
        }

        private sealed class CapturingLogger : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(
                LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel))
                    return;

                var values = new Dictionary<string, object?>(StringComparer.Ordinal);
                if (state is IReadOnlyList<KeyValuePair<string, object?>> pairs)
                {
                    foreach (var pair in pairs)
                        values[pair.Key] = pair.Value;
                }

                Logs.Enqueue((logLevel, formatter(state, exception), values));
            }
        }
    }
}
