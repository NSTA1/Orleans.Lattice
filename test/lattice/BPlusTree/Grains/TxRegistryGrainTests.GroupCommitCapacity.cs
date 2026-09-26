using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Group-commit instruments and the persisted-state size budget (issue #3475).
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task MarkCommittedAsync_concurrent_calls_record_the_coalescing_factor_on_the_write_instruments()
    {
        var treeId = $"tx-registry-metrics-{Guid.NewGuid():N}";
        var writes = new List<(long value, string? outcome)>();
        var mutations = new List<int>();
        var durations = new List<double>();
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            [
                "orleans.lattice.tx_registry.writes",
                "orleans.lattice.tx_registry.write.mutations",
                "orleans.lattice.tx_registry.write.duration",
            ],
            l =>
            {
                l.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
                {
                    if (TagValue(tags, LatticeMetrics.TagTree) != treeId) return;
                    lock (writes) writes.Add((value, TagValue(tags, LatticeMetrics.TagOutcome)));
                });
                l.SetMeasurementEventCallback<int>((instrument, value, tags, _) =>
                {
                    if (TagValue(tags, LatticeMetrics.TagTree) != treeId) return;
                    lock (mutations) mutations.Add(value);
                });
                l.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
                {
                    if (TagValue(tags, LatticeMetrics.TagTree) != treeId) return;
                    lock (durations) durations.Add(value);
                });
            });

        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state, treeId);
        const int callers = 8;
        var calls = Enumerable.Range(0, callers)
            .Select(_ => OnTurnAsync(turn, () => grain.MarkCommittedAsync(Guid.NewGuid())))
            .ToArray();
        await DrainAsync(turn);
        gate.SetResult();
        await Task.WhenAll(calls);

        Assert.Multiple(() =>
        {
            Assert.That(writes, Has.Count.EqualTo(2), "One gated write plus one write carrying everything queued behind it.");
            Assert.That(writes.Select(w => w.outcome), Is.All.EqualTo("ok"));
            Assert.That(mutations, Is.EqualTo(new[] { 1, callers - 1 }), "The second write must carry every mutation that arrived while the first was outstanding.");
            Assert.That(durations, Has.Count.EqualTo(2));
            Assert.That(durations, Is.All.GreaterThanOrEqualTo(0d));
        });
    }

    [Test]
    public async Task MarkCommittedAsync_failed_write_records_a_fault_outcome()
    {
        var treeId = $"tx-registry-metrics-{Guid.NewGuid():N}";
        var outcomes = new List<string?>();
        using var listener = MeterListening.StartForMeter(
            LatticeMetrics.Meter,
            ["orleans.lattice.tx_registry.writes"],
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                if (TagValue(tags, LatticeMetrics.TagTree) != treeId) return;
                lock (outcomes) outcomes.Add(TagValue(tags, LatticeMetrics.TagOutcome));
            }));

        var (grain, state) = CreateGrain(treeId: treeId);
        state.ThrowOnWrite = new InvalidOperationException("storage down");
        Assert.ThrowsAsync<TxRegistryWriteFailedException>(() => grain.MarkCommittedAsync(Guid.NewGuid()));
        await grain.MarkCommittedAsync(Guid.NewGuid());

        Assert.That(outcomes, Is.EqualTo(new[] { "fault", "ok" }));
    }

    /// <summary>
    /// Measures the persisted registry row at the steady-state size the
    /// default <see cref="LatticeOptions.TxDecisionRetention"/> of 60 s implies
    /// at a group-commit target of about 200 sagas/s: roughly 12,000 retained
    /// tombstones. The registry state is not an
    /// <c>ILatticeBinaryPersistedState</c>, so production persists it through
    /// the JSON grain-storage serializer, and that is what is measured. The
    /// Azure Table grain-state limit is about 1 MB per row.
    /// <para>
    /// Measured: about 116 JSON bytes per retained tombstone, so 12,000
    /// tombstones serialise to about 1.39 MB and the row crosses 1 MB near
    /// 8,600 tombstones - about 143 sagas/s sustained at the default 60 s
    /// retention. Group commit adds no bytes per saga; it only makes that
    /// rate reachable. This is a documented capacity ceiling tracked against
    /// issue #3475, not a property the test can assert away, so the test pins
    /// the per-tombstone cost: a regression there lowers the ceiling silently.
    /// </para>
    /// </summary>
    [Test]
    public void TxRegistryState_serialised_bytes_per_retained_tombstone_stay_bounded()
    {
        const int tombstones = 12_000;
        var registry = new TxRegistryState();
        var forgottenAt = DateTimeOffset.UtcNow;
        for (var i = 0; i < tombstones; i++)
        {
            var txid = Guid.NewGuid();
            registry.Decisions[txid] = i % 10 == 0 ? TxStatus.Aborted : TxStatus.Committed;
            registry.ForgottenAt[txid] = forgottenAt.AddMilliseconds(i);
        }

        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddSingleton<OrleansJsonSerializer>();
        services.AddSingleton<JsonGrainStorageSerializer>();
        using var provider = services.BuildServiceProvider();
        var json = provider.GetRequiredService<JsonGrainStorageSerializer>();
        var binary = provider.GetRequiredService<Serializer<TxRegistryState>>();

        var started = Stopwatch.GetTimestamp();
        var jsonBytes = json.Serialize(registry).ToMemory().Length;
        var jsonElapsed = Stopwatch.GetElapsedTime(started);
        var binaryBytes = binary.SerializeToArray(registry).Length;

        var perTombstone = (double)jsonBytes / tombstones;
        var ceilingTombstones = (int)(1_000_000 / perTombstone);
        TestContext.Out.WriteLine(
            $"TxRegistryState with {tombstones} tombstones: JSON {jsonBytes:N0} bytes ({perTombstone:F1} per tombstone, serialised in {jsonElapsed.TotalMilliseconds:F1} ms), binary {binaryBytes:N0} bytes. "
            + $"A 1 MB row holds about {ceilingTombstones:N0} tombstones, about {ceilingTombstones / 60:N0} sagas/s at 60 s retention.");
        Assert.Multiple(() =>
        {
            Assert.That(perTombstone, Is.LessThanOrEqualTo(128d),
                "Each retained tombstone costs about 116 JSON bytes; growth lowers the sustainable saga rate before the ~1 MB row limit.");
            Assert.That(binaryBytes, Is.LessThan(jsonBytes));
        });
    }

    private static string? TagValue(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == key) return tag.Value as string;
        }
        return null;
    }
}
