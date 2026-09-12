using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <c>tree</c> tag this grain contributes to
/// <see cref="LatticeMetrics.CoordinatorPhaseTickFailures"/>.
/// <para>
/// The grain key is a logical <em>index</em> name, not a tree id, so the base
/// coordinator's default would report a value that names no tree in the
/// cluster. The reconciler's repairs land in the sibling index tree
/// <c>tag-{indexName}</c>, and that is the tree the counter names: a sweep
/// spans every covered subject tree, so no single subject tree could stand in
/// for it.
/// </para>
/// </summary>
public partial class TagIndexReconcileGrainTests
{
    private sealed record TickMeasurement(long Value, string? Tree);

    /// <summary>
    /// The callback the grain handed the timer registry when the sweep armed
    /// its work-pump. Invoking it is what makes a failure a real exception
    /// travelling the real phase-tick handler.
    /// </summary>
    private static Func<CancellationToken, Task> CapturedTick(ITimerRegistry registry) =>
        (Func<CancellationToken, Task>)registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer))
            .GetArguments()[2]!;

    private static async Task<List<TickMeasurement>> RecordAsync(Func<Task> body)
    {
        var measurements = new List<TickMeasurement>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.CoordinatorPhaseTickFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        tree = tag.Value?.ToString();
                    }
                }

                lock (measurements)
                {
                    measurements.Add(new TickMeasurement(value, tree));
                }
            }));

        await body();

        lock (measurements)
        {
            return [.. measurements];
        }
    }

    /// <summary>
    /// Arms the sweep through the production schedule reminder, then fails the
    /// state write a probe chunk performs. Persistence is the right injection
    /// point: the sweep's defensive arms cover an unreadable subject tree, so a
    /// storage fault is exactly the unexpected residue that reaches the base
    /// class's handler.
    /// </summary>
    private static async Task<List<TickMeasurement>> ArmAndFailOnceAsync()
    {
        var harness = CreateSweepGrain();
        harness.IndexTree.Data["\0covered\0tree-a"] = [1];

        return await RecordAsync(async () =>
        {
            await harness.Grain.ReceiveReminder("tag-index-reconcile-schedule", new TickStatus());
            harness.State.ThrowOnWrite = new InvalidOperationException("injected phase-tick fault");
            await CapturedTick(harness.Timers)(CancellationToken.None);
        });
    }

    [Test]
    public async Task A_reconcile_phase_tick_failure_names_the_index_tree()
    {
        var measurements = await ArmAndFailOnceAsync();

        Assert.That(
            measurements.Where(m => m.Value == 1).Select(m => m.Tree).ToArray(),
            Is.EqualTo(new[] { IndexTreeId }).AsCollection,
            "A reconciler failure must be attributable to the tree its repairs land in.");
    }

    [Test]
    public async Task The_bare_index_name_never_reaches_the_tree_tag()
    {
        // The negative half. A bare index name is a plausible single token, so
        // unlike the shipper's composite it is not legible as wrong at any
        // point an operator or a gate would look - which is exactly why it
        // needs an assertion rather than a reading.
        var measurements = await ArmAndFailOnceAsync();

        Assert.Multiple(() =>
        {
            Assert.That(measurements, Is.Not.Empty,
                "The counter must emit at all, or this assertion is vacuous.");
            Assert.That(measurements.Select(m => m.Tree), Has.None.EqualTo(IndexName),
                "'test-index' names no tree in the cluster; the index tree is 'tag-test-index'.");
        });
    }

    [Test]
    public async Task The_zero_prime_and_the_failure_agree_on_the_tree()
    {
        // A prime tagged differently from the failure exports a permanent zero
        // beside a series that appears from nowhere on the first failure.
        var measurements = await ArmAndFailOnceAsync();

        Assert.Multiple(() =>
        {
            Assert.That(measurements.First(m => m.Value == 0).Tree, Is.EqualTo(IndexTreeId));
            Assert.That(measurements.First(m => m.Value == 1).Tree, Is.EqualTo(IndexTreeId));
        });
    }
}
