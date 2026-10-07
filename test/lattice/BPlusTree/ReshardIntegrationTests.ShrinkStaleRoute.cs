using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4654 against a reshard shrink: a leaf of a retired shard is cleared on
/// purpose and has no state row, so a direct data operation on it fails closed.
/// No live reader may meet that failure: a read through a stale route - the
/// facade's cached pre-shrink map, a read in flight across a fold, or a caller
/// addressing a retired shard root directly - is redirected (stale routing) and
/// re-routed, never served a lost-leaf fault and never a wrong value.
/// </summary>
public partial class ReshardIntegrationTests
{
    [Test]
    public async Task Reads_through_stale_routes_during_a_shrink_are_re_routed_and_never_meet_a_cleared_retired_leaf()
    {
        var treeId = $"reshard-shrink-stale-{Guid.NewGuid():N}";
        await RegisterTreeAsync(treeId);
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var expected = await PopulateAsync(tree, "s", 200);
        var sample = expected.Keys.Take(16).ToList();
        var failures = new List<string>();

        async Task ReadEverythingAsync(string when)
        {
            foreach (var (key, value) in expected)
            {
                try
                {
                    var got = await tree.GetAsync(key);
                    if (got is null || !got.AsSpan().SequenceEqual(value))
                        failures.Add($"{when}: {key} read {(got is null ? "null" : Encoding.UTF8.GetString(got))}");
                }
                catch (Exception ex)
                {
                    failures.Add($"{when}: {key} faulted {ex.GetType().Name}: {ex.Message}");
                }
            }

            // A caller holding the pre-shrink map addresses every original shard
            // root directly. Each may serve the key or redirect it; none may
            // surface the cleared leaf of a retired shard.
            for (int idx = 0; idx < FourShardClusterFixture.TestShardCount; idx++)
            {
                var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{idx}");
                foreach (var key in sample)
                {
                    try
                    {
                        await shard.GetAsync(key);
                    }
                    catch (Exception ex) when (ex is ILatticeLeafUnavailable)
                    {
                        failures.Add($"{when}: shard {idx} surfaced {ex.GetType().Name} for {key}: {ex.Message}");
                    }
                    catch (Exception)
                    {
                        // A stale-routing refusal or similar redirect is the
                        // expected answer for a shard that no longer owns the key.
                    }
                }
            }
        }

        await tree.ReshardAsync(2);

        // A reader runs concurrently with the folds, so reads are in flight
        // across every routing swap, not only between passes.
        using var stop = new CancellationTokenSource();
        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await ReadEverythingAsync("concurrent");
            }
        });

        await DriveShrinkToCompletionAsync(treeId, () => ReadEverythingAsync("between passes"));
        stop.Cancel();
        await reader;
        await ReadEverythingAsync("after the shrink");

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures.Take(20)));
    }
}
