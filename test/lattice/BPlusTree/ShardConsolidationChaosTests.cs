using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos coverage for online shard consolidation: folds running against a tree
/// that is simultaneously being split, written to, and reactivated underneath
/// them.
/// <para>
/// The targeted integration suite exercises a fold against a quiet tree. This
/// fixture is the adversarial counterpart, because the deployment
/// consolidation exists to heal is by definition neither quiet nor stable: it
/// is a tree the splitter is still shattering, under sustained ingest, with
/// shards deactivating and reactivating under memory pressure. The properties
/// asserted are the same two that make a fold safe at all - no key is ever
/// unreachable and no acknowledged write is lost - held under interleavings
/// the targeted tests cannot produce.
/// </para>
/// <para>
/// CI-only: this fixture deliberately runs long and produces genuinely
/// concurrent interleavings, so it is excluded from the local development loop
/// by its category.
/// </para>
/// </summary>
[TestFixture]
[Category("Chaos")]
public class ShardConsolidationChaosTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ShardMap> GetMapAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.GetShardMapAsync(treeId)
            ?? ShardMap.CreateDefault(
                LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);
    }

    private async Task<bool> TryFoldAsync(string treeId, int donor, int survivor, CancellationToken ct)
    {
        var coordinator = _cluster.GrainFactory
            .GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{donor}");
        try
        {
            await coordinator.StartAsync(survivor);
        }
        catch (InvalidOperationException)
        {
            // Refused because a split holds the shard, the pair is not
            // adjacent under the map as it now stands, or the donor is already
            // retired. All are legitimate outcomes under concurrent topology
            // churn, and all must leave the tree intact - which is what the
            // reader and writer workers are asserting throughout.
            return false;
        }

        for (var i = 0; i < 200 && !ct.IsCancellationRequested; i++)
        {
            if (await coordinator.IsIdleAsync()) return true;
            await coordinator.RunConsolidationPassAsync();
        }

        return await coordinator.IsIdleAsync();
    }

    [Test]
    public async Task Consolidation_under_split_pressure_write_load_and_reactivation_loses_nothing()
    {
        var treeId = $"cons-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physicalTreeId = await _cluster.GrainFactory
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeId);

        // Seed a corpus wide enough to span every virtual slot stripe.
        var seeded = new ConcurrentDictionary<string, byte[]>();
        for (var i = 0; i < 600; i++)
        {
            var key = $"seed-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"seed-value-{i}");
            await tree.SetAsync(key, value);
            seeded[key] = value;
        }

        var acknowledged = new ConcurrentDictionary<string, byte[]>();
        var failures = new ConcurrentBag<string>();
        var reads = 0;
        var writes = 0;
        var folds = 0;
        var splits = 0;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var ct = cts.Token;

        // Readers: every seeded key must be reachable at every instant.
        var readers = Enumerable.Range(0, 4).Select(worker => Task.Run(async () =>
        {
            var rng = new Random(unchecked(worker * 104729 + 7));
            var keys = seeded.Keys.ToArray();
            while (!ct.IsCancellationRequested)
            {
                var key = keys[rng.Next(keys.Length)];
                try
                {
                    var actual = await tree.GetAsync(key, ct);
                    if (actual is null)
                        failures.Add($"reader: key '{key}' was unreachable during topology churn.");
                    else if (!actual.AsSpan().SequenceEqual(seeded[key]))
                        failures.Add($"reader: key '{key}' returned a wrong value during topology churn.");
                    Interlocked.Increment(ref reads);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"reader faulted on '{key}': {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct)).ToArray();

        // Writers: every acknowledged write must survive to the end.
        const int WriterCount = 3;
        var writers = Enumerable.Range(0, WriterCount).Select(worker => Task.Run(async () =>
        {
            var i = 0;
            while (!ct.IsCancellationRequested)
            {
                var key = $"chaos-w{worker}-{i:D6}";
                var value = Encoding.UTF8.GetBytes($"w{worker}-v{i}");
                try
                {
                    await tree.SetAsync(key, value, ct);
                    acknowledged[key] = value;
                    Interlocked.Increment(ref writes);
                    i++;
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"writer faulted on '{key}': {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct)).ToArray();

        // Reactivator: force shard roots to cycle so folds run across
        // activation boundaries and resume from persisted state.
        var reactivator = Task.Run(async () =>
        {
            var rng = new Random(31337);
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var indices = (await GetMapAsync(treeId)).GetPhysicalShardIndices();
                    if (indices.Count > 0)
                    {
                        var index = indices[rng.Next(indices.Count)];
                        await _cluster.GrainFactory
                            .GetGrain<IShardRootGrain>($"{physicalTreeId}/{index}").ForceDeactivateAsync();
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(750), ct);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"reactivator faulted: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct);

        // Splitter: keep shattering the tree so folds contend with the very
        // operation they are undoing.
        var splitter = Task.Run(async () =>
        {
            var rng = new Random(90210);
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var indices = (await GetMapAsync(treeId)).GetPhysicalShardIndices();
                    if (indices.Count > 0)
                    {
                        var source = indices[rng.Next(indices.Count)];
                        var split = _cluster.GrainFactory
                            .GetGrain<ITreeShardSplitGrain>($"{treeId}/{source}");
                        await split.SplitAsync(source);
                        await split.RunSplitPassAsync();
                        Interlocked.Increment(ref splits);
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                }
                catch (OperationCanceledException)
                {
                }
                catch (InvalidOperationException)
                {
                    // A shard already splitting, or too small to split, is an
                    // expected refusal under churn.
                }
                catch (Exception ex)
                {
                    failures.Add($"splitter faulted: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct);

        // Folder: repeatedly heal the cheapest adjacent pair the planner picks.
        var folder = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var map = await GetMapAsync(treeId);
                    if (!ShardConsolidationPlanner.TryPlanNext(map, out var plan))
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
                        continue;
                    }

                    if (await TryFoldAsync(treeId, plan.DonorShardIndex, plan.SurvivorShardIndex, ct))
                        Interlocked.Increment(ref folds);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"folder faulted: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct);

        await Task.WhenAll(readers.Concat(writers).Concat([reactivator, splitter, folder]));

        Assert.Multiple(() =>
        {
            Assert.That(reads, Is.GreaterThan(0), "The chaos readers must actually have run.");
            Assert.That(writes, Is.GreaterThan(0), "The chaos writers must actually have run.");
            Assert.That(folds, Is.GreaterThan(0), "At least one fold must have completed under churn.");
            Assert.That(splits, Is.GreaterThan(0), "The splitter must actually have contended with the folder.");
        });

        Assert.That(failures, Is.Empty,
            "Chaos failures:" + Environment.NewLine + string.Join(Environment.NewLine, failures.Take(20)));

        // Final settle: every seeded key and every acknowledged write must be
        // readable with its exact value, exactly once.
        foreach (var (key, value) in seeded)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Seeded key '{key}' was lost across the chaos run.");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Seeded key '{key}' has the wrong final value.");
        }

        foreach (var (key, value) in acknowledged)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Acknowledged write '{key}' was lost across the chaos run.");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Acknowledged write '{key}' has the wrong final value.");
        }

        // No key may be duplicated or dropped. The lower bound is exact - every
        // seeded key and every acknowledged write is asserted present above.
        // The upper bound carries a deliberate slack of one write per writer,
        // because a write can commit server-side and then have its
        // bookkeeping skipped when the writer's OperationCanceledException
        // fires at the cutoff: `acknowledged[key] = value` runs only after
        // SetAsync returns, so at the instant the token trips each writer may
        // have exactly one landed-but-unrecorded key in flight. That is a
        // property of this fixture's bookkeeping, not of the tree, and the
        // slack is bounded by the writer count rather than being open-ended,
        // so a real duplication or drop still fails here.
        Assert.That(await tree.CountAsync(),
            Is.InRange(seeded.Count + acknowledged.Count, seeded.Count + acknowledged.Count + WriterCount),
            "No key may be duplicated or dropped by the interleaved splits and folds.");
    }

    [Test]
    public async Task Folds_and_splits_never_leave_a_virtual_slot_unrouted()
    {
        // The routing-level invariant behind "no key is ever unreachable":
        // however splits and folds interleave, the map must stay total.
        var treeId = $"cons-chaos-map-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 200; i++)
            await tree.SetAsync($"mk-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var failures = new ConcurrentBag<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var ct = cts.Token;

        var observer = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var map = await GetMapAsync(treeId);
                    if (map.Slots.Length == 0)
                    {
                        failures.Add("The routing map became empty mid-churn.");
                        continue;
                    }
                    for (var slot = 0; slot < map.Slots.Length; slot++)
                    {
                        if (map.Slots[slot] < 0)
                            failures.Add($"Virtual slot {slot} lost its owner mid-churn.");
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"observer faulted: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct);

        var churner = Task.Run(async () =>
        {
            var rng = new Random(4242);
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var map = await GetMapAsync(treeId);
                    if (rng.Next(2) == 0)
                    {
                        var indices = map.GetPhysicalShardIndices();
                        var source = indices[rng.Next(indices.Count)];
                        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{source}");
                        await split.SplitAsync(source);
                        await split.RunSplitPassAsync();
                    }
                    else if (ShardConsolidationPlanner.TryPlanNext(map, out var plan))
                    {
                        await TryFoldAsync(treeId, plan.DonorShardIndex, plan.SurvivorShardIndex, ct);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (InvalidOperationException)
                {
                    // Expected refusals under contention.
                }
                catch (Exception ex)
                {
                    failures.Add($"churner faulted: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }, ct);

        await Task.WhenAll(observer, churner);

        Assert.That(failures, Is.Empty,
            "Routing failures:" + Environment.NewLine + string.Join(Environment.NewLine, failures.Take(20)));

        for (var i = 0; i < 200; i++)
        {
            Assert.That(await tree.GetAsync($"mk-{i:D4}"), Is.Not.Null,
                $"Key 'mk-{i:D4}' was unreachable after interleaved splits and folds.");
        }
    }
}
