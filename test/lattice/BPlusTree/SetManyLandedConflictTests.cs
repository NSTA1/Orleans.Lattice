using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.BPlusTree.PublicApiContract;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5095: a topology-grain write that lands but is reported as an etag
/// conflict (what a storage client's retry of a committed request looks like) must
/// be adopted rather than deactivating the grain, so an ascending
/// <see cref="ILattice.SetManyAsync"/> workload keeps completing; a conflict for a
/// write that did not land must still surface.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class SetManyLandedConflictTests
{
    private const string LandedTreeId = "landed-conflict-5095";
    private const string ProbePrefix = "seed-probe-5095-";
    private const int FaultEveryNthWrite = 9;

    private static readonly List<LandedThenConflictGrainStorage> Storages = [];
    private static volatile bool s_landedFaults;
    private static int s_topologyWrites;
    private static int s_landedProbeWrite;
    private static int s_unlandedProbeWrite;
    private static int s_divergedProbeWrite;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(LandedTreeId, new TreeRegistryEntry { MaxLeafKeys = 64, ShardCount = 4 });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        s_landedFaults = false;
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Ascending_SetMany_survives_landed_writes_reported_as_conflicts()
    {
        var tree = _cluster.Client.GetGrain<ILattice>(LandedTreeId);
        const int batches = 40;
        const int batchSize = 128;
        var injectedBefore = Injected();

        s_landedFaults = true;
        try
        {
            for (var b = 0; b < batches; b++)
            {
                var batch = Enumerable.Range(b * batchSize, batchSize)
                    .Select(i => KeyValuePair.Create($"k{i:D7}", BitConverter.GetBytes(i)))
                    .ToList();
                await tree.SetManyAsync(batch);
            }
        }
        finally
        {
            s_landedFaults = false;
        }

        Assert.That(Injected() - injectedBefore, Is.GreaterThan(10),
            "the fault injector must have reported landed writes as conflicts for the test to mean anything");
        Assert.That(await tree.CountAsync(), Is.EqualTo(batches * batchSize));
        var sample = Enumerable.Range(0, batches * batchSize).Where(i => i % 97 == 0).Select(i => $"k{i:D7}").ToList();
        var read = await tree.GetManyAsync(sample);
        Assert.That(read.Keys, Is.EquivalentTo(sample));
    }

    [Test]
    public async Task A_landed_write_reported_as_a_conflict_is_adopted_and_the_etag_stays_usable()
    {
        var probe = _cluster.Client.GetGrain<ITopologySeedPersistProbeGrain>(ProbePrefix + "landed");
        await probe.WriteAsync(1);
        var injectedBefore = Injected();

        Interlocked.Exchange(ref s_landedProbeWrite, 1);
        var adoptedETag = await probe.WriteAsync(2);

        Assert.That(Injected() - injectedBefore, Is.EqualTo(1));
        Assert.That(adoptedETag, Is.Not.Null.And.Not.Empty);
        Assert.That(await probe.WriteAsync(3), Is.Not.EqualTo(adoptedETag),
            "the adopted etag must be the committed one, or this follow-up write would conflict");
        Assert.That(await probe.ReadStoredAsync(), Is.EqualTo(3));
    }

    [Test]
    public async Task A_conflict_for_a_write_that_did_not_land_still_surfaces()
    {
        var probe = _cluster.Client.GetGrain<ITopologySeedPersistProbeGrain>(ProbePrefix + "unlanded");
        await probe.WriteAsync(1);
        var rejectedBefore = Storages.Sum(s => s.Rejected);

        Interlocked.Exchange(ref s_unlandedProbeWrite, 1);
        Assert.That(async () => await probe.WriteAsync(2), Throws.InstanceOf<InconsistentStateException>());

        Assert.That(Storages.Sum(s => s.Rejected) - rejectedBefore, Is.EqualTo(1));
        Assert.That(await _cluster.Client.GetGrain<ITopologySeedPersistProbeGrain>(ProbePrefix + "unlanded").ReadStoredAsync(),
            Is.EqualTo(1));
    }
    [Test]
    public async Task A_swallowed_conflict_that_cannot_be_adopted_retires_the_activation_so_later_writes_succeed()
    {
        // The wedge in an 8-silo set-many run: during a storage stall a topology
        // write landed, interleaved turns moved the in-memory state on, and the
        // conflict reported for the landed write could not be adopted. A
        // best-effort caller swallowed it, so the activation survived with a stale
        // etag and every later write it made failed the same way.
        var probe = _cluster.Client.GetGrain<ITopologySeedPersistProbeGrain>(ProbePrefix + "diverged");
        Assert.That(await probe.TryWriteSwallowingConflictAsync(1), Is.True);

        Interlocked.Exchange(ref s_divergedProbeWrite, 1);
        Assert.That(await probe.TryWriteSwallowingConflictAsync(2), Is.False,
            "the injected landed write whose state moved on must surface as a conflict");

        Assert.That(await probe.TryWriteSwallowingConflictAsync(3), Is.True,
            "a conflicted activation must be retired, so the next write reloads the row rather than reusing a stale etag");
        Assert.That(await probe.ReadStoredAsync(), Is.EqualTo(3));
    }

    private static long Injected() => Storages.Sum(s => s.Injected);

    private static bool IsProbe(GrainId grainId) => grainId.Key.ToString()!.StartsWith(ProbePrefix, StringComparison.Ordinal);

    private static bool DivergeProbe(GrainId grainId, object? state)
    {
        if (!IsProbe(grainId) || state is not InternalNodeState node || !TakeOneShot(ref s_divergedProbeWrite))
        {
            return false;
        }

        node.SubtreeEntryCount += 1_000;
        return true;
    }

    private static bool TakeOneShot(ref int flag) => Interlocked.CompareExchange(ref flag, 0, 1) == 1;

    private static bool IsTopologyState(string stateName) =>
        stateName is TopologySeedPersist.LeafStateName
            or TopologySeedPersist.InternalStateName
            or TopologySeedPersist.ShardRootStateName;

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) =>
                {
                    var storage = new LandedThenConflictGrainStorage(
                        new ProcessScopeMemoryGrainStorage(),
                        static (stateName, grainId) => IsProbe(grainId)
                            ? TakeOneShot(ref s_landedProbeWrite)
                            : s_landedFaults
                                && IsTopologyState(stateName)
                                && Interlocked.Increment(ref s_topologyWrites) % FaultEveryNthWrite == 0,
                        static (_, grainId) => IsProbe(grainId) && TakeOneShot(ref s_unlandedProbeWrite),
                        static (_, grainId, state) => DivergeProbe(grainId, state));
                    lock (Storages)
                    {
                        Storages.Add(storage);
                    }

                    return storage;
                }));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
