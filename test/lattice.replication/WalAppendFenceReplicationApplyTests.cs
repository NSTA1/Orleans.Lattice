using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Detector for the WAL append fence's replication-apply seam (epic #5605, design
/// "Production members" item 1; review S5): a replicated mutation applied while the
/// target leaf's activation is being superseded is retried at the seam against the
/// live activation, so the apply never fails - the dead-letter decorator records no
/// failure, parks nothing, and the entry is applied as 10.2.0 applies it.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[Category("Integration")]
public sealed class WalAppendFenceReplicationApplyTests(bool eagerClaims)
{
    private const string LocalCluster = "site-a";
    private const string OriginCluster = "remote";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        if (eagerClaims)
        {
            builder.AddSiloBuilderConfigurator<EagerAppendFenceSiloConfigurator>();
        }

        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = LocalCluster);
        }
    }

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    [Test]
    public async Task A_replicated_apply_on_a_superseded_activation_records_no_failure()
    {
        const string Tree = "fence-replication-apply";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(Tree, new TreeRegistryEntry { MaxLeafKeys = 1024, MaxInternalChildren = 16, ShardCount = 1 });
        var tree = _cluster.Client.GetGrain<ILattice>(Tree);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));
        var leaf = new Guid(SHA256.HashData(Encoding.UTF8.GetBytes($"{Tree}/0")).AsSpan(0, 16));

        // Eager mode: the live activation is superseded by a newer epoch, so the apply's
        // first append is refused and the seam retries. Pre-arm mode (#5833): another
        // activation arms instead, which refuses nothing; the apply's pre-arm append is
        // admitted and attributed to the live activation, as foreign to the arming one.
        var superseding = eagerClaims ? await SupersedeAsync(Tree, leaf) : 0;
        var other = Guid.NewGuid();
        var beforeApply = eagerClaims ? [] : await ArmAnotherActivationAsync(Tree, leaf, other);

        var options = new LatticeReplicationOptions { ClusterId = LocalCluster };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);
        var spy = new FailureSpy(new ReplicationApplier(_cluster.GrainFactory, monitor, replicationContext: new OverridesReplicationContext()));
        var decorator = new DeadLetterTrackingReplicationApplier(
            spy, _cluster.GrainFactory, monitor, NullLogger<DeadLetterTrackingReplicationApplier>.Instance);

        var parked = 0;
        using var listener = MeterListening.StartForInstrument(LatticeReplicationMetrics.DeadLetterEnqueued, l =>
            l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree && Equals(tag.Value, Tree))
                    {
                        Interlocked.Add(ref parked, (int)value);
                    }
                }
            }));

        var result = await decorator.ApplyAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "replicated",
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = OriginCluster,
        }, CancellationToken.None);

        var applied = await tree.GetAsync("replicated");
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf);
        var reported = false;
        for (var partition = 0; partition < beforeApply.Length; partition++)
        {
            var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{Tree}/{partition}");
            if (await shard.GetNextSequenceAsync(CancellationToken.None) == beforeApply[partition])
            {
                continue;
            }

            var rearm = await shard.ArmAppendEpochAsync(leaf, 1, other, WalRoutingInitiator.None, 0, CancellationToken.None);
            reported |= rearm.Accepted && rearm.ForeignHigh >= beforeApply[partition];
        }
        Assert.Multiple(() =>
        {
            Assert.That(spy.Failures, Is.Empty, "the inner apply never failed, so the decorator counted no failure");
            Assert.That(result.Applied, Is.True);
            Assert.That(Volatile.Read(ref parked), Is.Zero, "nothing was dead-lettered");
            Assert.That(applied is null ? null : Encoding.UTF8.GetString(applied), Is.EqualTo("v"));
            if (eagerClaims)
            {
                Assert.That(row.State.Epoch, Is.GreaterThan(superseding),
                    "anti-vacuity: the apply met the superseded activation, and its seam retry reached a fresh one");
            }
            else
            {
                Assert.That(row.RecordExists, Is.False, "the live, unarmed activation claimed to apply");
                Assert.That(reported, Is.True, "the apply's pre-arm append was not attributed as foreign to the arming activation");
            }
        });
    }

    /// <summary>Arms another activation of the leaf at every partition (pre-arm mode); returns each partition's next offset after it.</summary>
    private async Task<long[]> ArmAnotherActivationAsync(string treeName, Guid leaf, Guid activation)
    {
        var partitions = Math.Max(1, await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(treeName));
        var before = new long[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{treeName}/{partition}");
            var arm = await shard.ArmAppendEpochAsync(leaf, 1, activation, WalRoutingInitiator.None, 0, CancellationToken.None);
            Assert.That(arm.Accepted, Is.True, $"precondition: the other activation armed at partition {partition}");
            before[partition] = await shard.GetNextSequenceAsync(CancellationToken.None);
        }

        return before;
    }

    /// <summary>Test-only (#5833): holds the pre-arm class closed, so the leaves claim eagerly.</summary>
    private sealed class EagerAppendFenceSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) =>
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<IWalAppendFenceGate>(
                sp => new EagerGate(ActivatorUtilities.CreateInstance<ClusterManifestWalAppendFenceGate>(sp))));

        private sealed class EagerGate(IWalAppendFenceGate inner) : IWalAppendFenceGate
        {
            public bool IsOpen => inner.IsOpen;

            public bool IsPreArmOpen => false;
        }
    }

    /// <summary>Supersedes the leaf's live activation as a newer activation would.</summary>
    private async Task<long> SupersedeAsync(string treeName, Guid leaf)
    {
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leaf);
        Assert.That(row.State.Epoch, Is.GreaterThan(0), "precondition: the live activation claimed an epoch with the gate open");
        var superseding = row.State.Epoch + 1;
        row.State.Epoch = superseding;
        await WalAppendFenceRows.WriteAsync(Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(leaf), row);

        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(treeName);
        for (var partition = 0; partition < partitions; partition++)
        {
            var claim = await _cluster.Client.GetGrain<IWalShardGrain>($"{treeName}/{partition}")
                .ClaimAppendEpochAsync(leaf, superseding, CancellationToken.None);
            Assert.That(claim.Accepted, Is.True, $"the superseding claim at partition {partition}");
        }

        return superseding;
    }

    private sealed class FailureSpy(IReplicationApplier inner) : IReplicationApplier
    {
        public List<Exception> Failures { get; } = new();

        public async Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        {
            try
            {
                return await inner.ApplyAsync(entry, cancellationToken);
            }
            catch (Exception ex)
            {
                lock (Failures)
                {
                    Failures.Add(ex);
                }

                throw;
            }
        }
    }
}
