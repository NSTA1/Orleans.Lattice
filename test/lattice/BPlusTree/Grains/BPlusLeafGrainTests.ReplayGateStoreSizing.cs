using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Internal.Cgroups;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for sizing the per-silo replay gate by the WAL store (issue
/// #5957). The auto default admits the remote ceiling only when the silo's
/// registered <see cref="IWalStorageProvider"/> reports
/// <see cref="IWalStorageProvider.IsRemote"/>; a local store keeps the
/// CPU-derived ceiling, which 32 CPU-bound replays on a small host had starved.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public void Auto_resolves_to_the_remote_ceiling_on_a_remote_wal_store()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ResolveGateSizing(
                    LatticeOptions.AutoWalMaterialiserMaxConcurrentReplays, 4, 4, walStoreIsRemote: true).Ceiling,
                Is.EqualTo(LatticeOptions.RemoteWalMaterialiserMaxConcurrentReplays));
            Assert.That(BPlusLeafGrain.ResolveGateSizing(
                    LatticeOptions.AutoWalMaterialiserMaxConcurrentReplays, 64, null, walStoreIsRemote: true).Ceiling,
                Is.EqualTo(32), "the remote ceiling does not scale with CPU");
        });
    }

    [Test]
    public void Auto_resolves_to_the_cpu_derived_ceiling_on_a_local_wal_store()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ResolveGateSizing(
                    LatticeOptions.AutoWalMaterialiserMaxConcurrentReplays, 16, 4, walStoreIsRemote: false).Ceiling,
                Is.EqualTo(4));
            Assert.That(BPlusLeafGrain.ResolveGateSizing(
                    LatticeOptions.AutoWalMaterialiserMaxConcurrentReplays, 6, null, walStoreIsRemote: false).Ceiling,
                Is.EqualTo(6));
        });
    }

    [Test]
    public void Zero_and_explicit_replay_ceilings_ignore_the_wal_store()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ResolveGateSizing(0, 16, 4, walStoreIsRemote: true).Ceiling, Is.EqualTo(4),
                "0 stays CPU-derived on a remote store");
            Assert.That(BPlusLeafGrain.ResolveGateSizing(7, 16, 4, walStoreIsRemote: true).Ceiling, Is.EqualTo(7));
            Assert.That(BPlusLeafGrain.ResolveGateSizing(7, 16, 4, walStoreIsRemote: false).Ceiling, Is.EqualTo(7));
        });
    }

    [Test]
    public void The_replay_ceiling_defaults_to_auto()
    {
        Assert.That(new LatticeOptions().WalMaterialiserMaxConcurrentReplays,
            Is.EqualTo(LatticeOptions.AutoWalMaterialiserMaxConcurrentReplays));
    }

    [Test]
    public void The_in_memory_wal_store_is_not_remote()
    {
        Assert.That(((IWalStorageProvider)new InMemoryWalStorageProvider()).IsRemote, Is.False);
    }

    [TestCase(true)]
    [TestCase(false)]
    [NonParallelizable]
    public async Task An_auto_sized_gate_reads_whether_the_registered_wal_store_is_remote(bool remote)
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var provider = Substitute.For<IWalStorageProvider>();
            provider.IsRemote.Returns(remote);
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null,
                persistedCheckpoint: 0,
                walHead: 0,
                configureOptions: options => options.WalReplayServiceRateControllerEnabled = false,
                walStorageProvider: provider);
            state.State.TreeId = UniqueReplayPermitTree();
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

            var expected = remote
                ? LatticeOptions.RemoteWalMaterialiserMaxConcurrentReplays
                : BPlusLeafGrain.ResolveDefaultReplayCeiling(Environment.ProcessorCount, ContainerCpuGrant.Read());
            Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.EqualTo(expected),
                remote
                    ? "a remote WAL store sizes the auto gate to the remote ceiling"
                    : "a local WAL store keeps the CPU-derived ceiling (#5957)");
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
