using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Coverage for the per-key merge-mode label a capture stamps onto its
/// <see cref="BackupKeyDescriptor"/>s. A key's mode is read from the durable
/// per-key discriminator on the snapshot row when present, falling back to the
/// declared per-tree mode otherwise: a tree that declares a CRDT mode labels
/// every captured key <see cref="BackupKeyMergeMode.Crdt"/>, a last-writer-wins
/// or non-replicated tree labels plain keys
/// <see cref="BackupKeyMergeMode.LastWriterWins"/>, and a local-only tree that
/// mixes LWW and CRDT keys now labels each key with its true per-key mode.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupCaptureMergeModeTests
{
    private const string Tree = "orders";

    private static LatticeMergeMode? s_declaredMode;

    private TestCluster _cluster = null!;

    [TearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task CaptureAsync_labels_keys_last_writer_wins_for_a_non_replicated_tree()
    {
        await DeployAsync(declaredMode: null);
        var descriptors = await CaptureDescriptorsAsync(SeedLwwAsync);

        Assert.That(
            descriptors.Select(d => d.MergeMode),
            Is.All.EqualTo(BackupKeyMergeMode.LastWriterWins));
    }

    [Test]
    public async Task CaptureAsync_labels_keys_lww_when_the_tree_declares_the_lww_register_mode()
    {
        await DeployAsync(declaredMode: LatticeMergeMode.LwwRegister);
        var descriptors = await CaptureDescriptorsAsync(SeedLwwAsync);

        Assert.That(
            descriptors.Select(d => d.MergeMode),
            Is.All.EqualTo(BackupKeyMergeMode.LastWriterWins));
    }

    [Test]
    public async Task CaptureAsync_labels_keys_crdt_when_the_tree_declares_a_crdt_mode()
    {
        await DeployAsync(declaredMode: LatticeMergeMode.OrSet);
        var descriptors = await CaptureDescriptorsAsync(SeedCrdtAsync);

        Assert.That(descriptors, Is.Not.Empty);
        Assert.That(
            descriptors.Select(d => d.MergeMode),
            Is.All.EqualTo(BackupKeyMergeMode.Crdt));
    }

    [Test]
    public async Task CaptureAsync_labels_each_key_with_its_true_mode_for_a_mixed_local_only_tree()
    {
        // A local-only tree (no declared mode -> resolver returns null) that mixes
        // plain LWW keys with CRDT keys. Before the per-key discriminator the
        // resolver's null was treated as LWW and every key was mislabelled
        // LastWriterWins; now each key must carry its true mode.
        await DeployAsync(declaredMode: null);
        var descriptors = await CaptureDescriptorsAsync(SeedMixedAsync);

        var byKey = descriptors.ToDictionary(d => d.Key, d => d.MergeMode);
        Assert.Multiple(() =>
        {
            Assert.That(byKey["lww1"], Is.EqualTo(BackupKeyMergeMode.LastWriterWins));
            Assert.That(byKey["lww2"], Is.EqualTo(BackupKeyMergeMode.LastWriterWins));
            Assert.That(byKey["crdt1"], Is.EqualTo(BackupKeyMergeMode.Crdt));
            Assert.That(byKey["crdt2"], Is.EqualTo(BackupKeyMergeMode.Crdt));
        });
    }

    private static async Task SeedLwwAsync(ILattice tree)
    {
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await tree.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
    }

    private static async Task SeedCrdtAsync(ILattice tree)
    {
        // A declared CRDT tree rejects plain last-writer-wins writes, so seed the
        // keys through the matching CRDT accessor.
        await tree.OrSet("k1").AddAsync(Encoding.UTF8.GetBytes("e1"), "r1");
        await tree.OrSet("k2").AddAsync(Encoding.UTF8.GetBytes("e2"), "r1");
    }

    private static async Task SeedMixedAsync(ILattice tree)
    {
        // Local-only tree: plain LWW keys and CRDT keys coexist because nothing is
        // declared for replication.
        await tree.SetAsync("lww1", Encoding.UTF8.GetBytes("v1"));
        await tree.SetAsync("lww2", Encoding.UTF8.GetBytes("v2"));
        await tree.OrSet("crdt1").AddAsync(Encoding.UTF8.GetBytes("e1"), "r1");
        await tree.OrSet("crdt2").AddAsync(Encoding.UTF8.GetBytes("e2"), "r1");
    }

    private async Task DeployAsync(LatticeMergeMode? declaredMode)
    {
        s_declaredMode = declaredMode;
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    private async Task<IReadOnlyList<BackupKeyDescriptor>> CaptureDescriptorsAsync(Func<ILattice, Task> seed)
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(Tree);
        await seed(tree);

        var capture = _cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<ILatticeBackupCaptureService>();

        var result = await capture.CaptureAsync(
            new LatticeBackupCaptureRequest("mode-labelled", BackupScopeSelector.WholeTree(Tree)));

        return result.Manifest.KeyDescriptors;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeBackup();
            // Override the default (null-returning) resolver so the capture sees a
            // declared per-tree mode. A plain AddSingleton after AddLattice wins
            // over the core TryAddSingleton default.
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver>(
                new StubMergeModeResolver(Tree, s_declaredMode));
        }
    }

    private sealed class StubMergeModeResolver(string targetTree, LatticeMergeMode? mode)
        : ILatticeMergeModeResolver
    {
        // Only the tree under test carries the declared mode; the sink's own
        // reserved system trees stay non-replicated so their plain writes work.
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId == targetTree ? mode : null;
    }
}
