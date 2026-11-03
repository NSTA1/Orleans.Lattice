using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5731. An enable is acknowledged only once this silo's compiled snapshot -
/// the receiver gate's enrolment view - shows the tree enabled under the mode.
/// </summary>
public sealed partial class LatticeReplicationConfigAuthorityTests
{
    private static (LatticeReplicationConfigAuthority Authority, CompiledReplicationConfigSnapshotMaintainer Compiled)
        CreateCompilingAuthority(InMemoryConfigStore store, ILatticeReplicationAdmin? admin = null)
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.LocalReplicaId.Returns(LocalReplica);
        context.IsReplicationEnabled.Returns(true);
        var compiled = new CompiledReplicationConfigSnapshotMaintainer(
            store, NullLogger<CompiledReplicationConfigSnapshotMaintainer>.Instance);
        var authority = new LatticeReplicationConfigAuthority(
            store,
            new LatticeReplicationPreconditionValidator(context),
            context,
            admin ?? Substitute.For<ILatticeReplicationAdmin>(),
            Monitor(null),
            bootstrapCoordinator: null,
            runtimeConfig: compiled);
        return (authority, compiled);
    }

    private static bool Compiles(CompiledReplicationConfigSnapshotMaintainer compiled, string tree) =>
        compiled.Current.TryGetTree(tree, out var projection)
        && projection.Enabled
        && projection.Mode == LatticeMergeMode.LwwRegister;

    [Test]
    public async Task An_already_enabled_acknowledgement_compiles_an_enrolment_the_snapshot_has_not_seen()
    {
        // The tree holds a peer's enrolment that reached it without a config
        // mutation (a re-seed cutover), so this silo's snapshot predates it.
        var store = new InMemoryConfigStore();
        var (authority, compiled) = CreateCompilingAuthority(store);
        await compiled.RebuildNowAsync();
        var peer = new LatticeReplicationConfigEntry();
        peer.SetMode("site-b", LatticeMergeMode.LwwRegister);
        peer.Enable("site-b", 1);
        store.Seed(Tree, peer);
        Assert.That(Compiles(compiled, Tree), Is.False, "PRECONDITION: the snapshot predates the peer's enrolment");

        var result = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);

        Assert.Multiple(() =>
        {
            Assert.That(result.AlreadyEnabled, Is.True);
            Assert.That(Compiles(compiled, Tree), Is.True,
                "the acknowledged enable is in force on this silo's receiver gate when it returns");
        });
    }

    [Test]
    public async Task A_new_enable_is_compiled_before_its_bootstrap_is_requested()
    {
        var store = new InMemoryConfigStore();
        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var compiledAtRequest = false;
        CompiledReplicationConfigSnapshotMaintainer? compiledRef = null;
        admin.RequestSnapshotAsync(Tree, "site-b", Arg.Any<CancellationToken>()).Returns(_ =>
        {
            compiledAtRequest = Compiles(compiledRef!, Tree);
            return Task.FromResult(new OperatorReseedDecision(true, DateTimeOffset.UtcNow, null));
        });
        var (authority, compiled) = CreateCompilingAuthority(store, admin);
        compiledRef = compiled;
        await compiled.RebuildNowAsync();

        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: "site-b");

        Assert.That(compiledAtRequest, Is.True,
            "the bootstrap drains through a receiver gate that already admits the tree");
    }

    [Test]
    public async Task An_enable_the_snapshot_already_shows_makes_no_rebuild()
    {
        var store = new InMemoryConfigStore();
        var (authority, compiled) = CreateCompilingAuthority(store);
        await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);
        var epoch = compiled.CurrentEpoch;

        var again = await authority.EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);

        Assert.Multiple(() =>
        {
            Assert.That(again.AlreadyEnabled, Is.True);
            Assert.That(compiled.CurrentEpoch, Is.EqualTo(epoch), "no config read when the snapshot is current");
        });
    }
}
