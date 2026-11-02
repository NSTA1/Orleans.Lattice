using NSubstitute;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The bootstrap a receiver's enable requests when the tree is already
/// enrolled locally, because its enrolment converged from a peer through the
/// config tree before the receiver's own enable ran (issue #5594).
/// </summary>
public sealed partial class LatticeReplicationConfigAuthorityTests
{
    private const string Source = "site-b";

    [Test]
    public async Task EnableReplicationAsync_on_an_already_enrolled_receiver_with_a_source_requests_bootstrap()
    {
        // The source enables first; its enrolment converges to this receiver
        // through the shared config OR-Map before the receiver's own enable.
        var store = new InMemoryConfigStore();
        await CreateAuthority(store, localReplicaId: Source)
            .EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);

        var admin = Substitute.For<ILatticeReplicationAdmin>();
        var receiver = CreateAuthority(store, admin: admin);

        var result = await receiver.EnableReplicationAsync(
            Tree, LatticeMergeMode.LwwRegister, bootstrapSourceClusterId: Source);

        // The enable is still idempotent for the config, but the named source's
        // pre-enrolment rows reach this receiver only through the snapshot.
        Assert.That(result.AlreadyEnabled, Is.True);
        Assert.That(result.BootstrapRequested, Is.True);
        await admin.Received(1).RequestSnapshotAsync(Tree, Source, Arg.Any<CancellationToken>());
    }
}
