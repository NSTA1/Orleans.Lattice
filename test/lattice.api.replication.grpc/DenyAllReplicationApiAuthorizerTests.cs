namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Verifies the default-deny authorizers: the auto-registered
/// <see cref="DenyAllReplicationApiAuthorizer"/> rejects every call regardless of
/// operation or target, and the opt-in
/// <see cref="AllowAllReplicationApiAuthorizer"/> permits every call. These are
/// the two ends of the transport-boundary authorization posture.
/// </summary>
public sealed class DenyAllReplicationApiAuthorizerTests
{
    private static LatticeReplicationApiAuthorizationContext Context(LatticeReplicationApiOperation operation) =>
        new(new FakeServerCallContext("/orleans.lattice.api.replication/EnableReplication"), operation, "orders");

    [Test]
    public async Task DenyAll_denies_every_operation()
    {
        var authorizer = new DenyAllReplicationApiAuthorizer();

        Assert.Multiple(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.EnableReplication), default), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.DisableReplication), default), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.GetReplicationConfig), default), Is.False);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.Unknown), default), Is.False);
        });
    }

    [Test]
    public async Task AllowAll_permits_every_operation()
    {
        var authorizer = new AllowAllReplicationApiAuthorizer();

        Assert.Multiple(async () =>
        {
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.EnableReplication), default), Is.True);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.DisableReplication), default), Is.True);
            Assert.That(await authorizer.IsAuthorizedAsync(Context(LatticeReplicationApiOperation.GetReplicationConfig), default), Is.True);
        });
    }
}
