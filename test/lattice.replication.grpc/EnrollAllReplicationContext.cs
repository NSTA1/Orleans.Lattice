using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Test double that reports a replication merge mode for every tree, so the
/// peer-facing tree-scope gate on the replication gRPC surface admits whatever
/// tree name a fixture happens to use.
/// <para>
/// The gate fails closed when no <see cref="ILatticeReplicationContext"/> is
/// available, which is the correct posture for a real deployment (the silo's
/// <c>AddLatticeReplication</c> always registers one alongside the
/// <c>IReplicationApplier</c> the same service depends on), but the standalone
/// web-host fixtures here register the gRPC binding without a silo. Registering
/// this double keeps those fixtures exercising the wire shape they exist to
/// cover; the gate itself is covered by
/// <see cref="LatticeReplicationGrpcServiceEnrollmentTests"/>.
/// </para>
/// </summary>
internal sealed class EnrollAllReplicationContext : ILatticeReplicationContext
{
    public bool IsReplicationEnabled => true;

    public string LocalReplicaId => "test-local";

    public LatticeMergeMode? ResolveMergeMode(string treeId) => LatticeMergeMode.LwwRegister;
}

/// <summary>
/// Registration helper for <see cref="EnrollAllReplicationContext"/>, so the
/// web-host fixtures can satisfy the tree-scope gate with a single call.
/// </summary>
internal static class EnrollAllReplicationContextRegistration
{
    public static Microsoft.Extensions.DependencyInjection.IServiceCollection AddEnrollAllReplicationContext(
        this Microsoft.Extensions.DependencyInjection.IServiceCollection services)
        => Microsoft.Extensions.DependencyInjection.ServiceCollectionServiceExtensions
            .AddSingleton<ILatticeReplicationContext>(services, new EnrollAllReplicationContext());
}
