using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.Security;

[TestFixture]
public class LatticeReplicationGrpcAuthInterceptorTests
{
    private static IOptionsMonitor<LatticeReplicationSecurityOptions> OptionsFor(LatticeReplicationSecurityOptions o)
    {
        var m = Substitute.For<IOptionsMonitor<LatticeReplicationSecurityOptions>>();
        m.CurrentValue.Returns(o);
        return m;
    }

    [Test]
    public void Constructor_throws_on_null_secrets()
    {
        Assert.That(
            () => new LatticeReplicationGrpcAuthInterceptor(
                null!,
                OptionsFor(new LatticeReplicationSecurityOptions()),
                NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options()
    {
        Assert.That(
            () => new LatticeReplicationGrpcAuthInterceptor(
                Substitute.For<IReplicationSecretProvider>(),
                null!,
                NullLogger<LatticeReplicationGrpcAuthInterceptor>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_logger()
    {
        Assert.That(
            () => new LatticeReplicationGrpcAuthInterceptor(
                Substitute.For<IReplicationSecretProvider>(),
                OptionsFor(new LatticeReplicationSecurityOptions()),
                null!),
            Throws.ArgumentNullException);
    }
}
