using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Adapters;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// the dormant seam — confirms that <c>AddLatticeReplication</c> registers the
/// three commit-log adapter seams the core library resolves through
/// <see cref="System.IServiceProvider"/>. The seams are dormant — no
/// foreground site invokes them in the dormant seam — but the resolution shape
/// is the gate the future foreground caller/c will rely on.
/// </summary>
[TestFixture]
public class CommitLogAdapterSeamRegistrationTests
{
    [Test]
    public void AddLatticeReplication_registers_ICommitLogWriter()
    {
        var provider = BuildProvider();

        var resolved = provider.GetService<ICommitLogWriter>();

        Assert.That(resolved, Is.Not.Null);
        Assert.That(resolved, Is.InstanceOf<ReplicationCommitLogWriter>());
    }

    [Test]
    public void AddLatticeReplication_registers_ICommitLogReader()
    {
        var provider = BuildProvider();

        var resolved = provider.GetService<ICommitLogReader>();

        Assert.That(resolved, Is.Not.Null);
        Assert.That(resolved, Is.InstanceOf<ReplicationCommitLogReader>());
    }

    [Test]
    public void AddLatticeReplication_registers_ILeafSnapshotProvider()
    {
        var provider = BuildProvider();

        var resolved = provider.GetService<ILeafSnapshotProvider>();

        Assert.That(resolved, Is.Not.Null);
        Assert.That(resolved, Is.InstanceOf<ReplicationLeafSnapshotProvider>());
    }

    [Test]
    public void Commit_log_adapter_seams_are_singletons()
    {
        var provider = BuildProvider();

        var writer1 = provider.GetService<ICommitLogWriter>();
        var writer2 = provider.GetService<ICommitLogWriter>();
        var reader1 = provider.GetService<ICommitLogReader>();
        var reader2 = provider.GetService<ICommitLogReader>();
        var snap1 = provider.GetService<ILeafSnapshotProvider>();
        var snap2 = provider.GetService<ILeafSnapshotProvider>();

        Assert.Multiple(() =>
        {
            Assert.That(writer1, Is.SameAs(writer2));
            Assert.That(reader1, Is.SameAs(reader2));
            Assert.That(snap1, Is.SameAs(snap2));
        });
    }

    private static ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLatticeReplication(o =>
        {
            o.ClusterId = "test-cluster";
        });

        return services.BuildServiceProvider();
    }
}
