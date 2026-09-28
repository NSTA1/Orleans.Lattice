using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <c>AddLatticeReplicationStatusApi</c>: the ordering guard, the
/// null guard, idempotent wiring, independence from the control facade, options
/// layering and validation, and resolution of the facade.
/// </summary>
[TestFixture]
public sealed class LatticeApiReplicationStatusRegistrationTests
{
    [Test]
    public void AddLatticeReplicationStatusApi_without_replication_throws()
    {
        Assert.That(() => new FakeSiloBuilder().AddLatticeReplicationStatusApi(), Throws.InvalidOperationException);
    }

    [Test]
    public void AddLatticeReplicationStatusApi_null_builder_throws()
    {
        Assert.That(() => ((ISiloBuilder)null!).AddLatticeReplicationStatusApi(), Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationStatusApi_wires_the_facade_and_read_path_once()
    {
        var builder = CreateBuilder();

        builder.AddLatticeReplicationStatusApi();
        var afterFirst = builder.Services.Count;
        builder.AddLatticeReplicationStatusApi();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ILatticeReplicationStatus)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(IReplicationPeerStatusReader)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ReplicationAccessAuthorizer)), Is.EqualTo(1));
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(IValidateOptions<LatticeReplicationStatusOptions>)),
                Is.EqualTo(1));
            Assert.That(builder.Services.Count, Is.EqualTo(afterFirst), "a repeated call without a delegate adds nothing");
        });
    }

    [Test]
    public void AddLatticeReplicationStatusApi_does_not_register_the_control_facade()
    {
        var builder = CreateBuilder();

        builder.AddLatticeReplicationStatusApi();

        Assert.That(builder.Services.Any(d => d.ServiceType == typeof(ILatticeReplicationControl)), Is.False);
    }

    [Test]
    public void AddLatticeReplicationStatusApi_resolves_the_facade()
    {
        var builder = CreateBuilder();
        builder.AddLatticeReplicationStatusApi();

        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ILatticeReplicationStatus>(), Is.InstanceOf<LatticeReplicationStatus>());
    }

    [Test]
    public void AddLatticeReplicationStatusApi_layers_every_configure_delegate_lazily()
    {
        var builder = CreateBuilder();
        var applied = new List<string>();

        builder.AddLatticeReplicationStatusApi(o => { applied.Add("first"); o.LaggingEntriesBehind = 7; });
        builder.AddLatticeReplicationStatusApi(_ => applied.Add("second"));
        Assert.That(applied, Is.Empty, "registration must not eagerly run a configure delegate");

        using var provider = builder.Services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeReplicationStatusOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.EqualTo(new[] { "first", "second" }));
            Assert.That(options.LaggingEntriesBehind, Is.EqualTo(7));
        });
    }

    [Test]
    public void AddLatticeReplicationStatusApi_rejects_invalid_thresholds_when_the_options_resolve()
    {
        var builder = CreateBuilder();
        builder.AddLatticeReplicationStatusApi(o => o.LaggingEntriesBehind = -1);

        using var provider = builder.Services.BuildServiceProvider();

        Assert.That(
            () => provider.GetRequiredService<IOptions<LatticeReplicationStatusOptions>>().Value,
            Throws.TypeOf<OptionsValidationException>());
    }

    [Test]
    public void AddLatticeReplicationStatusApi_keeps_a_reader_registered_first()
    {
        var builder = CreateBuilder(withReader: false);
        var reader = new StatsBackedPeerStatusReader();
        builder.Services.AddSingleton<IReplicationPeerStatusReader>(reader);

        builder.AddLatticeReplicationStatusApi();

        Assert.That(
            builder.Services.Single(d => d.ServiceType == typeof(IReplicationPeerStatusReader)).ImplementationInstance,
            Is.SameAs(reader));
    }

    /// <summary>
    /// The minimum registration set the facade needs to resolve without a silo:
    /// the telemetry state whose absence the ordering guard rejects, a reader
    /// standing in for the grain-service fan-out, the access gate, the tenant
    /// resolver, and the replication options.
    /// </summary>
    private static FakeSiloBuilder CreateBuilder(bool withReader = true)
    {
        var builder = new FakeSiloBuilder();
        builder.Services.AddSingleton(new ReplicationPeerStats());
        if (withReader)
        {
            builder.Services.AddSingleton<IReplicationPeerStatusReader>(new StatsBackedPeerStatusReader());
        }

        builder.Services.AddSingleton<ILatticeAccessGate>(new AllowingAccessGate());
        builder.Services.AddSingleton<ITenantContextResolver>(new DefaultTenantContextResolver());
        builder.Services.AddOptions<LatticeReplicationOptions>();
        return builder;
    }

    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
