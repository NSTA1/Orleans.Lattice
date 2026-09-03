using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Hosting;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// DI-level coverage for
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeSagaParticipant{TParticipant}(ISiloBuilder, string?)"/>:
/// a host-defined participant is enlisted into the resolved
/// <see cref="ISagaParticipant"/> set alongside the built-in restore participant,
/// registration is idempotent per participant type, an optional name wraps the
/// participant in a diagnostic decorator, and the public arguments are validated.
/// </summary>
[TestFixture]
public class LatticeSagaParticipantRegistrationTests
{
    private static ISiloBuilder BuilderWith(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    private static IServiceCollection ReplicationServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");
        return services;
    }

    [Test]
    public void AddLatticeSagaParticipant_enlists_custom_participant_alongside_restore()
    {
        var services = ReplicationServices();
        BuilderWith(services).AddLatticeSagaParticipant<ExampleSagaParticipant>();

        using var provider = services.BuildServiceProvider();
        var participants = provider.GetServices<ISagaParticipant>().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(participants.OfType<ExampleSagaParticipant>().Count(), Is.EqualTo(1),
                "the custom participant must be enlisted exactly once");
            Assert.That(participants.OfType<RestoreParticipant>().Any(), Is.True,
                "the built-in restore participant must still be enlisted alongside it");
        });
    }

    [Test]
    public void AddLatticeSagaParticipant_is_idempotent_per_participant_type()
    {
        var services = ReplicationServices();
        var builder = BuilderWith(services);
        builder.AddLatticeSagaParticipant<ExampleSagaParticipant>();
        builder.AddLatticeSagaParticipant<ExampleSagaParticipant>();

        using var provider = services.BuildServiceProvider();
        var count = provider.GetServices<ISagaParticipant>().OfType<ExampleSagaParticipant>().Count();

        Assert.That(count, Is.EqualTo(1), "repeated registration must enlist the participant once");
    }

    [Test]
    public void AddLatticeSagaParticipant_with_name_wraps_participant_in_named_decorator()
    {
        var services = ReplicationServices();
        BuilderWith(services).AddLatticeSagaParticipant<ExampleSagaParticipant>("orders-config");

        using var provider = services.BuildServiceProvider();
        var named = provider.GetServices<ISagaParticipant>()
            .OfType<NamedSagaParticipant<ExampleSagaParticipant>>()
            .SingleOrDefault();

        Assert.That(named, Is.Not.Null, "the named overload must enlist a diagnostic wrapper");
        Assert.Multiple(() =>
        {
            Assert.That(named!.Name, Is.EqualTo("orders-config"));
            Assert.That(named.Inner, Is.InstanceOf<ExampleSagaParticipant>());
        });
    }

    [Test]
    public async Task Named_wrapper_forwards_every_spi_call_to_the_inner_participant()
    {
        var services = ReplicationServices();
        BuilderWith(services).AddLatticeSagaParticipant<ExampleSagaParticipant>("orders-config");

        using var provider = services.BuildServiceProvider();
        var named = provider.GetServices<ISagaParticipant>()
            .OfType<NamedSagaParticipant<ExampleSagaParticipant>>()
            .Single();
        var request = new SagaControlRequest { SagaId = "saga-x", TargetTree = "orders" };

        var vote = await named.PrepareAsync(request);
        await named.CommitAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That(named.Inner.PrepareCount, Is.EqualTo(1));
            Assert.That(named.Inner.CommitCount, Is.EqualTo(1));
            Assert.That(named.Inner.CommittedValue, Is.EqualTo("example-value"));
        });
    }

    [Test]
    public async Task Named_wrapper_forwards_the_compensation_and_status_calls_too()
    {
        // The wrapper is diagnostic only: it must forward every SPI call
        // unchanged. An abort or status that stopped at the decorator would
        // silently drop a rollback the coordinator believes happened.
        var services = ReplicationServices();
        BuilderWith(services).AddLatticeSagaParticipant<ExampleSagaParticipant>("orders-config");

        using var provider = services.BuildServiceProvider();
        var named = provider.GetServices<ISagaParticipant>()
            .OfType<NamedSagaParticipant<ExampleSagaParticipant>>()
            .Single();
        var request = new SagaControlRequest { SagaId = "saga-y", TargetTree = "orders" };

        await named.PrepareAsync(request);
        Assert.That(await named.GetStatusAsync(request), Is.EqualTo(SagaPhase.Prepared),
            "status is passed straight through, not synthesised by the decorator");

        await named.AbortAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(named.Inner.AbortCount, Is.EqualTo(1));
            Assert.That(named.Inner.HasPendingValue, Is.False,
                "the compensation reached the inner participant, not just the log");
        });
        Assert.That(await named.GetStatusAsync(request), Is.EqualTo(SagaPhase.None));
    }

    [Test]
    public void AddLatticeSagaParticipant_null_builder_throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => ((ISiloBuilder)null!).AddLatticeSagaParticipant<ExampleSagaParticipant>());
    }

    [Test]
    public void AddLatticeSagaParticipant_blank_name_throws()
    {
        var services = new ServiceCollection();
        var builder = BuilderWith(services);

        Assert.Throws<ArgumentException>(
            () => builder.AddLatticeSagaParticipant<ExampleSagaParticipant>("  "));
    }
}
