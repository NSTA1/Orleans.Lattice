using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Verifies the DI wiring that lets the durable
/// <see cref="LatticeSagaControlHandler"/> win over the transport-only
/// <see cref="NoParticipantSagaControlHandler"/> default. The gRPC binding
/// registers the default with <c>TryAddSingleton</c>, which is a no-op once
/// <c>AddLatticeReplication</c> has already registered the real handler, so the
/// durable participant model is the effective inbound handler.
/// </summary>
[TestFixture]
public class SagaControlHandlerRegistrationTests
{
    private static ISiloBuilder BuilderWith(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    [Test]
    public void Real_handler_wins_over_no_participant_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();

        // AddLatticeReplication registers the durable handler first.
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");

        // The gRPC binding then registers the transport-only default via
        // TryAddSingleton, which must defer to the already-registered handler.
        services.TryAddSingleton<ILatticeSagaControlHandler, NoParticipantSagaControlHandler>();

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<ILatticeSagaControlHandler>();

        Assert.That(resolved, Is.TypeOf<LatticeSagaControlHandler>());
    }
}
