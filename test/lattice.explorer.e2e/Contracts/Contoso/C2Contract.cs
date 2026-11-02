using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// Resilience (C2, #5390). The chapter takes the estate through five outages and brings it
/// back, so it produces only a healthy estate: every silo of both regions up, each gateway
/// routing to all of them, Prometheus up, and west and east each answering for the replicated
/// <c>factory-orders</c>. The seeder restores any part that is down.
/// </summary>
public sealed class C2Contract() : ChapterContract("C2")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var estate = context.Estate;
        foreach (var (name, region) in estate.Regions)
        {
            for (var index = 0; index < region.Silos.Count; index++)
            {
                if (!region.Silos[index].IsRunning)
                {
                    await estate.RestartSiloAsync(name, index);
                }
            }

            await estate.WaitForGatewayToMatchSilosAsync(name);
        }

        if (!estate.PrometheusIsRunning)
        {
            await estate.StartPrometheusAsync();
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var estate = context.Estate;
        foreach (var (name, region) in estate.Regions)
        {
            Require(region.Silos.All(silo => silo.IsRunning), $"every {name} silo is up");
            var destinations = await estate.GatewayDestinationsAsync(name);
            Require(destinations.Count > 0 && destinations.All(destination => destination.Healthy), $"the {name} gateway routes to every silo");
            await ContractWait.UntilAsync($"{name} to answer for factory-orders", async () =>
                await ContractCalls.GetAsync(ContractFacades.Of(context, region: name).Data, Names.Trees.FactoryOrders, Names.Orders.Key(1)) is not null);
        }

        Require(estate.PrometheusIsRunning, "Prometheus is up");
        if (!context.IsProduced("C3"))
        {
            foreach (var region in new[] { JourneyEstate.West, JourneyEstate.East })
            {
                var config = await ContractFacades.Of(context, region: region).Replication.GetReplicationConfigAsync();
                Require(config.Trees.Any(tree => tree.TreeId == Names.Trees.FactoryOrders && tree.Enabled), $"factory-orders is still enrolled in {region}");
            }
        }
    }
}
