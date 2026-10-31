using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A7, going multi-region (#5379). Consumes F4's east region, with <c>factory-lines</c>
/// enrolled statically there. Produces <c>factory-orders</c> enrolled at runtime in both
/// regions with last-writer-wins, east bootstrapped from west, and converged: east holds the
/// 240 seeded orders. C3 disables it.
/// </summary>
public sealed class A7Contract() : ChapterContract("A7")
{
    /// <summary>The merge mode <c>factory-orders</c> replicates with.</summary>
    public const LatticeMergeMode OrdersMergeMode = LatticeMergeMode.LwwRegister;

    private static readonly TimeSpan ConvergencePatience = TimeSpan.FromMinutes(4);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await ContractFacades.Of(context).Replication.EnableReplicationAsync(Names.Trees.FactoryOrders, OrdersMergeMode);
        await ContractFacades.Of(context, region: JourneyEstate.East).Replication
            .EnableReplicationAsync(Names.Trees.FactoryOrders, OrdersMergeMode, bootstrapSourceClusterId: JourneyEstate.West);
        await ContractWait.UntilAsync("factory-orders to converge in east", () => SeededOrdersArrivedAsync(context), ConvergencePatience);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        if (context.IsProduced("C3"))
        {
            return;
        }

        foreach (var region in new[] { JourneyEstate.West, JourneyEstate.East })
        {
            var config = await ContractFacades.Of(context, region: region).Replication.GetReplicationConfigAsync();
            Require(
                config.Trees.Any(tree => tree.TreeId == Names.Trees.FactoryOrders && tree.Enabled && tree.Mode == OrdersMergeMode),
                $"factory-orders is enrolled in {region}");
        }

        Require(await SeededOrdersArrivedAsync(context), "east holds the 240 seeded orders");
    }

    private static async Task<bool> SeededOrdersArrivedAsync(ChapterContext context)
    {
        var east = ContractFacades.Of(context, region: JourneyEstate.East).Data;
        var seeded = await ContractCalls.RangeAsync(east, Names.Trees.FactoryOrders, Names.Orders.Key(1), Names.Orders.Key(Names.Orders.SeededCount + 1));
        return seeded.Count == Names.Orders.SeededCount;
    }
}
