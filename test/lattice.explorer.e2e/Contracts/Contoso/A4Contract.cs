using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Orleans.Lattice.Schema;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A4, the schema baseline (#5375). Produces <see cref="ContosoSchema.OrdersBaseline"/>, the
/// strict policy on <c>factory-orders</c>; versioning on in family
/// <see cref="ContosoSchema.OrdersFamily"/> at target 1 (not strict), with
/// <see cref="ContosoSchema.OrdersRegistrationJson"/> registered; and the line feed's invalid
/// orders refused by the policy (a user-origin write that breaks a policy is refused, never
/// dead-lettered). U2 later revises the policy and advances the target to 2.
/// </summary>
public sealed class A4Contract() : ChapterContract("A4")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var schema = ContractFacades.Of(context).Schema;
        await schema.SetPolicyAsync(Names.Trees.FactoryOrders, ContosoSchema.OrdersBaseline);
        if (await schema.GetVersionConfigAsync(Names.Trees.FactoryOrders) is null)
        {
            await schema.SetVersionConfigAsync(Names.Trees.FactoryOrders, new LatticeSchemaVersionConfig(ContosoSchema.OrdersFamily, targetVersion: 1));
        }

        var registry = await schema.GetRegistryAsync(Names.Trees.FactoryOrders);
        if (!registry.Descriptors.Any(d => d.SchemaId == ContosoSchema.OrdersFamily))
        {
            await schema.RegisterSchemaAsync(Names.Trees.FactoryOrders, ContosoSchema.ParseRegistration(ContosoSchema.OrdersRegistrationJson));
        }

        // The line feed's next invalid order is refused by the policy, as every user-origin
        // write that breaks it is; it counts the refusal.
        var feed = context.Estate.LineFeed;
        var invalid = (await feed.StatusAsync()).InvalidOrdersWritten;
        await feed.WaitUntilAsync(status => status.InvalidOrdersWritten > invalid, TimeSpan.FromMinutes(2));
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var schema = ContractFacades.Of(context).Schema;
        var revised = context.IsProduced("U2");
        var policy = await schema.GetPolicyAsync(Names.Trees.FactoryOrders);
        if (!revised)
        {
            Require(ContosoSchema.SameAs(policy, ContosoSchema.OrdersBaseline), "the strict baseline policy on factory-orders (total >= 0, status in the set)");
        }
        else
        {
            Require(policy is { StrictIngest: true } && ContosoSchema.Holds(policy, ContosoSchema.TotalAtLeastZero), "a strict policy holding total >= 0");
        }

        var config = await schema.GetVersionConfigAsync(Names.Trees.FactoryOrders);
        Require(config is { SchemaId: ContosoSchema.OrdersFamily }, $"versioning is on in family {ContosoSchema.OrdersFamily}");
        if (!revised)
        {
            RequireEqual(config!.Value.TargetVersion, 1u, "the target stays version 1");
        }

        var registry = await schema.GetRegistryAsync(Names.Trees.FactoryOrders);
        Require(
            registry.Descriptors.Any(d => d is { SchemaId: ContosoSchema.OrdersFamily, Version: 1 })
            && registry.Descriptors.Any(d => d is { SchemaId: ContosoSchema.OrdersFamily, Version: 2 })
            && registry.Upcasters.Any(u => u is { SchemaId: ContosoSchema.OrdersFamily, FromVersion: 1, ToVersion: 2 }),
            "versions 1 and 2 and the hop between them are registered");
    }
}
