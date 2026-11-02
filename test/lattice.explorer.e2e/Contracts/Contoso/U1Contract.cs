using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A data steward's day (U1, #5383), established as priya through the rights A2 granted her.
/// Produces <c>order/000017</c> valid (its total made positive), <c>order/000101</c>'s edit to
/// the invalid status <see cref="StoryData.InvalidStatus"/> refused by the policy (the stored
/// order unchanged), no <c>order/scratch</c>, the archive
/// <c>factory-orders-archive-2026-10</c> holding <see cref="StoryData.Archive"/>, the deleted
/// <c>factory-history</c>, and <see cref="StoryData.Corrections"/> applied (48 corrected, 2
/// refused), with the corrections file's path at <see cref="StoryKeys.CorrectionsFile"/>.
/// </summary>
public sealed class U1Contract() : ChapterContract("U1")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var priya = ContractFacades.Of(context, Personas.Priya.Id);
        var olivia = ContractFacades.Of(context);
        var data = priya.Data;

        await data.SetAsync(new DataSetRequest { TreeId = Names.Trees.FactoryOrders, Key = Names.Orders.Order17, Value = ValidOrder17.Value });
        await TrySetAsync(data, RefusedOrder101);
        await data.SetAsync(new DataSetRequest { TreeId = Names.Trees.FactoryOrders, Key = Names.Orders.Scratch, Value = StoryData.Order(1).Value });
        await data.DeleteAsync(new DataDeleteRequest { TreeId = Names.Trees.FactoryOrders, Key = Names.Orders.Scratch });

        if (!await ContractCalls.TreeIsLiveAsync(olivia.TreeAdmin, Names.Trees.FactoryOrdersArchive))
        {
            await priya.TreeAdmin.CreateTreeAsync(Names.Trees.FactoryOrdersArchive);
        }

        await ContractCalls.SetManyAsync(data, Names.Trees.FactoryOrdersArchive, StoryData.Archive);

        if (!(await olivia.TreeAdmin.GetTreeDeletionStatusAsync(Names.Trees.FactoryHistory)).IsDeleted)
        {
            await priya.TreeAdmin.CreateTreeAsync(Names.Trees.FactoryHistory);
            await priya.TreeAdmin.DeleteTreeAsync(Names.Trees.FactoryHistory);
        }

        foreach (var correction in StoryData.Corrections)
        {
            await TrySetAsync(data, correction);
        }

        context.Record(StoryKeys.CorrectionsFile, StoryData.WriteFile(Path.Combine(context.ArtifactsDirectory, "files"), Names.Files.CorrectionsNdjson));
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context);
        var data = facades.Data;
        var restored = context.IsProduced("C1");

        if (!restored)
        {
            var order17 = await ContractCalls.GetAsync(data, Names.Trees.FactoryOrders, Names.Orders.Order17);
            Require(order17 is not null && A3Contract.TotalOf(order17) >= 0, "order/000017 is valid");
        }

        var order101 = await ContractCalls.GetAsync(data, Names.Trees.FactoryOrders, Names.Orders.Order101);
        Require(order101 is not null && A3Contract.StatusOf(order101) != StoryData.InvalidStatus, "order/000101's invalid edit was refused");
        Require(await ContractCalls.GetAsync(data, Names.Trees.FactoryOrders, Names.Orders.Scratch) is null, "order/scratch is deleted");

        RequireEqual(await ContractCalls.CountPrefixAsync(data, Names.Trees.FactoryOrdersArchive, "order/"), Names.Files.ArchiveRows, "the archive holds 5,000 entries");
        Require((await facades.TreeAdmin.GetTreeDeletionStatusAsync(Names.Trees.FactoryHistory)).IsDeleted, "factory-history is deleted");

        foreach (var number in StoryData.CorrectedOrders)
        {
            var value = await ContractCalls.GetAsync(data, Names.Trees.FactoryOrders, Names.Orders.Key(number));
            Require(value is not null && A3Contract.TotalOf(value) == StoryData.CorrectedTotal(number), $"{Names.Orders.Key(number)} carries its correction");
        }

        foreach (var refused in new[] { StoryData.RefusedNegativeOrder, StoryData.RefusedStatusOrder })
        {
            var value = await ContractCalls.GetAsync(data, Names.Trees.FactoryOrders, Names.Orders.Key(refused));
            Require(value is not null && A3Contract.TotalOf(value) >= 0 && A3Contract.StatusOf(value) != StoryData.InvalidStatus, $"the refused correction of {Names.Orders.Key(refused)} did not land");
        }

        var file = context.Recall(StoryKeys.CorrectionsFile);
        Require(File.Exists(file), $"the corrections file is kept for C1 ({file})");
    }

    /// <summary>U1's fix of <c>order/000017</c>: its total made positive.</summary>
    public static StoryEntry ValidOrder17 { get; } = new(
        Names.Orders.Order17, StoryData.OrderJson(17, -StoryData.SeededTotal(17), StoryData.SeededStatus(17)));

    /// <summary>U1's edit of <c>order/000101</c> to a status outside the set, which the policy refuses.</summary>
    public static StoryEntry RefusedOrder101 { get; } = new(
        Names.Orders.Order101, StoryData.OrderJson(101, StoryData.SeededTotal(101), StoryData.InvalidStatus));

    /// <summary>Writes <paramref name="entry"/> to <c>factory-orders</c>; a refusal by the policy is the expected outcome for an invalid one.</summary>
    /// <param name="data">The Data API.</param>
    /// <param name="entry">The entry.</param>
    /// <returns>Whether the write landed.</returns>
    public static async Task<bool> TrySetAsync(LatticeDataApiGrpcClient data, StoryEntry entry)
    {
        try
        {
            await data.SetAsync(new DataSetRequest { TreeId = Names.Trees.FactoryOrders, Key = entry.Key, Value = entry.Value });
            return true;
        }
        catch (Grpc.Core.RpcException ex) when (!ContractWait.IsTransient(ex.StatusCode))
        {
            return false;
        }
    }
}
