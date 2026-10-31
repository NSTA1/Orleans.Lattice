using System.Globalization;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// The scratch tree step C2.2 scans: a few hundred order-shaped values under a non-strict copy
/// of the orders policy, some of them breaking it. The harness creates it before the step and
/// purges it after, both through the APIs (named in #5390 as harness actions), so the story's
/// own trees are left as they were.
/// </summary>
internal static class C2ScanTree
{
    /// <summary>The scratch tree's id.</summary>
    public const string TreeId = "scratch-c2-compliance";

    /// <summary>How many values it holds.</summary>
    public const int Count = 300;

    /// <summary>Every how many values one carries a negative total, which the policy refuses.</summary>
    public const int NegativeEvery = 25;

    /// <summary>How many values break the policy.</summary>
    public const int NonCompliant = Count / NegativeEvery;

    /// <summary>The policy: the orders baseline's two rules, not strict, so its breaches can be stored and then found.</summary>
    public static LatticeSchemaPolicy Policy => new([ContosoSchema.TotalAtLeastZero, ContosoSchema.StatusInSet]);

    /// <summary>Creates the tree, writes its values and sets its policy; idempotent.</summary>
    /// <param name="estate">The estate.</param>
    public static async Task SeedAsync(JourneyEstate estate)
    {
        var facades = ContractFacades.Of(estate);
        if (!await ContractCalls.TreeIsLiveAsync(facades.TreeAdmin, TreeId))
        {
            await ContractWait.RetryingAsync($"creating {TreeId}", () => facades.TreeAdmin.CreateTreeAsync(TreeId));
        }

        await ContractCalls.SetManyAsync(facades.Data, TreeId, Enumerable.Range(1, Count).Select(Entry));
        await ContractWait.UntilAsync($"the policy of {TreeId}", async () =>
        {
            await facades.Schema.SetPolicyAsync(TreeId, Policy);
            return true;
        });
    }

    /// <summary>Deletes and purges the tree.</summary>
    /// <param name="estate">The estate.</param>
    public static Task PurgeAsync(JourneyEstate estate) =>
        ContractCalls.DeleteAndPurgeAsync(ContractFacades.Of(estate).TreeAdmin, TreeId);

    private static StoryEntry Entry(int number)
    {
        var total = number % NegativeEvery == 0 ? -(10 + (number % 97)) : 10 + (number % 97);
        return new StoryEntry(
            "order/" + number.ToString("D6", CultureInfo.InvariantCulture),
            string.Create(CultureInfo.InvariantCulture, $"{{\"order\":{number},\"total\":{total},\"status\":\"open\",\"line\":\"line-{(number % 3) + 1}\"}}"));
    }
}
