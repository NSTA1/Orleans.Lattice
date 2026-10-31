using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>The tree the data export journey reads (issue #5366), and who may read how much of it.</summary>
internal sealed partial class ExplorerWorld
{
    /// <summary>
    /// A ledger of JSON entries under <c>public/</c> and <c>private/</c>, and one binary
    /// value, which the export journey downloads. <see cref="WorldIdentities.Dave"/> may
    /// range-read only <c>public/</c>, so his export is narrowed; <see cref="WorldIdentities.Carol"/>
    /// may read single keys but not range-read, so she is offered no export at all.
    /// </summary>
    public const string ExportTree = "export-ledger";

    /// <summary>How many <c>public/</c> entries <see cref="ExportTree"/> holds.</summary>
    public const int ExportPublicEntries = 6;

    /// <summary>How many <c>private/</c> entries <see cref="ExportTree"/> holds.</summary>
    public const int ExportPrivateEntries = 3;

    private async Task SeedExportAsync()
    {
        var grains = Head.Services.GetRequiredService<IGrainFactory>();
        var policy = Head.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (LatticeSystemOrigin.Enter())
        {
            var tree = grains.GetGrain<ILattice>(ExportTree);
            for (var i = 1; i <= ExportPublicEntries; i++)
            {
                await tree.SetAsync($"public/{i:D4}", Encoding.UTF8.GetBytes($"{{\"line\":{i},\"status\":\"{(i % 2 == 0 ? "running" : "idle")}\"}}"));
            }

            for (var i = 1; i <= ExportPrivateEntries; i++)
            {
                await tree.SetAsync($"private/{i:D4}", Encoding.UTF8.GetBytes($"{{\"cost\":{i * 100}}}"));
            }

            await tree.SetAsync("public/blob", [0x00, 0xFF, 0x10, 0x20]);

            await policy.PutRuleAsync(new LatticeAuthorizationRule(
                ruleId: "dave-read-export-ledger-public",
                subject: LatticeSubjectSelector.User(WorldIdentities.Dave),
                scope: LatticeScope.Prefix(ExportTree, "public/"),
                operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                effect: LatticeEffect.Allow));

            await policy.PutRuleAsync(new LatticeAuthorizationRule(
                ruleId: "carol-point-read-export-ledger",
                subject: LatticeSubjectSelector.User(WorldIdentities.Carol),
                scope: LatticeScope.Tree(ExportTree),
                operations: LatticeOperation.Read,
                effect: LatticeEffect.Allow));
        }
    }
}
