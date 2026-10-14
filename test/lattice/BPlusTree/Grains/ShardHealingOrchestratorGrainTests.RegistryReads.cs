using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardHealingOrchestratorGrainTests
{
    private static readonly HashSet<string> RegistryReadMembers = new(StringComparer.Ordinal)
    {
        nameof(ILatticeRegistry.GetEntryAsync),
        nameof(ILatticeRegistry.GetEntriesAsync),
        nameof(ILatticeRegistry.ResolveAsync),
        nameof(ILatticeRegistry.GetShardMapAsync),
        nameof(ILatticeRegistry.ExistsAsync),
    };

    private static int CountRegistryReads(ILatticeRegistry registry) =>
        registry.ReceivedCalls().Count(c => RegistryReadMembers.Contains(c.GetMethodInfo().Name));

    [Test]
    public async Task A_structurally_settled_sweep_reads_the_registry_once()
    {
        // Physical count equals the pinned base, so the structural clause settles
        // the sweep. The options, the map and the physical id all come from one
        // registry row, so the sweep pays exactly one registry read.
        var h = CreateGrain(physicalShardCount: 2, baseShardCount: 2);

        await h.Grain.RunHealingPassAsync();

        Assert.That(CountRegistryReads(h.Registry), Is.EqualTo(1));
        await h.Registry.Received(1).GetEntryAsync(TreeId);
    }

    [Test]
    public async Task An_over_split_sweep_reads_the_registry_once()
    {
        // The load path also needs the physical tree id; it is projected from the
        // same entry rather than read again with a resolve.
        var h = CreateGrain();

        await h.Grain.RunHealingPassAsync();

        Assert.That(CountRegistryReads(h.Registry), Is.EqualTo(1));
        await h.Registry.DidNotReceiveWithAnyArgs().ResolveAsync(default!);
        await h.Registry.DidNotReceiveWithAnyArgs().GetShardMapAsync(default!);
    }
}
