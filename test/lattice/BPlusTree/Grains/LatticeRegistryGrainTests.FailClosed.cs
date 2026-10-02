using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    // --- Issue #4230: non-create verbs fail closed on an absent row ---

    /// <summary>
    /// Seeds <paramref name="treeId"/> with a registered row carrying no overrides,
    /// for tests of a non-create verb, which now refuses a tree with no row.
    /// </summary>
    private static void SeedRegisteredRow(ISystemLattice tree, string treeId) =>
        tree.GetAsync(treeId).Returns(Task.FromResult<byte[]?>(
            System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
                new Orleans.Lattice.BPlusTree.State.TreeRegistryEntry { ShardCount = 4 })));

    private static readonly string[] NonCreateVerbs =
    [
        "SetShardMapAsync", "ReassignSlotsAsync", "AllocateNextShardIndexAsync", "SetPublishEventsAsync",
        "SetHistoryRetentionAsync", "SetMaintainProjectionDigestAsync", "SetMaxCacheValueBytesAsync",
        "SetWalMaxRetainedBytesAsync", "LatchProjectionDigestPermanentlyDisabledAsync",
        "UpdateWalPlacementAsync(single)", "UpdateWalPlacementAsync(batch)",
    ];

    private static Task InvokeNonCreateVerb(LatticeRegistryGrain g, string verb, string id) => verb switch
    {
        "SetShardMapAsync" => g.SetShardMapAsync(id, ShardMap.CreateDefault(8, 4)),
        "ReassignSlotsAsync" => g.ReassignSlotsAsync(id, [0], 1, ShardMap.CreateDefault(8, 4)),
        "AllocateNextShardIndexAsync" => g.AllocateNextShardIndexAsync(id, 3),
        "SetPublishEventsAsync" => g.SetPublishEventsAsync(id, true),
        "SetHistoryRetentionAsync" => g.SetHistoryRetentionAsync(id, HistoryRetentionMode.FullValue, TimeSpan.FromHours(1)),
        "SetMaintainProjectionDigestAsync" => g.SetMaintainProjectionDigestAsync(id, true),
        "SetMaxCacheValueBytesAsync" => g.SetMaxCacheValueBytesAsync(id, 1024),
        "SetWalMaxRetainedBytesAsync" => g.SetWalMaxRetainedBytesAsync(id, 1024),
        "LatchProjectionDigestPermanentlyDisabledAsync" => g.LatchProjectionDigestPermanentlyDisabledAsync(id),
        "UpdateWalPlacementAsync(single)" => g.UpdateWalPlacementAsync(id, 0, 0, "dedicated"),
        "UpdateWalPlacementAsync(batch)" => g.UpdateWalPlacementAsync(id, 0, [(0, "dedicated")]),
        _ => throw new ArgumentOutOfRangeException(nameof(verb), verb, null),
    };

    [TestCaseSource(nameof(NonCreateVerbs))]
    public void Non_create_verb_refuses_an_absent_row_and_writes_nothing(string verb)
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("gone").Returns(Task.FromResult<byte[]?>(null));

        var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(() => InvokeNonCreateVerb(grain, verb, "gone"));

        Assert.That(ex!.TreeId, Is.EqualTo("gone"));
        tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [TestCaseSource(nameof(NonCreateVerbs))]
    public async Task Non_create_verb_still_rewrites_an_existing_row(string verb)
    {
        var (grain, tree) = CreateGrain();
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new Orleans.Lattice.BPlusTree.State.TreeRegistryEntry { ShardCount = 4 });
        tree.GetAsync("live").Returns(Task.FromResult<byte[]?>(bytes));

        await InvokeNonCreateVerb(grain, verb, "live");

        await tree.Received(1).SetAsync("live", Arg.Any<byte[]>());
    }
}
