using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task MultiKeyReads_registry_outcome_matches_single_key_visibility(
        [Values("GetMany", "GetKeys", "GetEntries", "GetLiveEntries", "GetLiveRawEntries", "Count", "RangedCount", "GetStats")] string reader,
        [Values("Indeterminate", "Committed", "InFlight", "Aborted")] string outcome,
        [Values(false, true)] bool scoped,
        [Values(false, true)] bool alreadyTerminal)
    {
        LatticeRegistrySnapshotContext.Current = null;
        var status = Enum.Parse<TxStatus>(outcome);
        var (grain, registry) = BuildLeafWithRegistry(scoped ? TxStatus.Committed : status);
        var txid = Guid.NewGuid();
        if (alreadyTerminal)
            await MarkRecentlyTerminalAsync(grain, txid);
        await SeedPreparedOverrideAsync(grain, txid);
        await grain.SetAsync("deleted", [3]);
        await grain.SetAsync("stable", [4]);
        await PreparePendingDeleteAsync(grain, txid, "deleted");
        await PreparePendingSetAsync(grain, txid, "fresh", [5]);

        using var scope = scoped
            ? LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [txid] = status })
            : null;
        var expected = new Dictionary<string, byte[]> { ["stable"] = [4] };
        if (status == TxStatus.Committed && !alreadyTerminal)
        {
            expected["k"] = [2, 2];
            expected["fresh"] = [5];
        }
        else if (status != TxStatus.Indeterminate)
        {
            expected["k"] = [1];
            expected["deleted"] = [3];
        }

        switch (reader)
        {
            case "Count":
                Assert.That(await grain.CountAsync(), Is.EqualTo(expected.Count));
                break;
            case "RangedCount":
                Assert.That(await grain.CountAsync("fresh", "stable"), Is.EqualTo(
                    expected.Keys.Count(key => string.CompareOrdinal(key, "fresh") >= 0
                        && string.CompareOrdinal(key, "stable") < 0)));
                break;
            case "GetStats":
                var stats = await grain.GetStatsAsync();
                Assert.That(stats.LiveKeys, Is.EqualTo(expected.Count));
                Assert.That(stats.Tombstones, Is.EqualTo(status == TxStatus.Committed && !alreadyTerminal ? 1 : 0));
                break;
            case "GetKeys":
                Assert.That(await grain.GetKeysAsync(), Is.EquivalentTo(expected.Keys));
                break;
            default:
                var actual = reader switch
                {
                    "GetMany" => await grain.GetManyAsync(["deleted", "fresh", "k", "stable", "missing"]),
                    "GetEntries" => (await grain.GetEntriesAsync()).ToDictionary(pair => pair.Key, pair => pair.Value),
                    "GetLiveEntries" => await grain.GetLiveEntriesAsync(),
                    "GetLiveRawEntries" => (await grain.GetLiveRawEntriesAsync()).ToDictionary(entry => entry.Key, entry => entry.Value!),
                    _ => throw new ArgumentOutOfRangeException(nameof(reader)),
                };
                Assert.That(actual.Keys, Is.EquivalentTo(expected.Keys),
                    $"Prepared k=[2,2] and pre-saga k=[1] must both be hidden for Indeterminate; actual k=[{string.Join(",", actual.GetValueOrDefault("k") ?? [])}]");
                foreach (var (key, value) in expected)
                    Assert.That(actual[key], Is.EqualTo(value), key);
                break;
        }

        if (scoped)
            await registry.DidNotReceive().GetStatusManyAsync(Arg.Any<IReadOnlyList<Guid>>());
        else
            await registry.Received().GetStatusManyAsync(Arg.Is<IReadOnlyList<Guid>>(ids => ids.Contains(txid)));
    }
}
