using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    [TestCase(null)]
    [TestCase("logical")]
    public async Task RegisterAsync_roundtrips_derivation_without_changing_restore_provenance(string? derivedFrom)
    {
        var (grain, tree) = CreateGrain();
        byte[]? bytes = null;
        tree.SetAsync("physical", Arg.Do<byte[]>(value => bytes = value)).Returns(Task.CompletedTask);
        await grain.RegisterAsync("physical", new TreeRegistryEntry
        {
            DerivedFrom = derivedFrom,
            RestoreShadowOfTreeId = derivedFrom,
        });
        tree.GetAsync("physical").Returns(_ => bytes);

        var entry = await grain.GetEntryAsync("physical");

        Assert.That(entry!.DerivedFrom, Is.EqualTo(derivedFrom));
        Assert.That(entry.RestoreShadowOfTreeId, Is.EqualTo(derivedFrom));
    }

    [Test]
    public void DeserializeEntry_without_derivation_is_an_independent_tree()
    {
        var entry = JsonSerializer.Deserialize<TreeRegistryEntry>("{}");
        Assert.That(entry!.DerivedFrom, Is.Null);
    }

    [Test]
    public async Task GetAliasesTargetingAsync_tracks_set_remove_swap_and_unregister_from_persisted_entries()
    {
        var (grain, tree) = CreateGrain();
        var entries = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        tree.GetAsync(Arg.Any<string>()).Returns(call =>
            entries.TryGetValue(call.Arg<string>(), out var bytes) ? bytes : null);
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(call =>
        {
            entries[call.Arg<string>()] = call.Arg<byte[]>();
            return Task.CompletedTask;
        });
        tree.DeleteAsync(Arg.Any<string>()).Returns(call =>
        {
            return Task.FromResult(entries.Remove(call.Arg<string>()));
        });
        tree.EntriesAsync().Returns(_ => RegistryRows(entries));

        Assert.That(await grain.GetAliasesTargetingAsync("physical"), Is.Empty);
        await grain.RegisterAsync("physical", new TreeRegistryEntry { DerivedFrom = "alpha" });
        await grain.SetAliasAsync("beta", "physical");
        await grain.SetAliasAsync("alpha", "physical");
        await grain.SetAliasAsync("alpha", "physical");
        await grain.SetAliasAsync("case-sensitive", "Physical");
        Assert.That(await grain.GetAliasesTargetingAsync("physical"), Is.EqualTo(new[] { "alpha", "beta" }));

        await grain.SetAliasAsync("alpha", "replacement");
        Assert.That(await grain.GetAliasesTargetingAsync("physical"), Is.EqualTo(new[] { "beta" }));
        Assert.That(await grain.GetAliasesTargetingAsync("replacement"), Is.EqualTo(new[] { "alpha" }));
        await grain.RemoveAliasAsync("beta");
        Assert.That(await grain.GetAliasesTargetingAsync("physical"), Is.Empty);
        await grain.UnregisterAsync("alpha");
        Assert.That(await grain.GetAliasesTargetingAsync("replacement"), Is.Empty);
    }

    [Test]
    public void GetAliasesTargetingAsync_rejects_null_before_scanning()
    {
        var (grain, tree) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.GetAliasesTargetingAsync(null!));
        Assert.That(tree.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void GetAliasesTargetingAsync_refuses_malformed_entries_instead_of_returning_an_incomplete_result()
    {
        var (grain, tree) = CreateGrain();
        tree.EntriesAsync().Returns(RegistryRows(new Dictionary<string, byte[]> { ["broken"] = "{"u8.ToArray() }));
        Assert.ThrowsAsync<JsonException>(() => grain.GetAliasesTargetingAsync("physical"));
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> RegistryRows(
        IEnumerable<KeyValuePair<string, byte[]>> entries)
    {
        foreach (var entry in entries)
            yield return entry;
        await Task.CompletedTask;
    }
}
