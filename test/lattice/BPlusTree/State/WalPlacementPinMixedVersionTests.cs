using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// A placement pin written by a build that holds durable WAL move fences
/// (issue #4525) must stay readable by a silo built before them, in both shapes
/// the pin travels in: the Orleans wire format (a registry reply to a WAL shard
/// activation) and the registry's persisted JSON. The older silo sees no fence,
/// which is why a WAL move requires every silo on the fence-aware version, but
/// the read itself must not fail.
/// </summary>
[TestFixture]
public sealed class WalPlacementPinMixedVersionTests
{
    private static WalPlacementPin FencedPin() =>
        WalPlacementPin.Create()
            .WithPartition(1, "secondary", 3)
            .WithFence(0, new WalMoveFence { MoveId = "move-a", SourceProviderKey = "default", LeaseExpiresUtcTicks = 42 });

    [Test]
    public void An_older_silo_reads_a_fenced_pin_from_the_wire_and_ignores_the_fence()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var bytes = serializer.SerializeToArray(new CurrentPinHolder { Pin = FencedPin() });

        var legacy = serializer.Deserialize<LegacyPinHolder>(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(legacy.Pin, Is.Not.Null);
            Assert.That(legacy.Pin!.Version, Is.EqualTo(3));
            Assert.That(legacy.Pin.DefaultProviderKey, Is.EqualTo("default"));
            Assert.That(legacy.Pin.Overrides, Is.EquivalentTo(new Dictionary<int, string> { [1] = "secondary" }));
            Assert.That(legacy.Tail, Is.EqualTo("after-the-pin"), "skipping the unknown fence field must not misalign later fields");
        });
    }

    [Test]
    public void An_older_silo_reads_a_fenced_registry_entry_from_its_json_and_ignores_the_fence()
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(
            new TreeRegistryEntry { ShardCount = 1, WalPlacement = FencedPin() },
            RegistryEntryContext.Default.TreeRegistryEntry);
        Assert.That(System.Text.Encoding.UTF8.GetString(json), Does.Contain("Fences"),
            "the fence is persisted with the pin");

        var legacy = JsonSerializer.Deserialize<LegacyRegistryEntry>(json);

        Assert.Multiple(() =>
        {
            Assert.That(legacy!.WalPlacement!.Version, Is.EqualTo(3));
            Assert.That(legacy.WalPlacement.Overrides, Is.EquivalentTo(new Dictionary<int, string> { [1] = "secondary" }));
        });
    }

    [Test]
    public void A_pin_written_before_fences_existed_reads_as_unfenced()
    {
        var legacyJson = """{"ShardCount":1,"WalPlacement":{"Version":2,"DefaultProviderKey":"default","Overrides":{"0":"secondary"}}}""";

        var entry = JsonSerializer.Deserialize(legacyJson, RegistryEntryContext.Default.TreeRegistryEntry);

        Assert.Multiple(() =>
        {
            Assert.That(entry!.WalPlacement!.Version, Is.EqualTo(2));
            Assert.That(entry.WalPlacement.ResolveKey(0), Is.EqualTo("secondary"));
            Assert.That(entry.WalPlacement.Fences, Is.Null);
            Assert.That(entry.WalPlacement.ResolveFence(0), Is.Null);
        });
    }

    /// <summary>Carries the current pin, followed by a field after it.</summary>
    [GenerateSerializer]
    internal sealed class CurrentPinHolder
    {
        [Id(0)] public WalPlacementPin? Pin { get; set; }
        [Id(1)] public string Tail { get; set; } = "after-the-pin";
    }

    /// <summary>The same holder as a pre-#4525 silo sees it.</summary>
    [GenerateSerializer]
    internal sealed class LegacyPinHolder
    {
        [Id(0)] public LegacyPin? Pin { get; set; }
        [Id(1)] public string Tail { get; set; } = "";
    }

    /// <summary><see cref="WalPlacementPin"/> as it was before the fence slot ([Id(3)]) was added.</summary>
    [GenerateSerializer]
    internal sealed record LegacyPin
    {
        [Id(0)] public long Version { get; init; }
        [Id(1)] public string DefaultProviderKey { get; init; } = "";
        [Id(2)] public Dictionary<int, string>? Overrides { get; init; }
    }

    /// <summary>The registry entry JSON shape as a pre-#4525 silo binds it.</summary>
    internal sealed class LegacyRegistryEntry
    {
        public LegacyPin? WalPlacement { get; set; }
    }
}
