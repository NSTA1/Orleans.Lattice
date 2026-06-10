using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Pins the same-silo grain-proxy aliasing contract for every public/internal
/// record returned across a grain boundary. Orleans elides the same-silo deep
/// copy on any type carrying <c>[Immutable]</c> - including its nested
/// mutable reference-typed properties such as <c>byte[]</c>, <c>List&lt;T&gt;</c>,
/// <c>Dictionary&lt;TK, TV&gt;</c>, a mutable class like <c>ShardMap</c> /
/// <c>VersionVector</c>, or a <c>readonly record struct</c> whose generic
/// parameter closes over a mutable reference (e.g. <c>LwwValue&lt;byte[]&gt;</c>).
/// <para>
/// Grain code in <c>BPlusLeafGrain</c> constructs these wrappers by aliasing
/// fields directly out of <c>state.State.Entries[...]</c>; if Orleans returns
/// the same reference to the caller, a caller-side mutation silently corrupts
/// the grain's persisted state and leaks across reads on other activations.
/// </para>
/// <para>
/// <see cref="DeepCopier{T}"/> is exactly the path Orleans uses on the
/// same-silo proxy boundary, so a same-reference copy is the canonical
/// evidence of the alias. The fixture was extracted from a bug-hunter cycle
/// (Class E, "mutable type marked <c>[Immutable]</c>") in which the probe
/// confirmed aliasing across 11 measurement points spanning 10 types - nine
/// record-class wrappers and the <c>LwwValue&lt;T&gt;</c> record-struct.
/// </para>
/// </summary>
[TestFixture]
public sealed class ImmutableRecordCopyAliasingTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private DeepCopier<T> Copier<T>() => _services.GetRequiredService<DeepCopier<T>>();

    [Test]
    public void VersionedValue_deep_copy_does_not_alias_inner_byte_array()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var original = new VersionedValue { Value = bytes, Version = HybridLogicalClock.Zero };

        var copy = Copier<VersionedValue>().Copy(original);

        Assert.That(copy.Value, Is.Not.Null);
        Assert.That(
            ReferenceEquals(copy.Value, original.Value),
            Is.False,
            "VersionedValue is returned from ILattice.GetWithVersionAsync with Value aliased " +
            "to state.State.Entries[key].Value; aliasing the byte[] across the grain-proxy boundary " +
            "lets a caller mutate the grain's persisted state.");
    }

    [Test]
    public void GetOrSetResult_deep_copy_does_not_alias_inner_byte_array()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var original = new GetOrSetResult { ExistingValue = bytes };

        var copy = Copier<GetOrSetResult>().Copy(original);

        Assert.That(copy.ExistingValue, Is.Not.Null);
        Assert.That(
            ReferenceEquals(copy.ExistingValue, original.ExistingValue),
            Is.False,
            "GetOrSetResult.ExistingValue is aliased to state.State.Entries[key].Value in " +
            "BPlusLeafGrain.GetOrSetAsync; the byte[] must be deep-copied at the grain-proxy boundary.");
    }

    [Test]
    public void EntriesPage_deep_copy_does_not_alias_the_entries_list_or_inner_byte_arrays()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var entries = new List<KeyValuePair<string, byte[]>> { new("k", bytes) };
        var movedSlots = new int[] { 7 };
        var original = new EntriesPage { Entries = entries, HasMore = false, MovedAwaySlots = movedSlots };

        var copy = Copier<EntriesPage>().Copy(original);

        Assert.That(ReferenceEquals(copy, original), Is.False, "the wrapper record itself");
        Assert.That(ReferenceEquals(copy.Entries, original.Entries), Is.False, "Entries list");
        Assert.That(ReferenceEquals(copy.Entries[0].Value, original.Entries[0].Value), Is.False, "Entries[0].Value byte[]");
        Assert.That(ReferenceEquals(copy.MovedAwaySlots, original.MovedAwaySlots), Is.False, "MovedAwaySlots int[]");
    }

    [Test]
    public void KeysPage_deep_copy_does_not_alias_the_keys_list_or_moved_slots()
    {
        var keys = new List<string> { "k" };
        var movedSlots = new int[] { 7 };
        var original = new KeysPage { Keys = keys, HasMore = false, MovedAwaySlots = movedSlots };

        var copy = Copier<KeysPage>().Copy(original);

        Assert.That(ReferenceEquals(copy.Keys, original.Keys), Is.False, "Keys list");
        Assert.That(ReferenceEquals(copy.MovedAwaySlots, original.MovedAwaySlots), Is.False, "MovedAwaySlots int[]");
    }

    [Test]
    public void LatticeCursorEntriesPage_deep_copy_does_not_alias_the_entries_or_inner_byte_arrays()
    {
        var bytes = new byte[] { 1, 2, 3 };
        IReadOnlyList<KeyValuePair<string, byte[]>> entries =
            new List<KeyValuePair<string, byte[]>> { new("k", bytes) };
        var original = new LatticeCursorEntriesPage { Entries = entries, HasMore = false };

        var copy = Copier<LatticeCursorEntriesPage>().Copy(original);

        Assert.That(ReferenceEquals(copy.Entries, original.Entries), Is.False, "Entries IReadOnlyList");
        Assert.That(ReferenceEquals(copy.Entries[0].Value, original.Entries[0].Value), Is.False, "Entries[0].Value byte[]");
    }

    [Test]
    public void LatticeCursorKeysPage_deep_copy_does_not_alias_the_keys_list()
    {
        IReadOnlyList<string> keys = new List<string> { "k" };
        var original = new LatticeCursorKeysPage { Keys = keys, HasMore = false };

        var copy = Copier<LatticeCursorKeysPage>().Copy(original);

        Assert.That(ReferenceEquals(copy.Keys, original.Keys), Is.False);
    }

    [Test]
    public void RoutingInfo_deep_copy_does_not_alias_the_inner_ShardMap_instance()
    {
        var map = ShardMap.CreateDefault(8, 4);
        var original = new RoutingInfo("tree", map);

        var copy = Copier<RoutingInfo>().Copy(original);

        Assert.That(
            ReferenceEquals(copy.Map, original.Map),
            Is.False,
            "RoutingInfo is returned from ILattice.GetRoutingAsync; ShardMap is a mutable class with " +
            "Slots {get;set;} and Version {get;set;}, so aliasing the ShardMap across the grain-proxy " +
            "boundary lets the caller corrupt the registry grain's routing state.");
    }

    [Test]
    public void TreeRegistryEntry_deep_copy_does_not_alias_the_inner_ShardMap_instance()
    {
        var map = ShardMap.CreateDefault(8, 4);
        var original = new TreeRegistryEntry { ShardMap = map };

        var copy = Copier<TreeRegistryEntry>().Copy(original);

        Assert.That(ReferenceEquals(copy.ShardMap, original.ShardMap), Is.False);
    }

    [Test]
    public void StateDelta_deep_copy_does_not_alias_the_entries_dictionary_or_inner_byte_arrays()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create(bytes, HybridLogicalClock.Zero),
        };
        var original = new StateDelta
        {
            Entries = entries,
            Version = new VersionVector(),
            SplitKey = null,
        };

        var copy = Copier<StateDelta>().Copy(original);

        Assert.That(ReferenceEquals(copy.Entries, original.Entries), Is.False, "Entries Dictionary");
        Assert.That(
            ReferenceEquals(copy.Entries["k"].Value, original.Entries["k"].Value),
            Is.False,
            "StateDelta is returned from IBPlusLeafGrain.GetDeltaSinceAsync; its Dictionary aliases " +
            "state.State.Entries values and aliasing the inner byte[] lets the caller corrupt leaf state.");
    }

    [Test]
    public void LwwValue_of_byte_array_deep_copy_does_not_alias_inner_byte_array()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var original = LwwValue<byte[]>.Create(bytes, HybridLogicalClock.Zero);

        var copy = Copier<LwwValue<byte[]>>().Copy(original);

        Assert.That(copy.Value, Is.Not.Null);
        Assert.That(
            ReferenceEquals(copy.Value, original.Value),
            Is.False,
            "LwwValue<byte[]>.Value is aliased to state.State.Entries[key].Value in BPlusLeafGrain; " +
            "[Immutable] on a readonly record struct propagates to the closed generic so DeepCopier<LwwValue<byte[]>> " +
            "returns the same byte[] reference and lets a caller mutate the leaf grain's persisted state.");
    }

    [Test]
    public void LatticeTreeBatch_deep_copy_does_not_alias_the_entries_list_or_inner_byte_arrays()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var entries = new List<KeyValuePair<string, byte[]>> { new("k", bytes) };
        var original = new LatticeTreeBatch("orders", entries);

        var copy = Copier<LatticeTreeBatch>().Copy(original);

        Assert.That(ReferenceEquals(copy.Entries, original.Entries), Is.False, "Entries list");
        Assert.That(
            ReferenceEquals(copy.Entries[0].Value, original.Entries[0].Value),
            Is.False,
            "LatticeTreeBatch is passed to SetManyAtomicAcrossTreesAsync and persisted by the " +
            "cross-tree coordinator grain; if it were marked [Immutable] Orleans would alias the " +
            "caller's Entries list and inner byte[] straight into the grain's persisted state, " +
            "letting a caller-side mutation corrupt an in-flight cross-tree saga.");
    }

    [Test]
    public void LwwValue_deep_copy_does_not_alias_inner_VectorClock_instance()
    {
        var clock = new VersionVector();
        clock.MergeFrom(new VersionVector());
        var original = new LwwValue<byte[]>
        {
            Value = new byte[] { 1 },
            Timestamp = HybridLogicalClock.Zero,
            VectorClock = clock,
        };

        var copy = Copier<LwwValue<byte[]>>().Copy(original);

        Assert.That(copy.VectorClock, Is.Not.Null);
        Assert.That(
            ReferenceEquals(copy.VectorClock, original.VectorClock),
            Is.False,
            "LwwValue.VectorClock is a mutable VersionVector class. Aliasing it across the grain-proxy " +
            "boundary lets a caller mutate the leaf grain's frontier through MergeFrom.");
    }
}

