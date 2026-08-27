using System.Text.Json;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="ILatticeRegistry.GetEntriesAsync"/> - the batched
/// counterpart to <see cref="ILatticeRegistry.GetEntryAsync"/>. Pins the three
/// properties callers depend on: the caller pays one round-trip for a whole
/// page, the backing reads are issued as one concurrent wave rather than N
/// sequential awaits, and unregistered ids are simply absent from the result
/// rather than mapped to a null value.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    private static byte[] Serialize(TreeRegistryEntry entry) =>
        JsonSerializer.SerializeToUtf8Bytes(entry);

    [Test]
    public async Task GetEntriesAsync_returns_entries_keyed_by_tree_id()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("alpha").Returns(Task.FromResult<byte[]?>(Serialize(new TreeRegistryEntry { ShardCount = 3 })));
        tree.GetAsync("beta").Returns(Task.FromResult<byte[]?>(Serialize(new TreeRegistryEntry { ShardCount = 7 })));

        var entries = await grain.GetEntriesAsync(["alpha", "beta"]);

        Assert.Multiple(() =>
        {
            Assert.That(entries["alpha"].ShardCount, Is.EqualTo(3));
            Assert.That(entries["beta"].ShardCount, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task GetEntriesAsync_reads_every_requested_id_exactly_once()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));

        await grain.GetEntriesAsync(["a", "b", "c", "d"]);

        await tree.Received(1).GetAsync("a");
        await tree.Received(1).GetAsync("b");
        await tree.Received(1).GetAsync("c");
        await tree.Received(1).GetAsync("d");
    }

    [Test]
    public void The_system_tree_facade_deliberately_exposes_no_multi_get()
    {
        // Deadlock regression, pinned structurally because the type system is
        // what enforces it. Routing GetEntriesAsync through a system-tree
        // GetManyAsync looks like the obvious primitive, but
        // LatticeGrain.GetManyAsyncCore ends every attempt with an
        // unconditional ILatticeRegistry.GetShardMapAsync topology re-probe.
        // Issued from inside LatticeRegistryGrain that closes a two-hop cycle
        // back onto this non-reentrant activation, which is still executing the
        // very turn that is awaiting - so the probe queues behind it forever and
        // the call times out. The single-key ISystemLattice.GetAsync carries no
        // such re-probe, which is why the per-entry GetEntryAsync path has
        // always worked from here. If a future change adds a multi-get to the
        // system-tree facade, this test fires: read this comment before wiring
        // it into the registry grain.
        var multiGet = typeof(ISystemLattice).GetMethod("GetManyAsync");

        Assert.That(multiGet, Is.Null);
    }

    [Test]
    public async Task GetEntriesAsync_issues_the_reads_as_one_concurrent_wave()
    {
        // The registry-side half of the win: N sequential awaits become one
        // wave. Every read must be in flight before any of them completes, so
        // the page costs one read latency rather than N of them.
        var (grain, tree) = CreateGrain();
        var gates = new Dictionary<string, TaskCompletionSource<byte[]?>>(StringComparer.Ordinal)
        {
            ["a"] = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ["b"] = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ["c"] = new(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        foreach (var (id, gate) in gates)
        {
            tree.GetAsync(id).Returns(gate.Task);
        }

        var pending = grain.GetEntriesAsync(["a", "b", "c"]);

        // Not one read has been allowed to complete, yet all three were issued.
        Assert.That(pending.IsCompleted, Is.False);
        await tree.Received(1).GetAsync("a");
        await tree.Received(1).GetAsync("b");
        await tree.Received(1).GetAsync("c");

        foreach (var gate in gates.Values)
        {
            gate.SetResult(Serialize(new TreeRegistryEntry { ShardCount = 1 }));
        }

        Assert.That(await pending, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task GetEntriesAsync_omits_unregistered_ids()
    {
        // A key that does not exist (or is tombstoned) reads back as null and is
        // absent from the result, never present with a null value.
        var (grain, tree) = CreateGrain();
        tree.GetAsync("known").Returns(Task.FromResult<byte[]?>(Serialize(new TreeRegistryEntry { ShardCount = 1 })));
        tree.GetAsync("missing").Returns(Task.FromResult<byte[]?>(null));

        var entries = await grain.GetEntriesAsync(["known", "missing"]);

        Assert.Multiple(() =>
        {
            Assert.That(entries.ContainsKey("known"), Is.True);
            Assert.That(entries.ContainsKey("missing"), Is.False);
            Assert.That(entries, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task GetEntriesAsync_returns_empty_for_an_empty_list_without_touching_the_registry_tree()
    {
        var (grain, tree) = CreateGrain();

        var entries = await grain.GetEntriesAsync([]);

        Assert.That(entries, Is.Empty);
        await tree.DidNotReceive().GetAsync(Arg.Any<string>());
    }

    [Test]
    public void GetEntriesAsync_throws_for_null_tree_ids()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.GetEntriesAsync(null!),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void GetEntriesAsync_throws_for_a_null_id_in_the_list()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.GetEntriesAsync(["ok", null!]),
            Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public async Task GetEntriesAsync_agrees_with_GetEntryAsync_for_the_same_id()
    {
        // The batched member is a call-shape optimisation, so a single id read
        // either way must deserialize to the same entry shape.
        var (grain, tree) = CreateGrain();
        var stored = Serialize(new TreeRegistryEntry
        {
            ShardCount = 5,
            MaxLeafKeys = 128,
            PhysicalTreeId = "physical",
        });
        tree.GetAsync("solo").Returns(Task.FromResult<byte[]?>(stored));

        var single = await grain.GetEntryAsync("solo");
        var batched = await grain.GetEntriesAsync(["solo"]);

        Assert.That(batched["solo"], Is.EqualTo(single));
    }

    [Test]
    public async Task GetEntriesAsync_collapses_a_duplicated_id_to_one_entry()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("dup").Returns(Task.FromResult<byte[]?>(Serialize(new TreeRegistryEntry { ShardCount = 2 })));

        var entries = await grain.GetEntriesAsync(["dup", "dup"]);

        Assert.Multiple(() =>
        {
            Assert.That(entries, Has.Count.EqualTo(1));
            Assert.That(entries["dup"].ShardCount, Is.EqualTo(2));
        });
    }
}
