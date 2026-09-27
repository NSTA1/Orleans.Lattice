using NSubstitute;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The index tree's structural pin (issue #2829). A tree's leaf key bound is
/// seeded by its first registration and never re-seeded, and the first touch of
/// an unregistered tree registers it lazily with the core default of 128 keys -
/// an eighth of the leaf size the byte bound admits for records as large as a
/// durable index's chunks. So the pin has to carry the derived bound, and it has
/// to land before the first read or write on every path.
/// </summary>
public sealed partial class LatticeRepoContextAnnBackingFactoryTests
{
    /// <summary>A factory whose registry call is recorded and controlled by the test.</summary>
    private LatticeRepoContextAnnBackingFactory PinnedFactory(
        IndexTree tree, Func<int, Task> register, long maxLeafBytes = LatticeOptions.DefaultMaxLeafBytes)
        => new(tree.GrainFactory, _serializer, Options(maxLeafBytes), register);

    private static int GetCalls(IndexTree tree)
        => tree.Tree.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILattice.GetAsync));

    [Test]
    public async Task The_index_tree_is_registered_with_the_leaf_key_bound_derived_from_its_byte_bound()
    {
        var pinned = new List<int>();
        var factory = PinnedFactory(new IndexTree(), keys =>
        {
            pinned.Add(keys);
            return Task.CompletedTask;
        });

        await factory.EnsureIndexTreePinnedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pinned, Is.EqualTo(new[] { 1024 }),
                "at the default 64 MiB byte bound the index tree must be pinned at 1024 keys per leaf, "
                + "not the core default of 128 that splits a leaf at about 8 MiB");
            Assert.That(factory.IndexTreeMaxLeafKeys,
                Is.EqualTo(DurableVectorIndexOptions.ResolveMaxLeafKeys(LatticeOptions.DefaultMaxLeafBytes)));
        });
    }

    [Test]
    public async Task The_pin_tracks_a_non_default_byte_bound_on_the_index_tree()
    {
        var pinned = new List<int>();
        const long maxLeafBytes = 128L * 1024 * 1024;
        var factory = PinnedFactory(new IndexTree(), keys =>
        {
            pinned.Add(keys);
            return Task.CompletedTask;
        }, maxLeafBytes);

        await factory.EnsureIndexTreePinnedAsync();

        Assert.That(pinned, Is.EqualTo(new[] { DurableVectorIndexOptions.ResolveMaxLeafKeys(maxLeafBytes) }));
        Assert.That(pinned[0], Is.EqualTo(2048), "doubling the byte bound doubles the key bound");
    }

    [Test]
    public async Task The_pin_is_registered_once_per_factory()
    {
        var registrations = 0;
        var tree = new IndexTree();
        var factory = PinnedFactory(tree, _ =>
        {
            registrations++;
            return Task.CompletedTask;
        });

        await factory.CreateStore(RepoId, LiveSpace).ReadAsync("a", Ct);
        await factory.CreateStore(RepoId, OldSpace).ReadAsync("b", Ct);
        await factory.ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        Assert.That(registrations, Is.EqualTo(1));
    }

    [Test]
    public async Task No_store_operation_reaches_the_index_tree_before_the_pin_lands()
    {
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tree = new IndexTree();
        var store = PinnedFactory(tree, _ => registration.Task).CreateStore(RepoId, LiveSpace);

        var read = store.ReadAsync("key", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(read.IsCompleted, Is.False, "the read must wait for the pin");
            Assert.That(GetCalls(tree), Is.Zero,
                "a read that reached the tree first would register it lazily with the core default bound");
        });

        registration.SetResult();
        await read;

        Assert.That(GetCalls(tree), Is.EqualTo(1));
    }

    [Test]
    public async Task Reclamation_waits_for_the_pin_before_its_first_scan()
    {
        var registration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tree = new IndexTree();
        tree.PutSpace(RepoId, OldSpace, 2);
        tree.PutSpace(RepoId, LiveSpace, 2);

        var reclaim = PinnedFactory(tree, _ => registration.Task)
            .ReclaimSupersededSpacesAsync(RepoId, LiveSpace, Ct);

        var scans = tree.Tree.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILattice.KeysAsync));
        Assert.That(scans, Is.Zero, "the key scan is itself a first touch of the index tree");

        registration.SetResult();
        Assert.That(await reclaim, Is.EqualTo(1));
    }

    [Test]
    public async Task A_faulted_pin_is_retried_on_the_next_operation_rather_than_cached()
    {
        var attempts = 0;
        var tree = new IndexTree();
        var store = PinnedFactory(tree, _ => ++attempts == 1
                ? Task.FromException(new InvalidOperationException("registry unavailable"))
                : Task.CompletedTask)
            .CreateStore(RepoId, LiveSpace);

        Assert.That(async () => await store.ReadAsync("key", Ct), Throws.InvalidOperationException);
        Assert.That(GetCalls(tree), Is.Zero, "an operation whose pin faulted must not reach the tree");

        await store.ReadAsync("key", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(attempts, Is.EqualTo(2), "one transient registry fault must not fail every later operation");
            Assert.That(GetCalls(tree), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_default_registration_goes_through_the_tree_registry()
    {
        var tree = new IndexTree();
        var factory = new LatticeRepoContextAnnBackingFactory(tree.GrainFactory, _serializer, Options());

        await factory.EnsureIndexTreePinnedAsync();

        // The registry and its entry type are internal to the core assembly, so the
        // call is read back by reflection: the grain factory substitute hands the
        // same recursive substitute back for the same key, and its recorded calls
        // carry the registration.
        var registryType = typeof(LatticeOptions).Assembly.GetType("Orleans.Lattice.BPlusTree.ILatticeRegistry", throwOnError: true)!;
        var getGrain = typeof(IGrainFactory).GetMethods()
            .Single(m => m.Name == nameof(IGrainFactory.GetGrain)
                && m.IsGenericMethodDefinition
                && m.GetParameters().Select(p => p.ParameterType).SequenceEqual([typeof(string), typeof(string)]))
            .MakeGenericMethod(registryType);
        var registry = getGrain.Invoke(tree.GrainFactory, ["_lattice_trees", null])!;

        var register = registry.ReceivedCalls().Single(c => c.GetMethodInfo().Name == "RegisterAsync");
        var arguments = register.GetArguments();
        var maxLeafKeys = arguments[1]!.GetType().GetProperty("MaxLeafKeys")!.GetValue(arguments[1]);

        Assert.Multiple(() =>
        {
            Assert.That(arguments[0], Is.EqualTo(RepoContextTrees.VectorIndex));
            Assert.That(maxLeafKeys, Is.EqualTo(1024));
        });
    }
}
