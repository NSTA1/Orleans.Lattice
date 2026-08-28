using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// Regression cover for the read-side-effect defect: every documented read-only
/// data verb, and the two delete verbs whose contract calls an unknown tree a
/// routine no-op, must answer without <b>registering</b> the tree.
/// </summary>
/// <remarks>
/// Routing an operation into a tree's shard root activates the grains behind it,
/// and the options resolver those grains go through lazily seeds a durable
/// registry entry for a tree that has no structural pin. A plain read of a name
/// nobody created therefore created it, with a full default shard configuration -
/// an unbounded catalogue-growth vector, and a way for a caller holding only read
/// grants to provision trees it is not permitted to create. Each test below asserts
/// both halves: the documented empty answer, <em>and</em> that the registry is
/// still unaware of the tree afterwards.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class DataApiReadSideEffectTests
{
    private CrdtApiDataClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new CrdtApiDataClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    private ILatticeDataApi Api => _fixture.Api;

    private Task<bool> RegisteredAsync(string treeId) =>
        _fixture.Cluster.Client
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .ExistsAsync(treeId);

    /// <summary>A tree id that is never created, unique per test so tests cannot mask each other.</summary>
    private static string Absent(string discriminator) => $"absent-{discriminator}-{Guid.NewGuid():N}";

    [Test]
    public async Task GCounterGetAsync_on_an_unknown_tree_reads_zero_without_registering_it()
    {
        var tree = Absent("gcounter");

        var value = await Api.GCounterGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.Zero);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task CounterGetAsync_on_an_unknown_tree_reads_zero_without_registering_it()
    {
        var tree = Absent("pncounter");

        var value = await Api.CounterGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.Zero);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task GSetGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("gset");

        var elements = await Api.GSetGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(elements, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task SetGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("orset");

        var elements = await Api.SetGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(elements, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task RwSetGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("rwset");

        var elements = await Api.RwSetGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(elements, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task OrFlagGetAsync_on_an_unknown_tree_reads_false_without_registering_it()
    {
        var tree = Absent("orflag");

        var enabled = await Api.OrFlagGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(enabled, Is.False);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task RwFlagGetAsync_on_an_unknown_tree_reads_false_without_registering_it()
    {
        var tree = Absent("rwflag");

        var enabled = await Api.RwFlagGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(enabled, Is.False);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task RegisterGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("mvregister");

        var values = await Api.RegisterGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task MaxRegisterGetAsync_on_an_unknown_tree_reads_null_without_registering_it()
    {
        var tree = Absent("maxregister");

        var value = await Api.MaxRegisterGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.Null);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task MinRegisterGetAsync_on_an_unknown_tree_reads_null_without_registering_it()
    {
        var tree = Absent("minregister");

        var value = await Api.MinRegisterGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.Null);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task MapGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("ormap");

        var fields = await Api.MapGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(fields, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task SequenceGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("sequence");

        var values = await Api.SequenceGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task VersionVectorGetAsync_on_an_unknown_tree_reads_empty_without_registering_it()
    {
        var tree = Absent("versionvector");

        var clock = await Api.VersionVectorGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(clock, Is.Empty);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task DeleteAsync_on_an_unknown_tree_reports_nothing_deleted_without_registering_it()
    {
        var tree = Absent("delete");

        var deleted = await Api.DeleteAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.False);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task DeleteRangeAsync_on_an_unknown_tree_reports_nothing_deleted_without_registering_it()
    {
        var tree = Absent("deleterange");

        var result = await Api.DeleteRangeAsync(new DataRangeDeleteRequest
        {
            TreeId = tree,
            StartInclusive = "a",
            EndExclusive = "z",
        });

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo(tree));
            Assert.That(result.DeletedCount, Is.Zero);
            Assert.That(registered, Is.False);
        });
    }

    [Test]
    public async Task A_registered_tree_still_serves_its_crdt_reads_after_the_existence_probe()
    {
        // Guards against the probe short-circuiting a tree that genuinely exists:
        // the documented empty answer must be reserved for an absent tree only.
        var tree = "read-side-effect-live";
        await _fixture.RegisterTreeAsync(tree);
        await Api.GCounterIncrementAsync(tree, "k", "replica-a", 7);

        var value = await Api.GCounterGetAsync(tree, "k");

        var registered = await RegisteredAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(7));
            Assert.That(registered, Is.True);
        });
    }

    [Test]
    public async Task DeleteAsync_on_a_registered_tree_still_retracts_the_key()
    {
        var tree = "read-side-effect-delete-live";
        await _fixture.RegisterTreeAsync(tree);
        await Api.SetAsync(tree, "k", new byte[] { 1 });

        var deleted = await Api.DeleteAsync(tree, "k");
        var read = await Api.GetAsync(tree, "k");

        Assert.Multiple(() =>
        {
            Assert.That(deleted, Is.True);
            Assert.That(read.Found, Is.False);
        });
    }
}
