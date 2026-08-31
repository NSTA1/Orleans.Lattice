using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The maintainer's contract with the tree: one all-or-nothing batch per
/// update, upserts and tombstones travelling together, and no round trip at all
/// when nothing changed. The tree is a substitute, so these are ordinary unit
/// tests - no cluster, no timing, no waiting.
/// </summary>
[TestFixture]
public class GrainIndexMaintainerTests
{
    private ILattice _tree = null!;
    private GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState> _maintainer = null!;

    [SetUp]
    public void SetUp()
    {
        _tree = Substitute.For<ILattice>();
        _maintainer = new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(
            IndexedTestIndex.Definition(),
            _tree);
    }

    private static IndexedTestState State(int age = 17) => new()
    {
        Age = age,
        Country = "GB",
        LastSeen = new DateTimeOffset(2026, 8, 31, 9, 45, 57, TimeSpan.Zero),
        Status = TestStatus.Active,
    };

    [Test]
    public async Task A_first_update_writes_every_entry_in_one_upsert_only_batch()
    {
        var projection = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State());

        await _tree.Received(1).SetManyAtomicAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(e => e.Count == 4),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
        await _tree.DidNotReceive().SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
        Assert.That(projection.Entries, Has.Count.EqualTo(4));
    }

    [Test]
    public async Task A_moved_value_sends_its_upsert_and_its_tombstone_in_the_same_batch()
    {
        var first = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State(age: 17));
        _tree.ClearReceivedCalls();

        await _maintainer.UpdateAsync(first, "alice", State(age: 18));

        await _tree.Received(1).SetManyAtomicAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(u => u.Count == 1),
            Arg.Is<IReadOnlyList<string>>(d => d.Count == 1),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_unchanged_update_never_reaches_the_tree()
    {
        var first = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State());
        _tree.ClearReceivedCalls();

        var second = await _maintainer.UpdateAsync(first, "alice", State());

        Assert.That(_tree.ReceivedCalls(), Is.Empty);
        Assert.That(second.Entries, Is.EqualTo(first.Entries));
    }

    [Test]
    public async Task An_update_returns_the_projection_to_persist_as_the_next_baseline()
    {
        var first = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State(age: 17));
        var second = await _maintainer.UpdateAsync(first, "alice", State(age: 18));
        _tree.ClearReceivedCalls();

        await _maintainer.UpdateAsync(second, "alice", State(age: 18));

        Assert.That(_tree.ReceivedCalls(), Is.Empty, "the returned projection must reflect what was written");
    }

    [Test]
    public async Task Removing_a_grain_sends_a_delete_only_batch()
    {
        var projection = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State());
        _tree.ClearReceivedCalls();

        await _maintainer.RemoveAsync(projection);

        await _tree.Received(1).SetManyAtomicAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(u => u.Count == 0),
            Arg.Is<IReadOnlyList<string>>(d => d.Count == 4),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Removing_a_grain_that_was_never_indexed_never_reaches_the_tree()
    {
        await _maintainer.RemoveAsync(GrainIndexProjection.Empty("alice"));

        Assert.That(_tree.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task Removing_the_same_projection_twice_is_idempotent_from_the_callers_side()
    {
        var projection = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State());
        _tree.ClearReceivedCalls();

        await _maintainer.RemoveAsync(projection, "op-1");
        await _maintainer.RemoveAsync(projection, "op-1");

        await _tree.Received(2).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<IReadOnlyList<string>>(),
            "op-1",
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_supplied_operation_id_is_passed_straight_through()
    {
        await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State(), "op-42");

        await _tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            "op-42",
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_generated_operation_id_is_fresh_per_call_and_carries_no_reserved_separator()
    {
        var captured = new List<string>();
        await _tree.SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Do<string>(captured.Add),
            Arg.Any<CancellationToken>());
        _tree.ClearReceivedCalls();

        await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State());
        await _maintainer.UpdateAsync(GrainIndexProjection.Empty("bob"), "bob", State());

        Assert.That(captured, Has.Count.EqualTo(2));
        Assert.That(captured[0], Is.Not.EqualTo(captured[1]));
        foreach (var id in captured)
        {
            Assert.That(id, Is.Not.Empty);
            Assert.That(id, Does.Not.Contain("/"));
        }
    }

    [Test]
    public async Task The_cancellation_token_is_passed_to_the_tree()
    {
        using var cts = new CancellationTokenSource();

        await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", State(), null, cts.Token);

        await _tree.Received(1).SetManyAtomicAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(),
            Arg.Any<string>(),
            cts.Token);
    }

    [Test]
    public async Task Applying_an_empty_plan_never_reaches_the_tree()
    {
        await _maintainer.ApplyAsync(GrainIndexUpdatePlan.Between(
            GrainIndexProjection.Empty("alice"),
            GrainIndexProjection.Empty("alice")));

        Assert.That(_tree.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task Updating_from_a_grain_id_matches_the_string_key_overload()
    {
        var grainId = Orleans.Runtime.GrainId.Create("test", "alice");

        var projection = await _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), grainId, State());

        Assert.That(projection.GrainKey, Is.EqualTo("alice"));
    }

    [Test]
    public void The_maintainer_exposes_its_projector_and_tree()
    {
        Assert.That(_maintainer.Tree, Is.SameAs(_tree));
        Assert.That(_maintainer.Projector.Definition.Name, Is.EqualTo("Subjects"));
    }

    [Test]
    public void The_options_constructor_resolves_the_tree_named_by_the_index_options()
    {
        var definition = IndexedTestIndex.Definition("Subjects");
        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        options.Get("Subjects").Returns(new GrainIndexOptions
        {
            TreeName = GrainIndexTreeNames.ForIndex("Subjects"),
        });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex("Subjects"), null).Returns(_tree);

        var maintainer = new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(definition, factory, options);

        Assert.That(maintainer.Tree, Is.SameAs(_tree));
    }

    [Test]
    public void The_maintainer_rejects_null_constructor_arguments()
    {
        var definition = IndexedTestIndex.Definition();
        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        var factory = Substitute.For<IGrainFactory>();

        Assert.That(
            () => new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(null!, _tree),
            Throws.ArgumentNullException);
        Assert.That(
            () => new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(definition, (ILattice)null!),
            Throws.ArgumentNullException);
        Assert.That(
            () => new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(null!, factory, options),
            Throws.ArgumentNullException);
        Assert.That(
            () => new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(definition, null!, options),
            Throws.ArgumentNullException);
        Assert.That(
            () => new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(definition, factory, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_maintainer_rejects_null_method_arguments()
    {
        Assert.That(() => _maintainer.ApplyAsync(null!), Throws.ArgumentNullException);
        Assert.That(() => _maintainer.RemoveAsync(null!), Throws.ArgumentNullException);
        Assert.That(
            () => _maintainer.UpdateAsync(null!, "alice", State()),
            Throws.ArgumentNullException);
        Assert.That(
            () => _maintainer.UpdateAsync(GrainIndexProjection.Empty("alice"), "alice", null!),
            Throws.ArgumentNullException);
    }
}
