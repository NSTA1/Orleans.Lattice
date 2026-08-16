namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextTrees"/>: the one-named-tree-per-CRDT-family
/// map, including the structural / memory routing and the reserved vector trees.
/// </summary>
[TestFixture]
public sealed class RepoContextTreesTests
{
    [Test]
    public void Structural_kinds_map_to_the_structural_tree()
    {
        var structuralKinds = new[]
        {
            RepoContextRecordKind.Repo,
            RepoContextRecordKind.Package,
            RepoContextRecordKind.File,
        };

        Assert.Multiple(() =>
        {
            foreach (var kind in structuralKinds)
            {
                Assert.That(RepoContextTrees.ForKind(kind), Is.EqualTo(RepoContextTrees.Structural),
                    $"{kind} should map to the structural tree");
            }
        });
    }

    [Test]
    public void Symbol_kind_maps_to_the_dedicated_symbol_tree()
        => Assert.That(RepoContextTrees.ForKind(RepoContextRecordKind.Symbol),
            Is.EqualTo(RepoContextTrees.Symbol));

    [Test]
    public void Memory_kind_maps_to_the_memory_tree()
        => Assert.That(RepoContextTrees.ForKind(RepoContextRecordKind.Memory),
            Is.EqualTo(RepoContextTrees.Memory));

    [Test]
    public void ForKind_rejects_an_unknown_kind()
        => Assert.That(() => RepoContextTrees.ForKind((RepoContextRecordKind)999),
            Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void Content_kind_maps_to_the_content_tree()
        => Assert.That(RepoContextTrees.ForKind(RepoContextRecordKind.Content),
            Is.EqualTo(RepoContextTrees.Content));

    [Test]
    public void All_contains_every_named_tree_with_no_duplicates()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTrees.All, Is.EquivalentTo(new[]
            {
                RepoContextTrees.Structural,
                RepoContextTrees.Symbol,
                RepoContextTrees.Content,
                RepoContextTrees.Memory,
                RepoContextTrees.VectorMembership,
                RepoContextTrees.VectorPayload,
                RepoContextTrees.VectorMetadata,
            }));
            Assert.That(RepoContextTrees.All, Is.Unique);
        });
    }

    [Test]
    public void Tree_names_are_non_empty()
        => Assert.That(RepoContextTrees.All, Is.All.Not.Empty);
}
