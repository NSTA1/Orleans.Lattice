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
                RepoContextTrees.CrossReference,
                RepoContextTrees.Memory,
                RepoContextTrees.Session,
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

    [Test]
    [TestCase("repo-context-vector-metadata")]
    [TestCase("repo-context-vector-membership")]
    public void IsRebuildableVectorTree_is_true_for_the_two_rebuildable_vector_projections(string treeName)
        => Assert.That(RepoContextTrees.IsRebuildableVectorTree(treeName), Is.True);

    [Test]
    [TestCase("repo-context-vector-payload")]
    [TestCase("repo-context-structural")]
    [TestCase("repo-context-symbol")]
    [TestCase("repo-context-memory")]
    [TestCase("repo-context-content")]
    [TestCase("repo-context-xref")]
    [TestCase("repo-context-session")]
    [TestCase("some-unknown-tree")]
    [TestCase("")]
    public void IsRebuildableVectorTree_is_false_for_every_other_tree_failing_closed(string treeName)
        => Assert.That(RepoContextTrees.IsRebuildableVectorTree(treeName), Is.False);

    [Test]
    public void IsRebuildableVectorTree_is_false_for_null_failing_closed()
        => Assert.That(RepoContextTrees.IsRebuildableVectorTree(null), Is.False);

    [Test]
    public void CodeIndexTrees_covers_all_non_memory_trees_and_no_more()
    {
        // Invariant: the code-only reset sweep list, together with the Memory
        // store-of-record tree, must partition AllIncludingLocalDerived exactly.
        // A tree added to AllIncludingLocalDerived must be classified explicitly
        // as either a code-index tree (dropped by reset_index) or as memory
        // (preserved). This test fails the build on any drift, so the classifier
        // can never default a new tree into either bucket.
        var expected = RepoContextTrees.AllIncludingLocalDerived
            .Where(name => !string.Equals(name, RepoContextTrees.Memory, StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTrees.CodeIndexTrees, Is.EquivalentTo(expected));
            Assert.That(RepoContextTrees.CodeIndexTrees, Does.Not.Contain(RepoContextTrees.Memory),
                "The Memory tree is store-of-record and must never appear in the code-index sweep list.");
            Assert.That(RepoContextTrees.CodeIndexTrees, Is.Unique);
        });
    }

    [Test]
    [TestCase("repo-context-structural")]
    [TestCase("repo-context-symbol")]
    [TestCase("repo-context-content")]
    [TestCase("repo-context-xref")]
    [TestCase("repo-context-session")]
    [TestCase("repo-context-vector-membership")]
    [TestCase("repo-context-vector-payload")]
    [TestCase("repo-context-vector-metadata")]
    [TestCase("repo-context-vector-index")]
    public void IsCodeIndexTree_is_true_for_every_code_index_tree(string treeName)
        => Assert.That(RepoContextTrees.IsCodeIndexTree(treeName), Is.True);

    [Test]
    [TestCase("repo-context-memory")]
    [TestCase("some-unknown-tree")]
    [TestCase("")]
    public void IsCodeIndexTree_is_false_for_memory_and_unknown_trees_failing_closed(string treeName)
        => Assert.That(RepoContextTrees.IsCodeIndexTree(treeName), Is.False);

    [Test]
    public void IsCodeIndexTree_is_false_for_null_failing_closed()
        => Assert.That(RepoContextTrees.IsCodeIndexTree(null), Is.False);

    /// <summary>
    /// VectorPayload is content-addressed and write-once, so the self-healer's
    /// RebuildableVectorTrees allow-list deliberately excludes it (a drop-and-
    /// re-embed cannot re-derive it on its own). The operator-invoked code-only
    /// reset makes the opposite judgement: the operator has consented to the
    /// re-embedding cost and the follow-up ingest is what pays it, so the
    /// payload tree is included in the sweep. This test pins the difference so
    /// the two lists cannot be silently unified.
    /// </summary>
    [Test]
    public void CodeIndexTrees_includes_vector_payload_which_the_self_healer_allow_list_excludes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextTrees.CodeIndexTrees, Does.Contain(RepoContextTrees.VectorPayload));
            Assert.That(RepoContextTrees.IsRebuildableVectorTree(RepoContextTrees.VectorPayload), Is.False,
                "The self-healer must not silently drop the write-once payload tree.");
        });
    }
}
