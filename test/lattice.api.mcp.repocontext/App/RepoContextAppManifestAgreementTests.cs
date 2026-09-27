using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The CI regression that keeps the embedded app manifest and the
/// <see cref="RepoContextTrees"/> constants in agreement, following the mapping-test pattern
/// described on <c>LatticeDashboards</c>: a tree constant renamed, added or removed without
/// the manifest following it fails here rather than shipping a stale manifest.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tree set.</b> The manifest adopts every tree that holds a repository's data -
/// <see cref="RepoContextTrees.AllIncludingLocalDerived"/>, not the replication enrolment list
/// <see cref="RepoContextTrees.All"/> - because the app owns the local-derived trees too.
/// </para>
/// <para>
/// <b>Rebuildable mapping.</b> A tree is <c>rebuildable</c> (rederived after a restore rather
/// than restored from bytes) exactly when the constants already classify it as derived:
/// </para>
/// <list type="bullet">
/// <item><description><see cref="RepoContextTrees.IsRebuildableVectorTree"/> - vector metadata, vector membership and the vector-coverage digest, which the self-healer may drop and re-embed;</description></item>
/// <item><description><see cref="RepoContextTrees.LocalDerived"/> - the approximate vector index and the vector-coverage digest, wholly recomputable from the other vector trees.</description></item>
/// </list>
/// <para>
/// Everything else restores from bytes. That includes agent memory (store of record), the
/// content-addressed write-once vector payload tree (excluded from the rebuildable allow-list
/// because a drop-and-re-embed cannot re-derive it), and the code-index trees
/// (<see cref="RepoContextTrees.CodeIndexTrees"/>). The code-index classification is
/// deliberately not treated as rederivable: re-deriving it takes an operator-consented
/// re-ingest from the working files, which a restore cannot assume are present or unchanged.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextAppManifestAgreementTests
{
    private static AppManifest Manifest() => RepoContextAppManifest.Load().Manifest!;

    private static bool ConstantsClassifyAsRebuildable(string treeId)
        => RepoContextTrees.IsRebuildableVectorTree(treeId) || RepoContextTrees.LocalDerived.Contains(treeId);

    /// <summary>
    /// The comparison the regression runs: every disagreement between the manifest's adopted
    /// trees and an expected tree set with its rebuildable classification.
    /// </summary>
    private static IReadOnlyList<string> Disagreements(
        AppManifest manifest,
        IReadOnlyList<string> expectedTrees,
        Func<string, bool> isRebuildable)
    {
        var disagreements = new List<string>();
        var declared = new Dictionary<string, AppTreeDeclaration>(StringComparer.Ordinal);
        foreach (var tree in manifest.Trees)
        {
            if (tree.AdoptedTreeId is not { } adopted)
            {
                disagreements.Add($"tree '{tree.Name}' does not adopt a physical tree");
                continue;
            }

            declared[adopted] = tree;
            if (!expectedTrees.Contains(adopted, StringComparer.Ordinal))
            {
                disagreements.Add($"manifest adopts '{adopted}', which is not a repository-context tree constant");
            }
        }

        foreach (var expected in expectedTrees)
        {
            if (!declared.TryGetValue(expected, out var tree))
            {
                disagreements.Add($"tree constant '{expected}' is not adopted by the manifest");
            }
            else if (tree.Rebuildable != isRebuildable(expected))
            {
                disagreements.Add($"tree '{expected}' is declared rebuildable={tree.Rebuildable} but the constants classify it as {isRebuildable(expected)}");
            }
        }

        return disagreements;
    }

    [Test]
    public void Manifest_agrees_with_the_tree_constants()
        => Assert.That(
            Disagreements(Manifest(), RepoContextTrees.AllIncludingLocalDerived, ConstantsClassifyAsRebuildable),
            Is.Empty);

    [Test]
    public void Adopted_tree_ids_equal_every_tree_that_holds_repository_data()
        => Assert.That(
            Manifest().Trees.Select(t => t.AdoptedTreeId).Order(StringComparer.Ordinal),
            Is.EqualTo(RepoContextTrees.AllIncludingLocalDerived.Order(StringComparer.Ordinal)));

    [Test]
    public void Every_declared_tree_adopts_a_legacy_tree_so_none_is_structurally_granted()
        => Assert.That(Manifest().Trees.All(t => t.AdoptedTreeId is not null), Is.True);

    [Test]
    public void Rebuildable_trees_are_exactly_the_derived_vector_trees()
        => Assert.That(
            Manifest().Trees.Where(t => t.Rebuildable).Select(t => t.AdoptedTreeId).Order(StringComparer.Ordinal),
            Is.EqualTo(new[]
            {
                RepoContextTrees.VectorCoverage,
                RepoContextTrees.VectorIndex,
                RepoContextTrees.VectorMembership,
                RepoContextTrees.VectorMetadata,
            }.Order(StringComparer.Ordinal)));

    [Test]
    public void Store_of_record_and_write_once_trees_are_never_rebuildable()
    {
        var byId = Manifest().Trees.ToDictionary(t => t.AdoptedTreeId!, StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(byId[RepoContextTrees.Memory].Rebuildable, Is.False);
            Assert.That(byId[RepoContextTrees.VectorPayload].Rebuildable, Is.False);
            foreach (var codeIndex in RepoContextTrees.CodeIndexTrees.Where(t => !ConstantsClassifyAsRebuildable(t)))
            {
                Assert.That(byId[codeIndex].Rebuildable, Is.False, codeIndex);
            }
        });
    }

    [Test]
    public void Comparison_fails_when_a_tree_constant_is_renamed_without_the_manifest()
    {
        var renamed = RepoContextTrees.AllIncludingLocalDerived
            .Select(t => t == RepoContextTrees.Memory ? "repo-context-agent-memory" : t)
            .ToArray();

        var disagreements = Disagreements(Manifest(), renamed, ConstantsClassifyAsRebuildable);

        Assert.That(disagreements, Is.EquivalentTo(new[]
        {
            "manifest adopts 'repo-context-memory', which is not a repository-context tree constant",
            "tree constant 'repo-context-agent-memory' is not adopted by the manifest",
        }));
    }

    [Test]
    public void Comparison_fails_when_a_tree_constant_is_added_without_the_manifest()
    {
        var added = RepoContextTrees.AllIncludingLocalDerived.Append("repo-context-new-family").ToArray();

        Assert.That(
            Disagreements(Manifest(), added, ConstantsClassifyAsRebuildable),
            Is.EqualTo(new[] { "tree constant 'repo-context-new-family' is not adopted by the manifest" }));
    }

    [Test]
    public void Comparison_fails_when_a_rebuildable_classification_changes_without_the_manifest()
    {
        bool Flipped(string tree) => tree == RepoContextTrees.VectorPayload || ConstantsClassifyAsRebuildable(tree);

        Assert.That(
            Disagreements(Manifest(), RepoContextTrees.AllIncludingLocalDerived, Flipped),
            Is.EqualTo(new[] { $"tree '{RepoContextTrees.VectorPayload}' is declared rebuildable=False but the constants classify it as True" }));
    }

    [Test]
    public void Comparison_fails_for_a_manifest_tree_that_adopts_nothing()
    {
        var manifest = Manifest();
        var structural = manifest with
        {
            Trees = [.. manifest.Trees, new AppTreeDeclaration { Name = "extra" }],
        };

        Assert.That(
            Disagreements(structural, RepoContextTrees.AllIncludingLocalDerived, ConstantsClassifyAsRebuildable),
            Is.EqualTo(new[] { "tree 'extra' does not adopt a physical tree" }));
    }
}
