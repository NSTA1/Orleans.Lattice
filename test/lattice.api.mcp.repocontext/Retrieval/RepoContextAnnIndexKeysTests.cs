namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the approximate index's key layout, and in particular for the one
/// property the superseded-space reclamation is built on: <b>the repository sorts
/// before the embedding-space fingerprint</b>.
/// <para>
/// That ordering is what makes every space a repository has ever been indexed
/// under a set of siblings in one contiguous ordinal range, and therefore what
/// makes it possible to find the abandoned ones with a bounded, repository-scoped
/// scan. Were the fingerprint to sort first, a repository's spaces would be
/// scattered across the whole index tree and no repository-scoped scan would
/// exist - the abandoned prefixes could then only be found by having recorded
/// them at the time, which is a different mechanism entirely. These tests pin the
/// ordering so a later "tidier" key layout cannot silently remove the reclamation's
/// only means of enumeration.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexKeysTests
{
    private static readonly EmbeddingSpaceTag SpaceA = new("model-a", 8, VectorNormalization.UnitL2);
    private static readonly EmbeddingSpaceTag SpaceB = new("model-b", 8, VectorNormalization.UnitL2);

    [Test]
    public void Every_space_of_one_repository_is_a_sibling_under_the_repository_root()
    {
        var root = RepoContextAnnIndexKeys.RepositoryRoot("acme");
        var a = RepoContextAnnIndexKeys.IndexPrefix("acme", SpaceA);
        var b = RepoContextAnnIndexKeys.IndexPrefix("acme", SpaceB);

        Assert.Multiple(() =>
        {
            Assert.That(a, Does.StartWith(root),
                "A repository-scoped prefix scan can only find a space that sits under the repository root. "
                + "If the fingerprint sorted before the repository id, this would not hold and the reclamation "
                + "would need a per-repository record of known fingerprints instead of a scan.");
            Assert.That(b, Does.StartWith(root));
            Assert.That(a, Is.Not.EqualTo(b),
                "Two embedding spaces must never share a prefix: retirement works by range delete, so they would "
                + "delete each other's generations.");
            Assert.That(a[root.Length..].TrimEnd('/'), Does.Not.Contain("/"),
                "A space contributes exactly one path segment beneath the root, which is what lets the walk skip a "
                + "whole space in one hop.");
        });
    }

    [Test]
    public void The_prefix_carries_a_fingerprint_rather_than_the_model_id_itself()
    {
        var prefix = RepoContextAnnIndexKeys.IndexPrefix("acme", SpaceA);

        Assert.That(prefix, Does.Not.Contain(SpaceA.ModelId),
            "A model id must never be carried verbatim into a key: it is caller-supplied text, and the layout "
            + "depends on the segment being one fixed-width token.");
    }

    [Test]
    public void A_dimension_or_normalization_change_is_a_different_space()
    {
        var baseline = RepoContextAnnIndexKeys.IndexPrefix("acme", SpaceA);
        var rescaled = RepoContextAnnIndexKeys.IndexPrefix(
            "acme", new EmbeddingSpaceTag(SpaceA.ModelId, SpaceA.Dimension * 2, SpaceA.Normalization));
        var unnormalized = RepoContextAnnIndexKeys.IndexPrefix(
            "acme", new EmbeddingSpaceTag(SpaceA.ModelId, SpaceA.Dimension, VectorNormalization.None));

        Assert.Multiple(() =>
        {
            Assert.That(rescaled, Is.Not.EqualTo(baseline));
            Assert.That(unnormalized, Is.Not.EqualTo(baseline));
        });
    }

    [Test]
    public void A_build_grain_key_round_trips_even_when_the_repository_id_contains_a_separator()
    {
        // Parsed from the right, because the fingerprint is the fixed-width final
        // segment. A repository id is caller-supplied, so a left-hand parse would
        // mis-split one that contains a separator and address a coordinator for a
        // pair that does not exist.
        const string RepoId = "org/team/acme";
        var key = RepoContextAnnIndexKeys.BuildGrainKey(RepoId, SpaceA);

        Assert.That(
            RepoContextAnnIndexKeys.TryParseBuildGrainKey(key, out var repoId, out var fingerprint), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(repoId, Is.EqualTo(RepoId));
            Assert.That(fingerprint, Is.EqualTo(RepoContextAnnIndexKeys.SpaceFingerprint(SpaceA)));
        });
    }

    [TestCase("")]
    [TestCase("acme")]
    [TestCase("/acme")]
    [TestCase("acme/")]
    public void A_malformed_build_grain_key_is_refused(string key)
        => Assert.That(
            RepoContextAnnIndexKeys.TryParseBuildGrainKey(key, out _, out _), Is.False,
            "A key that names no pair must be refused rather than parsed into a partial identity.");

    [Test]
    public void A_key_under_the_root_resolves_to_the_space_prefix_it_belongs_to()
    {
        var root = RepoContextAnnIndexKeys.RepositoryRoot("acme");
        var prefix = RepoContextAnnIndexKeys.IndexPrefix("acme", SpaceA);

        Assert.That(RepoContextAnnIndexKeys.TrySpacePrefix(root, prefix + "g/0000000000000000001/p/00001", out var resolved), Is.True);
        Assert.That(resolved, Is.EqualTo(prefix),
            "Resolving any observed key to its whole space prefix is what lets the walk delete a space in one range "
            + "operation and then skip past it, instead of enumerating its records.");
    }

    [Test]
    public void A_key_that_names_no_space_never_resolves_to_the_repository_root()
    {
        // THE DANGEROUS CASE. A key sitting directly under the root with no
        // fingerprint segment must not resolve to a "space prefix" equal to the root
        // itself: the caller range-deletes whatever this returns, so that answer
        // would take every space the repository has, the live one included.
        var root = RepoContextAnnIndexKeys.RepositoryRoot("acme");

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextAnnIndexKeys.TrySpacePrefix(root, root, out _), Is.False);
            Assert.That(RepoContextAnnIndexKeys.TrySpacePrefix(root, root + "stray", out _), Is.False,
                "A key with no separator after the root names no space.");
            Assert.That(RepoContextAnnIndexKeys.TrySpacePrefix(root, root + "/x", out _), Is.False,
                "An empty fingerprint segment names no space either.");
            Assert.That(RepoContextAnnIndexKeys.TrySpacePrefix(root, "repo/other/vidx/abc/m", out _), Is.False,
                "A key belonging to a different repository is never in reach.");
        });
    }

    [Test]
    public void The_key_helpers_reject_a_null_repository()
        => Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextAnnIndexKeys.RepositoryRoot(null!), Throws.ArgumentNullException);
            Assert.That(() => RepoContextAnnIndexKeys.IndexPrefix(null!, SpaceA), Throws.ArgumentNullException);
            Assert.That(() => RepoContextAnnIndexKeys.BuildGrainKey(null!, SpaceA), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextAnnIndexKeys.TryParseBuildGrainKey(null!, out _, out _),
                Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextAnnIndexKeys.TrySpacePrefix(null!, "k", out _), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextAnnIndexKeys.TrySpacePrefix("r", null!, out _), Throws.ArgumentNullException);
        });
}
