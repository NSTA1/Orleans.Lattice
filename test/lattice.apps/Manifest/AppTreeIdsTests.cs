namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppTreeIdsTests
{
    [TestCase("legacy-notes", true)]
    [TestCase("a/other/items", true)]
    [TestCase("tag-status", true)]
    [TestCase("*", false)]
    [TestCase("", false)]
    [TestCase(null, false)]
    [TestCase("_lattice_trees", false)]
    [TestCase("sys-app-registry", false)]
    [TestCase("sys-app-activation", false)]
    [TestCase("sys-auth-policy", false)]
    [TestCase("sys-tenant-registry", false)]
    [TestCase("t/acme/legacy", false)]
    [TestCase("t/x", false)]
    public void IsGrantable_admits_only_ordinary_data_trees(string? treeId, bool grantable) =>
        Assert.That(AppTreeIds.IsGrantable(treeId), Is.EqualTo(grantable));

    [TestCase("legacy-notes", true)]
    [TestCase("archive/records", true)]
    [TestCase("a/other/items", false)]
    [TestCase("*", false)]
    [TestCase(" legacy", false)]
    [TestCase("legacy\t", false)]
    [TestCase("leg\u0001acy", false)]
    [TestCase("leg\u0085acy", false)]
    public void IsAdoptable_requires_a_grantable_trimmed_non_structural_id(string treeId, bool adoptable) =>
        Assert.That(AppTreeIds.IsAdoptable(treeId), Is.EqualTo(adoptable));

    [Test]
    public void IsAdoptable_bounds_the_id_length()
    {
        Assert.That(AppTreeIds.IsAdoptable(new string('x', AppManifestLimits.MaxTextLength)), Is.True);
        Assert.That(AppTreeIds.IsAdoptable(new string('x', AppManifestLimits.MaxTextLength + 1)), Is.False);
    }
}
