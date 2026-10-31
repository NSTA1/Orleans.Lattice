namespace Orleans.Lattice.Auth.Tests;

[TestFixture]
[Category("Unit")]
public sealed class PolicyAccessGateTagIsolationTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Tag_backing_tree_requires_explicit_grants(bool defaultAllow)
    {
        const string treeId = "tag-by-status";
        var harness = await AuthGateHarness.CreateAsync(new LatticeAuthOptions
        {
            DefaultEffect = defaultAllow ? LatticeEffect.Allow : LatticeEffect.Deny,
            AllTreesGrantsEnabled = true,
            BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
        },
            new LatticeAuthorizationRule("broad", LatticeSubjectSelector.User("reader"),
                LatticeScope.ClusterWide(), LatticeAuthOperations.All, LatticeEffect.Allow),
            new LatticeAuthorizationRule("operator", LatticeSubjectSelector.User("operator"),
                LatticeScope.Tree(treeId), LatticeAuthOperations.All, LatticeEffect.Allow));

        foreach (var operation in new[] { LatticeOperation.Read, LatticeOperation.RangeRead,
            LatticeOperation.Write, LatticeOperation.Delete })
        {
            foreach (var subject in new[] { LatticeSubject.Anonymous, new LatticeSubject("reader") })
            {
                var decision = await harness.Gate.AuthorizeAsync(
                    new LatticeAccessRequest(treeId, operation, subject, "secret\0orders\0private/1"));
                Assert.That(decision.Allowed, Is.False, $"{subject}, {operation}, {defaultAllow}");
                Assert.That(await harness.Gate.HasAnyGrantAsync(treeId, subject, operation), Is.False);
            }

            foreach (var subject in new[] { new LatticeSubject("operator"), new LatticeSubject("root") })
            {
                var decision = await harness.Gate.AuthorizeAsync(
                    new LatticeAccessRequest(treeId, operation, subject, "secret\0orders\0private/1"));
                Assert.That(decision.Allowed, Is.True);
            }
        }
    }
}
