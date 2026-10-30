namespace Orleans.Lattice.Auth.Tests;

[TestFixture]
[Category("Unit")]
public sealed class PolicyAccessGateSchemaIsolationTests
{
    [TestCase("sys-schema-dlq")]
    [TestCase("sys-schema-policy")]
    [TestCase("sys-schema-version")]
    [TestCase("sys-schema-policy-history")]
    public async Task Schema_backing_trees_require_explicit_grants(string treeId)
    {
        foreach (var defaultAllow in new[] { true, false })
        {
            var options = new LatticeAuthOptions
            {
                DefaultEffect = defaultAllow ? LatticeEffect.Allow : LatticeEffect.Deny,
                AllTreesGrantsEnabled = true,
                BootstrapAdministrators = new HashSet<string>(StringComparer.Ordinal) { "root" },
            };
            var harness = await AuthGateHarness.CreateAsync(options,
                new LatticeAuthorizationRule("broad", LatticeSubjectSelector.User("reader"),
                    LatticeScope.ClusterWide(), LatticeAuthOperations.All, LatticeEffect.Allow),
                new LatticeAuthorizationRule("operator", LatticeSubjectSelector.User("operator"),
                    LatticeScope.Tree(treeId), LatticeAuthOperations.All, LatticeEffect.Allow));

            foreach (var operation in new[] { LatticeOperation.Read, LatticeOperation.RangeRead })
            {
                foreach (var subject in new[] { LatticeSubject.Anonymous, new LatticeSubject("reader") })
                {
                    var decision = await harness.Gate.AuthorizeAsync(
                        new LatticeAccessRequest(treeId, operation, subject, "protected/key"));
                    Assert.That(decision.Allowed, Is.False, $"{treeId}: {subject}, {operation}, {defaultAllow}");
                    Assert.That(await harness.Gate.HasAnyGrantAsync(treeId, subject, operation), Is.False);
                }

                foreach (var subject in new[] { new LatticeSubject("operator"), new LatticeSubject("root") })
                {
                    var decision = await harness.Gate.AuthorizeAsync(
                        new LatticeAccessRequest(treeId, operation, subject, "protected/key"));
                    Assert.That(decision.Allowed, Is.True, $"{treeId}: explicit operator or bootstrap access");
                    Assert.That(await harness.Gate.HasAnyGrantAsync(treeId, subject, operation), Is.True);
                }
            }

            Assert.That((await harness.Gate.AuthorizeAsync(new LatticeAccessRequest(
                "ordinary", LatticeOperation.Read, new LatticeSubject("reader"), "key"))).Allowed, Is.True);
        }
    }
}
