namespace Orleans.Lattice.Auth.Tests;

[TestFixture]
[Category("Unit")]
public sealed class PolicyAccessGateBackupIsolationTests
{
    [TestCase("sys-backup-store")]
    [TestCase("sys-backup-catalog")]
    [TestCase("sys-backup-health")]
    [TestCase("sys-backup-catalog-history")]
    [TestCase("sys-backup-catalog-index")]
    public async Task Backup_backing_trees_require_explicit_grants(string treeId)
    {
        foreach (var defaultAllow in new[] { true, false })
        {
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
                        new LatticeAccessRequest(treeId, operation, subject, "artifact"));
                    Assert.That(decision.Allowed, Is.False, $"{treeId}: {subject}, {operation}, {defaultAllow}");
                    Assert.That(await harness.Gate.HasAnyGrantAsync(treeId, subject, operation), Is.False);
                }

                foreach (var subject in new[] { new LatticeSubject("operator"), new LatticeSubject("root") })
                {
                    var decision = await harness.Gate.AuthorizeAsync(
                        new LatticeAccessRequest(treeId, operation, subject, "artifact"));
                    Assert.That(decision.Allowed, Is.True, $"{treeId}: explicit operator or bootstrap access");
                    Assert.That(await harness.Gate.HasAnyGrantAsync(treeId, subject, operation), Is.True);
                }
            }

            Assert.That((await harness.Gate.AuthorizeAsync(new LatticeAccessRequest(
                "ordinary", LatticeOperation.Read, new LatticeSubject("reader"), "key"))).Allowed, Is.True);
        }
    }
}
