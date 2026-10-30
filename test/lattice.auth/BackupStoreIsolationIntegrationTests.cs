using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth.Tests;

[TestFixture]
[Category("Integration")]
public sealed class BackupStoreIsolationIntegrationTests
{
    private const string SourceTree = "backup-protected-source";
    private const string StoreTree = "sys-backup-store";
    private const string ArtifactKey = "a\u001fprotected-artifact\u001f0000000000";
    private static readonly byte[] Payload = Encoding.UTF8.GetBytes("protected-backup-payload");
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await _fixture.Lattice(SourceTree).SetAsync("secret", Payload);
        }

        // Seed the same artifact row the in-cluster sink writes, under its system origin.
        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.SiloServices.GetRequiredService<IGrainFactory>()
                .GetGrain<ILattice>(StoreTree).SetAsync(ArtifactKey, Payload);
        }

        await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
            "source-deny", LatticeSubjectSelector.User("reader"), LatticeScope.Tree(SourceTree),
            LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Deny));
        await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
            "operator", LatticeSubjectSelector.User("operator"), LatticeScope.Tree(StoreTree),
            LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow));
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [TestCase(true)]
    [TestCase(false)]
    public async Task Source_denial_cannot_be_bypassed_through_raw_backup_reads(bool defaultAllow)
    {
        var options = _fixture.SiloServices.GetRequiredService<IOptionsMonitor<LatticeAuthOptions>>().CurrentValue;
        options.DefaultEffect = defaultAllow ? LatticeEffect.Allow : LatticeEffect.Deny;
        options.AllTreesGrantsEnabled = true;
        if (defaultAllow)
        {
            await _fixture.Store.RemoveRuleAsync(LatticeScope.ClusterWideTreeId, "broad");
        }
        else
        {
            await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                "broad", LatticeSubjectSelector.User("reader"), LatticeScope.ClusterWide(),
                LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow));
        }

        await _fixture.RebuildPolicyAsync();
        using (AuthClusterFixture.AsSubject("reader"))
        {
            Assert.That(await _fixture.Lattice(SourceTree).GetAsync("secret"), Is.Null);
            Assert.That(await _fixture.Lattice(StoreTree).GetAsync(ArtifactKey), Is.Null);
            var rows = new List<byte[]>();
            await foreach (var entry in _fixture.Lattice(StoreTree).ScanEntriesAsync())
            {
                rows.Add(entry.Value);
            }

            Assert.That(rows, Is.Empty, "a source denial must not be bypassed through backup artifact scans");
        }

        foreach (var subject in new[] { "operator", AuthClusterFixture.BootstrapAdmin })
        {
            using (AuthClusterFixture.AsSubject(subject))
            {
                Assert.That(await _fixture.Lattice(StoreTree).GetAsync(ArtifactKey), Is.EqualTo(Payload));
            }
        }
    }
}
