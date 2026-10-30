using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Auth.Tests;

[TestFixture]
[Category("Integration")]
public sealed class SchemaDeadLetterIsolationIntegrationTests
{
    private const string SourceTree = "schema-protected-source";
    private const string DeadLetterTree = "sys-schema-dlq";
    private const string PreviewKey = "schema-protected-source/secret";
    private static readonly byte[] Preview = Encoding.UTF8.GetBytes("sensitive-dead-letter-preview");
    private AuthClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new AuthClusterFixture();
        await _fixture.InitializeAsync();
        using (AuthClusterFixture.AsSubject(AuthClusterFixture.BootstrapAdmin))
        {
            await _fixture.Lattice(SourceTree).SetAsync("secret", Preview);
        }

        using (LatticeSystemOrigin.Enter())
        {
            await _fixture.SiloServices.GetRequiredService<IGrainFactory>()
                .GetGrain<ILattice>(DeadLetterTree).SetAsync(PreviewKey, Preview);
        }

        await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
            "source-deny", LatticeSubjectSelector.User("reader"), LatticeScope.Tree(SourceTree),
            LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Deny));
        await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
            "operator", LatticeSubjectSelector.User("operator"), LatticeScope.Tree(DeadLetterTree),
            LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow));
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [TestCase(true)]
    [TestCase(false)]
    public async Task Source_denial_cannot_be_bypassed_through_raw_dead_letter_reads(bool defaultAllow)
    {
        var options = _fixture.SiloServices.GetRequiredService<IOptionsMonitor<LatticeAuthOptions>>().CurrentValue;
        options.DefaultEffect = defaultAllow ? LatticeEffect.Allow : LatticeEffect.Deny;
        options.AllTreesGrantsEnabled = true;
        if (!defaultAllow)
        {
            await _fixture.Store.PutRuleAsync(new LatticeAuthorizationRule(
                "broad", LatticeSubjectSelector.User("reader"), LatticeScope.ClusterWide(),
                LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow));
        }
        else
        {
            await _fixture.Store.RemoveRuleAsync(LatticeScope.ClusterWideTreeId, "broad");
        }

        await _fixture.RebuildPolicyAsync();
        using (AuthClusterFixture.AsSubject("reader"))
        {
            Assert.That(await _fixture.Lattice(SourceTree).GetAsync("secret"), Is.Null);
            Assert.That(await _fixture.Lattice(DeadLetterTree).GetAsync(PreviewKey), Is.Null);
            var rows = new List<byte[]>();
            await foreach (var entry in _fixture.Lattice(DeadLetterTree).ScanEntriesAsync())
            {
                rows.Add(entry.Value);
            }

            Assert.That(rows, Is.Empty, "a source denial must not be bypassed by scanning the shared DLQ");
        }

        foreach (var subject in new[] { "operator", AuthClusterFixture.BootstrapAdmin })
        {
            using (AuthClusterFixture.AsSubject(subject))
            {
                Assert.That(await _fixture.Lattice(DeadLetterTree).GetAsync(PreviewKey), Is.EqualTo(Preview));
            }
        }
    }
}
