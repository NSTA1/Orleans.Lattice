using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

[TestFixture]
[Category("Integration")]
public sealed class TagBackingStoreIsolationTests
{
    private const string PublicTree = "security-public-source";
    private const string PrivateTree = "security-private-source";
    private const string IndexName = "shared-security-test";
    private const string IndexTree = "tag-" + IndexName;
    private const string PrivateRow = "secret-diagnosis\0" + PrivateTree + "\0private/1";
    private AuthApiStateClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();
        await _fixture.CreatePopulatedTreeAsync(PublicTree, "public/1");
        await _fixture.CreatePopulatedTreeAsync(PrivateTree, "private/1");
        await _fixture.CreatePopulatedTreeAsync(IndexTree,
            "open\0" + PublicTree + "\0public/1",
            "open\0" + PrivateTree + "\0private/1",
            PrivateRow,
            "\0covered\0" + PublicTree,
            "\0covered\0" + PrivateTree);
        await _fixture.GrantAsync(
            new LatticeAuthorizationRule("private-deny", LatticeSubjectSelector.User("reader"),
                LatticeScope.Tree(PrivateTree), LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Deny),
            new LatticeAuthorizationRule("index-operator", LatticeSubjectSelector.User("operator"),
                LatticeScope.Tree(IndexTree), LatticeOperation.Read | LatticeOperation.RangeRead, LatticeEffect.Allow));
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [TestCase(true)]
    [TestCase(false)]
    public async Task Raw_index_reads_cannot_bypass_source_denial_while_source_browsing_still_works(bool defaultAllow)
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

        using (AuthApiStateClusterFixture.AsSubject("reader"))
        {
            var rawTree = _fixture.Cluster.Client.GetGrain<ILattice>(IndexTree);
            Assert.That(await rawTree.GetAsync(PrivateRow), Is.Null);
            var rawKeys = new List<string>();
            await foreach (var key in rawTree.ScanKeysAsync())
            {
                rawKeys.Add(key);
            }
            Assert.That(rawKeys, Is.Empty);
            var rawPage = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = IndexTree });
            Assert.That(rawPage.Entries, Is.Empty);

            var tags = await _fixture.Query.ListTagValuesAsync(
                new CatalogRequest { SourceTreeId = PublicTree, IndexName = IndexName });
            Assert.That(tags.Entries, Is.EqualTo(new[] { "open" }));
            var privateTags = await _fixture.Query.ListTagValuesAsync(
                new CatalogRequest { SourceTreeId = PrivateTree, IndexName = IndexName });
            Assert.That(privateTags.Entries, Is.Empty);
            var union = await _fixture.Query.ListIndexTagsAsync(new CatalogRequest { IndexName = IndexName });
            Assert.That(union.Entries, Is.Empty);

            var members = await _fixture.Query.ScanTagMembersAsync(
                new TagMemberScanRequest { IndexName = IndexName, Tag = "open" });
            Assert.That(members.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { PublicTree }));
            var entries = await _fixture.Query.ScanEntriesAsync(
                new EntryScanRequest { TreeId = PublicTree, IndexName = IndexName, Tag = "open" });
            Assert.That(entries.Entries.Select(e => e.Key), Is.EqualTo(new[] { "public/1" }));
            Assert.That(LatticeAccessGateContext.IsGateBypassed, Is.False);
        }

        foreach (var subject in new[] { "operator", AuthApiStateClusterFixture.BootstrapAdmin })
        {
            using (AuthApiStateClusterFixture.AsSubject(subject))
            {
                Assert.That(await _fixture.Cluster.Client.GetGrain<ILattice>(IndexTree).GetAsync(PrivateRow), Is.Not.Null);
            }
        }
    }
}
