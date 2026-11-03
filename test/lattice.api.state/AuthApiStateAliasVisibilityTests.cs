using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins how auth-backed read visibility treats a tree alias (#5597). The data
/// plane authorizes a read on the name the caller addressed, before the alias is
/// resolved, so a grant on an alias's target does not extend to the alias name.
/// The catalogue must agree with the data path: it lists an alias exactly to the
/// callers whose reads through that name are admitted, and never to a caller
/// granted only on the target, so it never lists a name every read of which the
/// caller would then be refused.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateAliasVisibilityTests
{
    private AuthApiStateClusterFixture _fixture = null!;

    private const string Target = "alias-vis-target";
    private const string Alias = "alias-vis-name";
    private const string TargetReader = "target-reader";
    private const string AliasReader = "alias-reader";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();

        await _fixture.CreatePopulatedTreeAsync(Target, "k/1", "k/2");
        await _fixture.CreatePopulatedTreeAsync(Alias);

        var registry = _fixture.SiloServices.GetRequiredService<IGrainFactory>()
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
        {
            await registry.SetAliasAsync(Alias, Target);
        }

        await _fixture.GrantAsync(
            new LatticeAuthorizationRule(
                "target-only",
                LatticeSubjectSelector.User(TargetReader),
                LatticeScope.Tree(Target),
                LatticeOperation.Read | LatticeOperation.RangeRead,
                LatticeEffect.Allow),
            new LatticeAuthorizationRule(
                "alias-only",
                LatticeSubjectSelector.User(AliasReader),
                LatticeScope.Tree(Alias),
                LatticeOperation.Read | LatticeOperation.RangeRead,
                LatticeEffect.Allow));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task ListTrees_caller_granted_only_on_the_target_does_not_see_the_alias()
    {
        using (AuthApiStateClusterFixture.AsSubject(TargetReader))
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

            var ids = page.Entries.Select(e => e.TreeId).ToArray();
            Assert.That(ids, Does.Contain(Target));
            Assert.That(ids, Does.Not.Contain(Alias));
        }
    }

    [Test]
    public async Task ScanEntries_caller_granted_only_on_the_target_is_refused_through_the_alias()
    {
        using (AuthApiStateClusterFixture.AsSubject(TargetReader))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = Alias, PageSize = 50 });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(result.Entries, Is.Empty);
        }
    }

    [Test]
    public async Task ListTrees_caller_granted_on_the_alias_sees_the_alias_and_not_the_target()
    {
        using (AuthApiStateClusterFixture.AsSubject(AliasReader))
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

            var ids = page.Entries.Select(e => e.TreeId).ToArray();
            Assert.That(ids, Does.Contain(Alias));
            Assert.That(ids, Does.Not.Contain(Target));
            var alias = page.Entries.Single(e => e.TreeId == Alias);
            Assert.That(alias.IsAlias, Is.True);
            Assert.That(alias.PhysicalTreeId, Is.EqualTo(Target));
        }
    }

    [Test]
    public async Task ScanEntries_caller_granted_on_the_alias_reads_the_target_through_it()
    {
        using (AuthApiStateClusterFixture.AsSubject(AliasReader))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest { TreeId = Alias, PageSize = 50 });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(
                result.Entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "k/1", "k/2" }));
        }
    }

    [Test]
    public async Task ListTrees_caller_with_no_grant_sees_neither_name()
    {
        using (AuthApiStateClusterFixture.AsSubject("stranger"))
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

            var ids = page.Entries.Select(e => e.TreeId).ToArray();
            Assert.That(ids, Does.Not.Contain(Alias));
            Assert.That(ids, Does.Not.Contain(Target));
        }
    }
}
