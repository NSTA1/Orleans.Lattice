using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Acceptance matrix proving the read-only state API's auth-backed visibility
/// (issue #981) honours <b>group</b>-scoped grants, not just user-scoped ones: a
/// caller inherits a prefix RangeRead grant through group membership, so a scan
/// returns only the keys under the authorized prefix, an unauthorized key reads
/// back not-found, and an unreadable tree is hidden from the catalog - while a
/// caller who is not in the granting group sees nothing. Complements
/// <see cref="AuthApiStateVisibilityTests"/>, which exercises the same surface
/// through user-scoped rules.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateGroupVisibilityTests
{
    private AuthApiStateClusterFixture _fixture = null!;

    private const string TreeA = "grp-vis-tree-a";
    private const string TreeB = "grp-vis-tree-b";
    private const string Group = "eu-readers";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();

        await _fixture.CreatePopulatedTreeAsync(TreeA, "x/1", "x/2", "y/1");
        await _fixture.CreatePopulatedTreeAsync(TreeB, "b/1");

        // Grant the GROUP (not a user) point + range read on treeA's "x/" prefix.
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "eu-readers-x",
            LatticeSubjectSelector.Group(Group),
            LatticeScope.Prefix(TreeA, "x/"),
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task ScanEntries_group_member_sees_only_the_permitted_prefix()
    {
        using (AuthApiStateClusterFixture.AsSubject("alice", Group))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = TreeA,
                PageSize = 50,
            });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(
                result.Entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "x/1", "x/2" }),
                "a member inherits the group's prefix RangeRead grant and sees only that prefix");
        }
    }

    [Test]
    public async Task GetEntry_group_member_reads_a_permitted_key_but_not_an_unauthorized_one()
    {
        using (AuthApiStateClusterFixture.AsSubject("alice", Group))
        {
            var permitted = await _fixture.Query.GetEntryAsync(TreeA, "x/1");
            Assert.That(permitted.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(permitted.Entry, Is.Not.Null);
            Assert.That(permitted.Entry!.Key, Is.EqualTo("x/1"));

            var unauthorized = await _fixture.Query.GetEntryAsync(TreeA, "y/1");
            Assert.That(unauthorized.Status, Is.EqualTo(StateQueryStatus.KeyNotFound),
                "the group grant covers only the 'x/' prefix, so 'y/1' reads back absent");
            Assert.That(unauthorized.Entry, Is.Null);
        }
    }

    [Test]
    public async Task ListTrees_group_member_sees_the_readable_tree_not_the_off_limits_one()
    {
        using (AuthApiStateClusterFixture.AsSubject("alice", Group))
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

            var ids = page.Entries.Select(e => e.TreeId).ToArray();
            Assert.That(ids, Does.Contain(TreeA));
            Assert.That(ids, Does.Not.Contain(TreeB),
                "a tree the group has no grant on stays out of the catalog");
        }
    }

    [Test]
    public async Task ScanEntries_non_member_sees_nothing()
    {
        // "bob" is authenticated but not in the granting group: fail-closed.
        using (AuthApiStateClusterFixture.AsSubject("bob", "other-group"))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = TreeA,
                PageSize = 50,
            });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound),
                "a caller outside the granting group inherits no visibility");
        }
    }
}
