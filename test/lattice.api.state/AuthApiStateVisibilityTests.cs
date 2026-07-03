using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Acceptance matrix for the state-API auth-backed read visibility (issue #981).
/// With the authorization add-on live and a fail-closed default-deny policy, the
/// read-only state API filters every read through the data-plane access gate
/// keyed off the caller's resolved subject: per-entry reads inherit the core
/// enforcement once the caller identity flows, and the catalog / structure
/// surfaces are explicitly scoped to trees the subject may read. An unresolved
/// (anonymous) caller is denied every read.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateVisibilityTests
{
    private AuthApiStateClusterFixture _fixture = null!;

    private const string TreeA = "vis-tree-a";
    private const string TreeB = "vis-tree-b";
    private const string Reader = "reader";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiStateClusterFixture();
        await _fixture.InitializeAsync();

        // treeA holds two readable-prefix keys ("x/") and one unreadable key
        // ("y/"); treeB is entirely off-limits to the reader.
        await _fixture.CreatePopulatedTreeAsync(TreeA, "x/1", "x/2", "y/1");
        await _fixture.CreatePopulatedTreeAsync(TreeB, "b/1");

        // Grant the reader point + range read on treeA's "x/" prefix only.
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "reader-x",
            LatticeSubjectSelector.User(Reader),
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
    public async Task ScanEntries_reader_sees_only_the_permitted_prefix()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = TreeA,
                PageSize = 50,
            });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(
                result.Entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "x/1", "x/2" }));
        }
    }

    [Test]
    public async Task GetEntry_reader_reads_a_permitted_key()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var result = await _fixture.Query.GetEntryAsync(TreeA, "x/1");

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Entry, Is.Not.Null);
            Assert.That(result.Entry!.Key, Is.EqualTo("x/1"));
        }
    }

    [Test]
    public async Task GetEntry_reader_cannot_read_an_unauthorised_key_returns_not_found()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var result = await _fixture.Query.GetEntryAsync(TreeA, "y/1");

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
            Assert.That(result.Entry, Is.Null);
        }
    }

    [Test]
    public async Task ListTrees_reader_does_not_see_an_unreadable_tree()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

            var ids = page.Entries.Select(e => e.TreeId).ToArray();
            Assert.That(ids, Does.Contain(TreeA));
            Assert.That(ids, Does.Not.Contain(TreeB));
        }
    }

    [Test]
    public async Task GetTreeStructure_reader_cannot_see_an_unreadable_tree()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest { TreeId = TreeB });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        }
    }

    [Test]
    public async Task GetTreeStructure_reader_can_see_a_readable_tree()
    {
        using (AuthApiStateClusterFixture.AsSubject(Reader))
        {
            var result = await _fixture.Query.GetTreeStructureAsync(new StructureRequest { TreeId = TreeA });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        }
    }

    [Test]
    public async Task ScanEntries_unresolved_identity_is_denied()
    {
        // No AsSubject scope: the caller is anonymous. Fail-closed to not-found.
        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = TreeA,
            PageSize = 50,
        });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
    }

    [Test]
    public async Task GetEntry_unresolved_identity_is_denied()
    {
        var result = await _fixture.Query.GetEntryAsync(TreeA, "x/1");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Entry, Is.Null);
    }

    [Test]
    public async Task ListTrees_unresolved_identity_sees_no_trees()
    {
        var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

        var ids = page.Entries.Select(e => e.TreeId).ToArray();
        Assert.That(ids, Does.Not.Contain(TreeA));
        Assert.That(ids, Does.Not.Contain(TreeB));
    }

    [Test]
    public async Task Reads_bootstrap_admin_sees_every_tree_and_key()
    {
        // The bootstrap administrator bypasses policy: full visibility, proving
        // the filter scopes by decision rather than blanket-denying.
        using (AuthApiStateClusterFixture.AsSubject(AuthApiStateClusterFixture.BootstrapAdmin))
        {
            var page = await _fixture.Query.ListTreesAsync(new CatalogRequest { PageSize = 100 });
            var ids = page.Entries.Select(e => e.TreeId).ToArray();
            Assert.That(ids, Does.Contain(TreeA));
            Assert.That(ids, Does.Contain(TreeB));

            var scan = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = TreeA,
                PageSize = 50,
            });
            Assert.That(
                scan.Entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "x/1", "x/2", "y/1" }));
        }
    }
}
