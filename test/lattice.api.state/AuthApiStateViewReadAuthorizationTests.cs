using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Regression coverage for the state-API materialised-view read authorization
/// boundary (issue #1103, finding A1). A view read binds under a view-read scope
/// that makes the data-plane access gate bypass itself, so - unless the state API
/// intervenes - a view read is gated by nothing. The fix authorizes a
/// <c>view-*</c> read by the readability of the view's SOURCE tree (mirroring the
/// view catalog): a caller with no read grant on the source (or an anonymous
/// caller) sees the view as not-found; a caller that can read the whole source
/// sees the whole view; and a prefix-granted caller sees only the view keys
/// inside its source-tree grant.
/// </summary>
/// <remarks>
/// Every test here fails without the fix: before it, <c>IsTreeReadHiddenAsync</c>
/// short-circuited to visible for a reserved view tree and the view rows were
/// returned to any caller, so the deny / prefix-prune assertions below would all
/// surface the full view.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiStateViewReadAuthorizationTests
{
    private AuthViewApiStateClusterFixture _fixture = null!;

    private const string Source = "a1-view-source";
    private const string ViewName = "a1mirror";
    private string _viewTreeId = null!;

    private const string PrefixReader = "prefix-reader";
    private const string SourceReader = "source-reader";
    private const string Outsider = "outsider";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthViewApiStateClusterFixture();
        await _fixture.InitializeAsync();

        // Source holds two "x/" keys the prefix reader may read and one "y/" key it
        // may not; the view mirrors all three verbatim.
        await _fixture.CreatePopulatedTreeAsync(Source, "x/1", "x/2", "y/1");
        _viewTreeId = await _fixture.CreateViewAsync(Source, ViewName);

        // prefix-reader: read + range-read on the source's "x/" prefix only.
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "a1-prefix",
            LatticeSubjectSelector.User(PrefixReader),
            LatticeScope.Prefix(Source, "x/"),
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow));

        // source-reader: read + range-read on the whole source tree.
        await _fixture.GrantAsync(new LatticeAuthorizationRule(
            "a1-whole",
            LatticeSubjectSelector.User(SourceReader),
            LatticeScope.Tree(Source),
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow));

        // outsider gets no grant at all.
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ScanEntries_view_is_hidden_from_a_subject_without_source_read_grant()
    {
        using (AuthViewApiStateClusterFixture.AsSubject(Outsider))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = _viewTreeId,
                PageSize = 50,
            });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(result.Entries, Is.Empty);
        }
    }

    [Test]
    public async Task ScanEntries_view_is_hidden_from_an_anonymous_caller()
    {
        // No AsSubject scope: the caller is anonymous and has no read grant on any
        // source, so the view is fail-closed to not-found.
        var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = _viewTreeId,
            PageSize = 50,
        });

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
        Assert.That(result.Entries, Is.Empty);
    }

    [Test]
    public async Task ScanEntries_view_is_fully_visible_to_a_whole_source_reader()
    {
        using (AuthViewApiStateClusterFixture.AsSubject(SourceReader))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = _viewTreeId,
                PageSize = 50,
            });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(
                result.Entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "x/1", "x/2", "y/1" }));
        }
    }

    [Test]
    public async Task ScanEntries_view_prefix_reader_sees_only_its_granted_keys()
    {
        using (AuthViewApiStateClusterFixture.AsSubject(PrefixReader))
        {
            var result = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = _viewTreeId,
                PageSize = 50,
            });

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(
                result.Entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "x/1", "x/2" }));
        }
    }

    [Test]
    public async Task GetEntry_view_is_hidden_from_a_subject_without_source_read_grant()
    {
        using (AuthViewApiStateClusterFixture.AsSubject(Outsider))
        {
            var result = await _fixture.Query.GetEntryAsync(_viewTreeId, "x/1");

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.TreeNotFound));
            Assert.That(result.Entry, Is.Null);
        }
    }

    [Test]
    public async Task GetEntry_view_prefix_reader_reads_a_granted_key()
    {
        using (AuthViewApiStateClusterFixture.AsSubject(PrefixReader))
        {
            var result = await _fixture.Query.GetEntryAsync(_viewTreeId, "x/1");

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
            Assert.That(result.Entry, Is.Not.Null);
            Assert.That(result.Entry!.Key, Is.EqualTo("x/1"));
        }
    }

    [Test]
    public async Task GetEntry_view_prefix_reader_cannot_read_an_ungranted_key()
    {
        using (AuthViewApiStateClusterFixture.AsSubject(PrefixReader))
        {
            var result = await _fixture.Query.GetEntryAsync(_viewTreeId, "y/1");

            Assert.That(result.Status, Is.EqualTo(StateQueryStatus.KeyNotFound));
            Assert.That(result.Entry, Is.Null);
        }
    }
}
