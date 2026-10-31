using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// The range read's gate-coverage report (<see cref="DataRangeRequest.IncludeGateCoverage"/>
/// and <see cref="DataRangePage.GateCoverage"/>) on a live access gate: a whole-tree
/// reader reads <see cref="LatticeRangeReadGateCoverage.Unrestricted"/>, a prefix
/// reader reads <see cref="LatticeRangeReadGateCoverage.Filtered"/>, a reader
/// without range-read rights reads <see cref="LatticeRangeReadGateCoverage.Denied"/>
/// without opening a cursor, and a tree the caller cannot observe reports nothing at all.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class AuthApiDataRangeCoverageTests
{
    private const string Writer = "coverage-writer";
    private const string Reader = "coverage-reader";

    private const LatticeOperation ReadOps = LatticeOperation.Read | LatticeOperation.RangeRead;

    private AuthApiDataClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AuthApiDataClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task A_whole_tree_reader_reads_unrestricted_coverage_on_the_first_page_only()
    {
        const string tree = "coverage-whole";
        await SeedAsync(tree);
        await _fixture.GrantAsync(Allow(Reader, LatticeScope.Tree(tree), ReadOps, "whole"));

        DataRangePage first;
        DataRangePage next;
        using (AuthApiDataClusterFixture.AsSubject(Reader))
        {
            first = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 2, IncludeGateCoverage = true });
            next = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, ContinuationToken = first.ContinuationToken, IncludeGateCoverage = true });
        }

        Assert.Multiple(() =>
        {
            Assert.That(first.GateCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Unrestricted));
            Assert.That(first.Entries, Has.Count.EqualTo(2));
            Assert.That(next.GateCoverage, Is.Null, "a continuation does not ask the gate again");
            Assert.That(next.Entries.Select(e => e.Key), Is.EqualTo(new[] { "y/1" }));
        });
    }

    [Test]
    public async Task A_prefix_reader_reads_filtered_coverage_and_only_its_keys()
    {
        const string tree = "coverage-prefix";
        await SeedAsync(tree);
        await _fixture.GrantAsync(Allow(Reader, LatticeScope.Prefix(tree, "x/"), ReadOps, "prefix"));

        DataRangePage page;
        using (AuthApiDataClusterFixture.AsSubject(Reader))
        {
            page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, PageSize = 100, IncludeGateCoverage = true });
        }

        Assert.Multiple(() =>
        {
            Assert.That(page.GateCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Filtered));
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "x/1", "x/2" }));
        });
    }

    [Test]
    public async Task A_reader_without_range_read_reads_denied_and_opens_no_cursor()
    {
        const string tree = "coverage-denied";
        await SeedAsync(tree);
        await _fixture.GrantAsync(Allow(Reader, LatticeScope.Tree(tree), LatticeOperation.Read, "denied"));

        DataRangePage page;
        using (AuthApiDataClusterFixture.AsSubject(Reader))
        {
            page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, IncludeGateCoverage = true });
        }

        Assert.Multiple(() =>
        {
            Assert.That(page.GateCoverage, Is.EqualTo(LatticeRangeReadGateCoverage.Denied));
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public async Task A_tree_the_caller_cannot_observe_reports_no_coverage()
    {
        const string tree = "coverage-hidden";
        await SeedAsync(tree);

        var page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree, IncludeGateCoverage = true });

        Assert.Multiple(() =>
        {
            Assert.That(page.GateCoverage, Is.Null, "an anonymous caller learns nothing about the tree");
            Assert.That(page.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task Coverage_is_not_reported_unless_asked_for()
    {
        const string tree = "coverage-unasked";
        await SeedAsync(tree);
        await _fixture.GrantAsync(Allow(Reader, LatticeScope.Tree(tree), ReadOps, "unasked"));

        DataRangePage page;
        using (AuthApiDataClusterFixture.AsSubject(Reader))
        {
            page = await _fixture.Api.ReadRangeAsync(new DataRangeRequest { TreeId = tree });
        }

        Assert.That(page.GateCoverage, Is.Null);
    }

    private static LatticeAuthorizationRule Allow(string subject, LatticeScope scope, LatticeOperation ops, string suffix) =>
        new($"{subject}-{suffix}", LatticeSubjectSelector.User(subject), scope, ops, LatticeEffect.Allow);

    private async Task SeedAsync(string tree)
    {
        await _fixture.RegisterTreeAsync(tree);
        await _fixture.GrantAsync(Allow(Writer, LatticeScope.Tree(tree), LatticeOperation.Write, tree));
        using (AuthApiDataClusterFixture.AsSubject(Writer))
        {
            await _fixture.Api.SetAsync(tree, "x/1", [1]);
            await _fixture.Api.SetAsync(tree, "x/2", [2]);
            await _fixture.Api.SetAsync(tree, "y/1", [3]);
        }
    }
}
