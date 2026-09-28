using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeStateQuery.ListCoveredTreesAsync"/> with
/// auth-backed visibility <i>enabled</i>.
/// </summary>
/// <remarks>
/// <para>
/// The endpoint serves its page along one of two paths, chosen by whether a
/// caller subject resolved. With visibility off nothing downstream can thin the
/// set, so the page is served by a bounded top-N selection that never buffers
/// the covered set. With visibility on any tree may still be dropped by the
/// per-tree read check, so the whole set is buffered and sorted instead.
/// </para>
/// <para>
/// Every existing fixture for this endpoint runs on a cluster with no access
/// gate registered, so only the first path was ever taken: the buffering arm,
/// the sort that follows it, and the per-tree omission that is the entire reason
/// the second path exists were all unexercised. These tests drive the second
/// path directly, which also pins the security property - a covered tree the
/// caller may not read must not be disclosed by the covered-tree listing.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeStateQueryCoveredTreeVisibilityTests
{
    private const string IndexName = "by-status";

    /// <summary>
    /// Builds a query whose tag index covers <paramref name="coveredTrees"/>.
    /// Passing <paramref name="readableTrees"/> registers an access gate and a
    /// membership context, which is what turns visibility on; leaving it
    /// <see langword="null"/> leaves visibility off (the pre-existing shape).
    /// </summary>
    private static LatticeStateQuery CreateQuery(
        string[] coveredTrees,
        string[]? readableTrees = null)
    {
        var index = Substitute.For<ILatticeMultiTreeTagIndex>();
        index.CoveredTreesAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([.. coveredTrees]));

        var factory = Substitute.For<ILatticeTagIndexFactory>();
        factory.CreateMultiTree(IndexName, Arg.Any<IReadOnlyCollection<string>?>()).Returns(index);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var services = new ServiceCollection();
        services.AddSingleton(factory);
        if (readableTrees is not null)
        {
            services.AddSingleton<ILatticeAccessGate>(new TreeScopedGate(readableTrees));
            services.AddSingleton<ILatticeMembershipContext>(new FixedSubject());
        }

        return new LatticeStateQuery(
            Substitute.For<IGrainFactory>(),
            options,
            Options.Create(new LatticeApiStateOptions()),
            services.BuildServiceProvider(),
            new NullTenantContextResolver());
    }

    [Test]
    public async Task ListCoveredTreesAsync_omits_a_covered_tree_the_subject_cannot_read()
    {
        var query = CreateQuery(
            coveredTrees: ["orders", "archive", "widgets"],
            readableTrees: ["archive", "widgets"]);

        var page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = IndexName });

        Assert.That(page.Entries, Is.EqualTo(new[] { "archive", "widgets" }),
            "A covered tree the caller may not read must not be disclosed by the listing.");
    }

    [Test]
    public async Task ListCoveredTreesAsync_with_visibility_disabled_returns_every_covered_tree()
    {
        // The negative control. Without it the assertion above is equally
        // satisfied by an endpoint that drops trees for some unrelated reason,
        // or by one whose covered set was never populated.
        var query = CreateQuery(coveredTrees: ["orders", "archive", "widgets"]);

        var page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = IndexName });

        Assert.That(page.Entries, Is.EqualTo(new[] { "archive", "orders", "widgets" }),
            "With no gate registered the covered set must be served unfiltered.");
    }

    [Test]
    public async Task ListCoveredTreesAsync_orders_the_visible_page_ordinally()
    {
        // The visibility-enabled path sorts a buffered list rather than running
        // the bounded top-N selection, so its ordering is a separate claim from
        // the one the unfiltered path already carries.
        var query = CreateQuery(
            coveredTrees: ["widgets", "orders", "archive"],
            readableTrees: ["widgets", "orders", "archive"]);

        var page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = IndexName });

        Assert.That(page.Entries, Is.EqualTo(new[] { "archive", "orders", "widgets" }),
            "The buffered path must order ordinally regardless of the covered set's own order.");
    }

    [Test]
    public async Task ListCoveredTreesAsync_pages_the_visible_set_with_a_continuation_token()
    {
        var query = CreateQuery(
            coveredTrees: ["widgets", "orders", "archive"],
            readableTrees: ["widgets", "orders", "archive"]);

        var first = await query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = IndexName, PageSize = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries, Is.EqualTo(new[] { "archive", "orders" }));
            Assert.That(first.NextPageToken, Is.EqualTo("orders"));
        });

        var second = await query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = IndexName, PageSize = 2, PageToken = first.NextPageToken });

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries, Is.EqualTo(new[] { "widgets" }));
            Assert.That(second.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListCoveredTreesAsync_pages_past_a_tree_the_subject_cannot_read()
    {
        // Pruning happens after the page-token filter and the ordering, so an
        // unreadable tree must not consume a slot in the page it was removed
        // from - the page still fills to its size from the readable remainder.
        var query = CreateQuery(
            coveredTrees: ["alpha", "bravo", "charlie", "delta"],
            readableTrees: ["alpha", "charlie", "delta"]);

        var page = await query.ListCoveredTreesAsync(
            new CatalogRequest { IndexName = IndexName, PageSize = 2 });

        Assert.That(page.Entries, Is.EqualTo(new[] { "alpha", "charlie" }),
            "The hidden tree must be skipped over, not left as a gap in the page.");
    }

    [Test]
    public async Task ListCoveredTreesAsync_returns_an_empty_page_when_no_covered_tree_is_readable()
    {
        var query = CreateQuery(
            coveredTrees: ["orders", "archive"],
            readableTrees: []);

        var page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = IndexName });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty,
                "A caller entitled to nothing must get an empty page, not the covered set.");
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    /// <summary>An access gate that allows reads on a fixed set of tree ids.</summary>
    private sealed class TreeScopedGate(string[] readableTrees) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
            => new(readableTrees.Contains(request.TreeId, StringComparer.Ordinal)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not readable"));
    }

    /// <summary>A membership context that always resolves the same named subject.</summary>
    private sealed class FixedSubject : ILatticeMembershipContext
    {
        private static readonly LatticeSubject Subject = new("alice");

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(Subject);
    }
}
