using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.State.Tests;

[TestFixture]
public sealed class LatticeStateQueryReadBoundaryTests
{
    private const string TreeId = "orders";
    private const string IndexName = "by-status";

    [TestCase("partial", null)]
    [TestCase("partial", "1")]
    [TestCase("denied", null)]
    [TestCase("anonymous", null)]
    public async Task Dead_letters_require_unfiltered_source_access(string access, string? token)
    {
        using var harness = new Harness(access);

        var page = await harness.Query.ListDeadLettersAsync(new DeadLetterQueueRequest
        {
            TreeId = TreeId,
            PageSize = 1,
            PageToken = token,
        });
        var count = await harness.Query.GetDeadLetterCountAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextPageToken, Is.Null);
            Assert.That(count, Is.Zero);
            Assert.That(harness.Store.ReceivedCalls(), Is.Empty,
                "A refused reader must not reach the system-origin dead-letter store.");
        });
    }

    [TestCase("whole")]
    [TestCase("no-gate")]
    [TestCase("disabled")]
    public async Task Dead_letters_remain_available_to_unfiltered_readers(string access)
    {
        using var harness = new Harness(access);

        var page = await harness.Query.ListDeadLettersAsync(new DeadLetterQueueRequest { TreeId = TreeId });
        var count = await harness.Query.GetDeadLetterCountAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "public/1", "private/2" }));
            Assert.That(page.Entries[1].ValuePreview, Is.EqualTo(new byte[] { 42 }));
            Assert.That(count, Is.EqualTo(2));
        });
    }

    [TestCase(false, null)]
    [TestCase(false, "open")]
    [TestCase(true, null)]
    [TestCase(true, "open")]
    public async Task Tag_catalogs_do_not_reveal_tags_of_keys_outside_a_partial_grant(
        bool indexWide, string? token)
    {
        using var harness = new Harness("partial");
        var request = new CatalogRequest { SourceTreeId = TreeId, IndexName = IndexName, PageToken = token };

        var page = indexWide
            ? await harness.Query.ListIndexTagsAsync(request)
            : await harness.Query.ListTagValuesAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextPageToken, Is.Null);
        });
        Assert.That(harness.Index.ReceivedCalls().Any(c => c.GetMethodInfo().Name == "TagsAsync"), Is.False);
        Assert.That(harness.SingleIndex.ReceivedCalls(), Is.Empty);
    }

    [TestCase("whole")]
    [TestCase("no-gate")]
    [TestCase("disabled")]
    public async Task Tag_catalogs_keep_pagination_for_unfiltered_readers(string access)
    {
        using var harness = new Harness(access);
        foreach (var indexWide in new[] { false, true })
        {
            var request = new CatalogRequest { SourceTreeId = TreeId, IndexName = IndexName, PageSize = 1 };
            var first = indexWide
                ? await harness.Query.ListIndexTagsAsync(request)
                : await harness.Query.ListTagValuesAsync(request);
            var next = request with { PageToken = first.NextPageToken };
            var second = indexWide
                ? await harness.Query.ListIndexTagsAsync(next)
                : await harness.Query.ListTagValuesAsync(next);

            Assert.Multiple(() =>
            {
                Assert.That(first.Entries, Is.EqualTo(new[] { "open" }));
                Assert.That(first.NextPageToken, Is.EqualTo("open"));
                Assert.That(second.Entries, Is.EqualTo(new[] { "secret-diagnosis" }));
                Assert.That(second.NextPageToken, Is.Null);
            });
        }
    }

    [TestCase("no-gate")]
    [TestCase("disabled")]
    [TestCase("whole")]
    public async Task Index_tags_do_not_reveal_other_tenants_even_without_read_visibility(string access)
    {
        using var harness = new Harness(access, ["t/acme/orders", "t/other/secrets"]);
        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        var page = await harness.Query.ListIndexTagsAsync(new CatalogRequest { IndexName = IndexName });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextPageToken, Is.Null);
        });
        Assert.That(harness.Index.ReceivedCalls().Any(c => c.GetMethodInfo().Name == "TagsAsync"), Is.False);
    }

    [Test]
    public async Task Index_tags_remain_available_when_every_covered_tree_is_tenant_visible()
    {
        using var harness = new Harness("no-gate", ["t/acme/orders"]);
        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        var page = await harness.Query.ListIndexTagsAsync(new CatalogRequest { IndexName = IndexName });

        Assert.That(page.Entries, Is.EqualTo(new[] { "open", "secret-diagnosis" }));
    }

    [TestCase(null)]
    [TestCase("t/acme/orders\0public/1")]
    public async Task Tag_members_apply_tenant_isolation_before_presence_probes_and_pagination(string? token)
    {
        using var harness = new Harness("no-gate", ["t/acme/orders", "t/other/secrets"]);
        using var tenant = LatticeActiveTenantContext.With(TenantId.Parse("acme"));

        var page = await harness.Query.ScanTagMembersAsync(new TagMemberScanRequest
        {
            IndexName = IndexName,
            Tag = "open",
            PageSize = 1,
            PageToken = token,
        });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(token is null
                ? new[] { "t/acme/orders" } : Array.Empty<string>()));
            Assert.That(page.NextPageToken, Is.Null,
                "A hidden tenant row must not create a continuation page.");
        });
        Assert.That(harness.ForeignTree.ReceivedCalls(), Is.Empty,
            "Tenant isolation must precede even a presence read of another tenant's tree.");
    }

    private static async IAsyncEnumerable<T> Stream<T>(
        IEnumerable<T> entries,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        bool requireIndexOrigin = false)
    {
        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (requireIndexOrigin)
            {
                Assert.That(LatticeAccessGateContext.IsGateBypassed, Is.True,
                    "Index rows are infrastructure after the source authorization check.");
            }
            yield return entry;
        }
        await Task.CompletedTask;
    }

    private sealed class Harness : IDisposable
    {
        private readonly ServiceProvider _services;
        public ILatticeSchemaDeadLetterStore Store { get; } = Substitute.For<ILatticeSchemaDeadLetterStore>();
        public ILatticeMultiTreeTagIndex Index { get; } = Substitute.For<ILatticeMultiTreeTagIndex>();
        public ILatticeTagIndex SingleIndex { get; } = Substitute.For<ILatticeTagIndex>();
        public ILattice ForeignTree { get; } = Substitute.For<ILattice>();
        public LatticeStateQuery Query { get; }

        public Harness(string access, string[]? covered = null)
        {
            Store.CountAsync(TreeId, Arg.Any<CancellationToken>()).Returns(2);
            Store.ListAsync(TreeId, Arg.Any<CancellationToken>()).Returns(_ => Stream(new[]
            {
                new LatticeSchemaDeadLetterEntry("public/1", [1], 1, "invalid",
                    LatticeSchemaDeadLetterSource.LocalRejected, DateTimeOffset.UnixEpoch),
                new LatticeSchemaDeadLetterEntry("private/2", [42], 1, "invalid",
                    LatticeSchemaDeadLetterSource.LocalRejected, DateTimeOffset.UnixEpoch),
            }));
            Index.CoveredTreesAsync(Arg.Any<CancellationToken>())
                .Returns(Task.FromResult<IReadOnlyList<string>>(covered ?? [TreeId]));
            Index.TagsAsync(Arg.Any<CancellationToken>()).Returns(_ =>
                Stream(new[] { "open", "secret-diagnosis" }, requireIndexOrigin: true));
            SingleIndex.TagsAsync(Arg.Any<CancellationToken>()).Returns(_ =>
                Stream(new[] { "open", "secret-diagnosis" }, requireIndexOrigin: true));
            var members = Substitute.For<ILatticeMultiTreeTagQuery>();
            members.GetAsyncEnumerator(Arg.Any<CancellationToken>()).Returns(call => Stream(new[]
            {
                new TaggedKey("t/acme/orders", "public/1"),
                new TaggedKey("t/other/secrets", "private/2"),
            }, requireIndexOrigin: true).GetAsyncEnumerator(call.Arg<CancellationToken>()));
            Index.WithAllTags(Arg.Any<string[]>()).Returns(members);
            var factory = Substitute.For<ILatticeTagIndexFactory>();
            factory.CreateMultiTree(IndexName, Arg.Any<IReadOnlyCollection<string>?>()).Returns(Index);
            factory.Create(Arg.Any<ILattice>(), IndexName).Returns(SingleIndex);
            var grainFactory = Substitute.For<IGrainFactory>();
            var tree = Substitute.For<ILattice>();
            tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
            tree.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ =>
            {
                Assert.That(LatticeAccessGateContext.IsGateBypassed, Is.False,
                    "The index bypass must not leak into a source-key presence read.");
                return true;
            });
            ForeignTree.ExistsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(true);
            grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(tree);
            grainFactory.GetGrain<ILattice>("t/other/secrets").Returns(ForeignTree);
            ForeignTree.ClearReceivedCalls();
            var services = new ServiceCollection();
            services.AddSingleton(Store);
            services.AddSingleton(factory);
            services.AddSingleton<ITenantEnumerationFilter>(new TenantFilter());
            if (access != "no-gate")
            {
                var gate = Substitute.For<ILatticeAccessGate>();
                gate.AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>())
                    .Returns(access switch
                    {
                        "partial" or "disabled" => LatticeAccessDecision.Filtered(
                            key => key.StartsWith("public/", StringComparison.Ordinal)),
                        "denied" => LatticeAccessDecision.Deny("no grant"),
                        _ => LatticeAccessDecision.Allow(),
                    });
                services.AddSingleton(gate);
                var membership = Substitute.For<ILatticeMembershipContext>();
                membership.ResolveCurrentAsync(Arg.Any<CancellationToken>())
                    .Returns(access == "anonymous" ? LatticeSubject.Anonymous : new LatticeSubject("reader"));
                services.AddSingleton(membership);
            }
            _services = services.BuildServiceProvider();
            Query = new LatticeStateQuery(grainFactory, Substitute.For<IOptionsMonitor<LatticeOptions>>(),
                Options.Create(new LatticeApiStateOptions
                {
                    ReadVisibility = access == "disabled"
                        ? LatticeStateApiReadVisibility.Disabled : LatticeStateApiReadVisibility.Auto,
                }), _services, new NullTenantContextResolver());
        }

        public void Dispose() => _services.Dispose();
    }

    private sealed class TenantFilter : ITenantEnumerationFilter
    {
        public bool IsActive => true;
        public IReadOnlyList<string> Filter(TenantId tenant, IReadOnlyList<string> treeIds) =>
            treeIds.Where(id => id.StartsWith($"t/{tenant}/", StringComparison.Ordinal)).ToArray();
    }
}
