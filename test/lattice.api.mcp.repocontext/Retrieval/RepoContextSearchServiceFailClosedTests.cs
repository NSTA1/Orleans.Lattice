using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextSearchService"/>'s fail-closed contract.
/// The read-only <c>repocontext_search</c> tool must never let an exception
/// escape as a protocol error. The harness-based tests cover the semantic path
/// degrading to keyword recall; this fixture covers the terminal case the
/// integration harness cannot easily provoke - the keyword/structural fallback
/// itself faulting (as it would when the stale-leaf-projection activation fault
/// that can trip the semantic path also trips the tree grains the keyword scan
/// walks) - and asserts the service degrades to the empty result.
/// </summary>
[TestFixture]
public sealed class RepoContextSearchServiceFailClosedTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    [Test]
    public async Task Search_returns_empty_when_the_keyword_fallback_throws()
    {
        // No embedder is bound, so the semantic path is skipped and the keyword
        // scan runs. Make the first tree the scan opens fault the way a stale
        // leaf projection would, and assert the tool degrades to the terminal
        // empty result instead of surfacing the exception.
        var tree = Substitute.For<ILattice>();
        tree.OpenEntryCursorAsync().ThrowsForAnyArgs(
            new InvalidOperationException("simulated stale leaf projection activation fault"));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = new RepoContextStore(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        var service = new RepoContextSearchService(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            embeddingProvider: null);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("empty"),
                "A throw on the keyword fallback must degrade to the terminal empty result.");
            Assert.That(result.Hits, Is.Empty);
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            Assert.That(result.Query, Is.EqualTo("widget"));
        });
    }
}
