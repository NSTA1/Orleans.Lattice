using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Pins the observability of the hydration-drift producer of
/// <see cref="RepoContextRetrievalPath.KeywordIndexDegraded"/> (issue #2688). The
/// index ranks candidates but none hydrates from the store of record; that outcome
/// used to return silently while its sibling (the thrown-fault branch) logged. Each
/// test drives the real <see cref="RepoContextSearchService.SearchAsync(string, string, int, CancellationToken)"/>
/// entry point, so removing the production log call turns them red.
/// </summary>
[TestFixture]
public sealed class RepoContextSearchServiceHydrationDriftLoggingTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpace Space = new("test-model", 3, normalized: true);

    [Test]
    public async Task Search_logs_a_warning_naming_the_repository_count_and_keys_when_no_candidate_hydrates()
    {
        var gone = RepoContextKeys.File("acme", "src/Gone.cs");
        var alsoGone = RepoContextKeys.File("acme", "src/AlsoGone.cs");
        using var logs = new CapturingLoggerProvider();
        var service = CreateService(IndexReturning(gone, gone, alsoGone), Logger(logs));

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        var warnings = Warnings(logs);
        Assert.Multiple(() =>
        {
            Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded));
            Assert.That(warnings, Has.Length.EqualTo(1), "Exactly one drift warning per degraded query.");
        });

        var message = warnings[0].Message;
        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("repository acme"));
            Assert.That(message, Does.Contain("ranked 3 matches over 2 distinct candidate sources"));
            Assert.That(message, Does.Contain($"[{gone}, {alsoGone}]"),
                "The sample names the non-hydrating keys once each, in rank order.");
            Assert.That(warnings[0].Exception, Is.Null, "Drift is not a thrown fault.");
        });
    }

    [Test]
    public async Task Search_bounds_the_drift_warning_key_sample()
    {
        var keys = new string[RepoContextSearchService.MaxDriftSampleKeys + 3];
        for (var i = 0; i < keys.Length; i++)
        {
            keys[i] = RepoContextKeys.File("acme", $"src/Gone{i}.cs");
        }

        using var logs = new CapturingLoggerProvider();
        var service = CreateService(IndexReturning(keys), Logger(logs));

        await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        var message = Warnings(logs).Single().Message;
        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain($"over {keys.Length} distinct candidate sources"),
                "The candidate count reports the full pool, not the sample.");
            Assert.That(message, Does.Contain(keys[RepoContextSearchService.MaxDriftSampleKeys - 1]));
            Assert.That(message, Does.Not.Contain(keys[RepoContextSearchService.MaxDriftSampleKeys]),
                "Keys beyond the bound are not named.");
        });
    }

    [Test]
    public async Task Search_does_not_log_a_drift_warning_when_a_candidate_hydrates()
    {
        var key = RepoContextKeys.File("acme", "src/Widget.cs");
        var gone = RepoContextKeys.File("acme", "src/Gone.cs");
        using var logs = new CapturingLoggerProvider();
        var service = CreateService(IndexReturning(gone, key), Logger(logs), hydratedKey: key);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("semantic"));
            Assert.That(Warnings(logs), Is.Empty, "A partially hydrating result is not index drift.");
        });
    }

    [Test]
    public async Task Search_skips_the_drift_warning_when_warning_is_disabled()
    {
        var logger = Substitute.For<ILogger<RepoContextSearchService>>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(false);
        var service = CreateService(IndexReturning(RepoContextKeys.File("acme", "src/Gone.cs")), logger);

        var result = await service.SearchAsync("acme", "widget", 10, CancellationToken.None);

        Assert.That(result.RetrievalPath, Is.EqualTo(RepoContextRetrievalPath.KeywordIndexDegraded));
        Assert.Multiple(() =>
        {
            Assert.That(
                logger.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILogger.Log)),
                Is.Zero,
                "No entry is written when Warning is disabled.");
            logger.Received().IsEnabled(LogLevel.Warning);
        });
    }

    [Test]
    public void FormatDriftSample_dedupes_skips_empty_keys_and_keeps_rank_order()
    {
        var matches = new[]
        {
            new RepoContextVectorMatch("v0", "b", 0.9),
            new RepoContextVectorMatch("v1", string.Empty, 0.8),
            new RepoContextVectorMatch("v2", "b", 0.7),
            new RepoContextVectorMatch("v3", "a", 0.6),
        };

        Assert.That(RepoContextSearchService.FormatDriftSample(matches, 5), Is.EqualTo("b, a"));
    }

    [Test]
    public void FormatDriftSample_stops_at_the_bound()
    {
        var matches = new[]
        {
            new RepoContextVectorMatch("v0", "a", 0.9),
            new RepoContextVectorMatch("v1", "b", 0.8),
            new RepoContextVectorMatch("v2", "c", 0.7),
        };

        Assert.That(RepoContextSearchService.FormatDriftSample(matches, 2), Is.EqualTo("a, b"));
    }

    [Test]
    public void FormatDriftSample_returns_empty_for_a_non_positive_bound_or_no_keys()
    {
        var matches = new[] { new RepoContextVectorMatch("v0", "a", 0.9) };

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextSearchService.FormatDriftSample(matches, 0), Is.Empty);
            Assert.That(RepoContextSearchService.FormatDriftSample(Array.Empty<RepoContextVectorMatch>(), 5), Is.Empty);
        });
    }

    [Test]
    public void FormatDriftSample_rejects_null_matches()
    {
        Assert.That(
            () => RepoContextSearchService.FormatDriftSample(null!, 5),
            Throws.ArgumentNullException);
    }

    private static CapturedLogEntry[] Warnings(CapturingLoggerProvider logs)
        => logs.Entries
            .Where(e => e.Level == LogLevel.Warning
                && e.Category == typeof(RepoContextSearchService).FullName)
            .ToArray();

    private static ILogger<RepoContextSearchService> Logger(CapturingLoggerProvider logs)
        => new LoggerFactory(new[] { (ILoggerProvider)logs }).CreateLogger<RepoContextSearchService>();

    private static RepoContextSearchService CreateService(
        IRepoContextSemanticIndex index,
        ILogger<RepoContextSearchService> logger,
        string? hydratedKey = null)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var key = call.ArgAt<string>(0);
                return Task.FromResult(
                    hydratedKey is not null && string.Equals(key, hydratedKey, StringComparison.Ordinal)
                        ? new VersionedValue
                        {
                            Value = Serializer.SerializeToArray(
                                new FileNode { RepoId = "acme", Path = "src/Widget.cs" }),
                        }
                        : new VersionedValue());
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = new RepoContextStore(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            new RepoContextVectorWriter(
                grainFactory,
                Serializer,
                Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory)),
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        return new RepoContextSearchService(
            grainFactory,
            Serializer,
            index,
            store,
            TimeProvider.System,
            logger,
            new RepoContextRetrievalLatencyReporter(),
            AvailableEmbedder());
    }

    private static IEmbeddingProvider AvailableEmbedder()
    {
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(Space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Success(Space, new[] { new ReadOnlyMemory<float>([1f, 0f, 0f]) }));
        return provider;
    }

    private static IRepoContextSemanticIndex IndexReturning(params string[] sourceKeys)
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);

        var matches = new List<RepoContextVectorMatch>(sourceKeys.Length);
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            matches.Add(new RepoContextVectorMatch($"vec-{i}", sourceKeys[i], 1d - (i * 0.01)));
        }

        index.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(matches));
        return index;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }
}
