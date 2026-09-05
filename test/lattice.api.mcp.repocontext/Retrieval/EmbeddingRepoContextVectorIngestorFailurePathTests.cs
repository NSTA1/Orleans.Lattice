using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for the resilience seams in
/// <see cref="EmbeddingRepoContextVectorIngestor"/> that only run when an
/// underlying write faults or a file cannot be read: the contentless-marker
/// bookkeeping is swallowed rather than allowed to sink a pass, a file whose
/// embedding batch never succeeds is reported as embedding nothing, an unreadable
/// file (missing or a directory) is skipped and retried, and a symbol pass that
/// embedded nothing because every coverage probe faulted surfaces the fault.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo (the reserved
/// vector trees) via <see cref="RepoContextMcpHarness"/> and reads file content off
/// a temp repo, so it is excluded from the fast unit dev loop. Faults are injected
/// at the grain call - the real seam - via <see cref="LatticeTreeFaultInjector"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorFailurePathTests
{
    private const string RepoId = "acme";

    private readonly List<string> _tempRoots = new();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [TearDown]
    public void TearDown()
    {
        foreach (var root in _tempRoots)
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }

        _tempRoots.Clear();
    }

    private string NewRepo()
    {
        var root = Path.Combine(Path.GetTempPath(), "rc-ingest-fail-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private RepoFileEntry Write(string root, string relativePath, string content)
    {
        var full = Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllText(full, content);
        return new RepoFileEntry(relativePath, "digest-" + relativePath, content.Length, "csharp");
    }

    private static RepoContextMcpHarnessOptions FaultingOptions(LatticeTreeFaultInjector injector)
    {
        injector.FailFirst = int.MaxValue;
        return new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        };
    }

    private static EmbeddingRepoContextVectorIngestor Ingestor(
        RepoContextMcpHarness harness, IEmbeddingProvider? provider)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            provider);

    private static async Task<bool> IsEmbeddedAsync(
        RepoContextMcpHarness harness, string relativePath, CancellationToken ct)
    {
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, ct);
        return members.Contains(VectorCodec.SourceId(RepoContextKeys.File(RepoId, relativePath)));
    }

    [Test]
    public async Task Ingest_when_marking_contentless_faults_swallows_and_leaves_no_marker()
    {
        var root = NewRepo();
        var empty = Write(root, "empty.cs", "   ");

        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.ApplyCrdtDeltaManyAsync),
        };
        await using var harness = await RepoContextMcpHarness.StartAsync(
            FaultingOptions(injector), Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        // The whitespace-only file has no embeddable passage, so the arm tries to
        // write a "contentless" marker. That write faults, but the pass must not
        // throw - the marker is recomputed next reconcile.
        var embedded = await ingestor.IngestAsync(
            RepoId, root, new[] { empty }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var sourceKey = RepoContextKeys.File(RepoId, "empty.cs");
        var coverage = await writer.ProbeCoverageAsync(RepoId, new[] { sourceKey }, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(0), "A contentless file embeds nothing.");
            Assert.That(injector.Failed, Is.GreaterThan(0), "The contentless-marker write was faulted.");
            Assert.That(coverage.Contentless, Does.Not.Contain(VectorCodec.SourceId(sourceKey)),
                "The faulted marker write left no persisted contentless marker.");
        });
    }

    [Test]
    public async Task Ingest_when_every_embedding_batch_fails_embeds_nothing()
    {
        var root = NewRepo();
        var a = Write(root, "a.cs", "class A { void Alpha() { System.Console.WriteLine(1); } }");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        // A provider that is available but fails every embed call: the batch is
        // skipped and nothing lands, so the file falls back to keyword recall.
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider { FailEmbeds = true });

        var embedded = await ingestor.IngestAsync(
            RepoId, root, new[] { a }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var aEmbedded = await IsEmbeddedAsync(harness, "a.cs", Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(0), "No embedding batch succeeded.");
            Assert.That(aEmbedded, Is.False, "A file whose embed call failed records no membership.");
        });
    }

    [Test]
    public async Task Ingest_skips_a_changed_file_that_is_missing_on_disk()
    {
        var root = NewRepo();
        var present = Write(root, "present.cs", "class P { void Pi() {} }");

        // A changed entry whose file was never written to disk: the read raises an
        // IOException, which the arm treats as transient and skips, leaving the
        // real file to embed.
        var missing = new RepoFileEntry("gone.cs", "digest-gone", 10, "csharp");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        var embedded = await ingestor.IngestAsync(
            RepoId, root, new[] { present, missing }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var presentEmbedded = await IsEmbeddedAsync(harness, "present.cs", Ct);
        var goneEmbedded = await IsEmbeddedAsync(harness, "gone.cs", Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "Only the readable file is embedded.");
            Assert.That(presentEmbedded, Is.True);
            Assert.That(goneEmbedded, Is.False,
                "The unreadable file is left uncovered for a later retry.");
        });
    }

    [Test]
    public async Task Ingest_skips_a_changed_path_that_is_a_directory()
    {
        var root = NewRepo();
        var present = Write(root, "present.cs", "class P { void Pi() {} }");

        // A path that resolves to a directory: on Windows reading it raises an
        // UnauthorizedAccessException, a different catch arm than the missing-file
        // case, which the ingestor also treats as skip-and-retry.
        Directory.CreateDirectory(Path.Combine(root, "adir.cs"));
        var directoryEntry = new RepoFileEntry("adir.cs", "digest-adir", 0, "csharp");

        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        var embedded = await ingestor.IngestAsync(
            RepoId, root, new[] { present, directoryEntry }, Array.Empty<RepoFileEntry>(), onProgress: null, Ct);

        var presentEmbedded = await IsEmbeddedAsync(harness, "present.cs", Ct);
        var dirEmbedded = await IsEmbeddedAsync(harness, "adir.cs", Ct);
        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "Only the readable file is embedded.");
            Assert.That(presentEmbedded, Is.True);
            Assert.That(dirEmbedded, Is.False,
                "The directory path is skipped, not embedded.");
        });
    }

    [Test]
    public async Task IngestSymbols_when_every_probe_faults_and_nothing_embeds_rethrows()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.GetManyAsync),
        };
        await using var harness = await RepoContextMcpHarness.StartAsync(
            FaultingOptions(injector), Ct);

        var serializer = harness.Services.GetRequiredService<Serializer>();
        var symbolTree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var keys = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var fqn = $"Acme.Probe.Symbol{i:D2}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Method };
            var key = RepoContextKeys.Symbol(RepoId, fqn);
            await symbolTree.SetAsync(key, serializer.SerializeToArray(record), Ct);
            keys.Add(key);
        }

        var ingestor = Ingestor(harness, new FakeEmbeddingProvider());

        // Every coverage probe faults, so the single page is skipped and nothing is
        // embedded. A pass that achieved nothing must surface the first fault.
        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(RepoId, keys, Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>());
        Assert.That(injector.Failed, Is.GreaterThan(0), "At least one coverage probe was faulted.");
    }
}
