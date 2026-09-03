using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Bootstrap;

/// <summary>
/// End-to-end unit tests for one <see cref="RepoContextBootstrapService"/>
/// ingestion pass, driven over a real throwaway working tree and an in-memory
/// structural tree.
/// <para>
/// The service is <c>internal sealed</c> and cannot be substituted, so it is
/// constructed for real over substituted collaborators - exactly as
/// <c>RepoIndexRunnerHarness</c> does - but here the structural tree is backed
/// by a range-honouring in-memory store rather than a gate, so a pass runs to
/// completion and its plan, its three self-healing back-fills, its
/// anchor-refresh rewrite, and its swallow-and-log fault arms are all
/// observable.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextBootstrapServicePassTests
{
    private const string RepoId = "acme";

    private BootstrapHarness _harness = null!;

    [SetUp]
    public void SetUp() => _harness = new BootstrapHarness();

    [TearDown]
    public void TearDown() => _harness.Dispose();

    // --- Request validation and the sink-free overload ---

    [Test]
    public void A_request_without_a_repository_root_is_rejected()
    {
        Assert.That(
            async () => await _harness.Service.RunAsync(
                new RepoContextBootstrapRequest { RepoRoot = "  ", RepoId = RepoId }),
            Throws.InstanceOf<ArgumentException>().And.Message.Contains("repository root"));
    }

    [Test]
    public void A_request_without_a_repository_id_is_rejected()
    {
        Assert.That(
            async () => await _harness.Service.RunAsync(
                new RepoContextBootstrapRequest { RepoRoot = _harness.RepoRoot, RepoId = "  " }),
            Throws.InstanceOf<ArgumentException>().And.Message.Contains("repository id"));
    }

    [Test]
    public void A_null_request_is_rejected()
    {
        Assert.That(
            async () => await _harness.Service.RunAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task A_pass_without_a_progress_sink_walks_and_applies()
    {
        // The sink-free overload takes the synchronous walk branch - no
        // concurrent progress pump - and must still commit the full plan.
        _harness.WriteFile("src/a.cs", "class A { }");
        _harness.WriteFile("docs/readme.md", "# readme");

        var result = await _harness.Service.RunAsync(_harness.Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.FilesScanned, Is.EqualTo(2));
            Assert.That(result.FilesAdded, Is.EqualTo(2));
            Assert.That(_harness.Store.Keys, Does.Contain(RepoContextKeys.File(RepoId, "src/a.cs")));
            Assert.That(_harness.Store.Keys, Does.Contain(RepoContextKeys.Repo(RepoId)));
        });
    }

    [Test]
    public async Task A_removed_file_is_deleted_from_the_structural_tree()
    {
        _harness.SeedStoredFile("gone.cs", "class Gone { }");

        var result = await _harness.Service.RunAsync(_harness.Request());

        Assert.Multiple(() =>
        {
            Assert.That(result.FilesRemoved, Is.EqualTo(1));
            Assert.That(_harness.Store.Keys, Does.Not.Contain(RepoContextKeys.File(RepoId, "gone.cs")));
        });
    }

    // --- Anchor-refresh (metadata-changed) rewrite ---

    [Test]
    public async Task An_anchor_stale_file_is_rewritten_without_being_re_embedded()
    {
        // Same bytes, but the stored ingest anchor is far older than the file's
        // modification time, so the stat fast-path cannot clear it. The re-read
        // proves the content identical, which makes it anchor-refresh work:
        // counted as unchanged, never re-embedded, but its node is rewritten.
        const string body = "class Stale { }";
        _harness.WriteFile("stale.cs", body);
        _harness.SeedStoredFile(
            "stale.cs", body,
            anchorWallTicks: 1,
            declaredSymbols: ["Ns.Stale"],
            symbolsProcessed: true,
            contentProcessed: true,
            tokenCount: 11,
            crossReferenced: true);

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.FilesUpdated, Is.Zero, "Identical content must not be reported as an update.");
            Assert.That(result.FilesUnchanged, Is.EqualTo(1),
                "An anchor refresh is reported within the unchanged tally.");
            Assert.That(_harness.ChangedOfferedToIngestor, Is.Empty,
                "An anchor-refreshed file must never be re-embedded.");
        });

        // The rewritten node must carry the stored markers forward rather than
        // blanking them, or the next pass would re-do work already done.
        var node = _harness.ReadStoredFile("stale.cs");
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(node.SymbolsProcessed), Is.Not.Null);
            Assert.That(RepoContextValues.ReadString(node.ContentProcessed), Is.Not.Null);
            Assert.That(RepoContextValues.ReadInt64(node.TokenCount), Is.EqualTo(11));
            Assert.That(RepoContextValues.ReadString(node.CrossReferenced), Is.Not.Null);
            Assert.That(RepoContextValues.ReadString(node.DeclaredSymbols), Is.Not.Null,
                "A node rewritten without a fresh extraction must keep its stored declared set.");
        });
    }

    // --- The three self-healing back-fills ---

    [Test]
    public async Task Each_back_fill_selects_only_the_files_missing_its_marker()
    {
        // a.cs  - supported, no symbol and no content marker  -> symbol + content
        // b.txt - unsupported language, no content marker     -> content only
        // c.cs  - supported, fully processed but never xref'd -> xref only
        // d.cs  - supported, every marker present             -> not selected
        SeedUnchanged("a.cs", "class A { }", symbolsProcessed: false, contentProcessed: false, tokenCount: -1);
        SeedUnchanged("b.txt", "plain text", symbolsProcessed: false, contentProcessed: false, tokenCount: -1);
        SeedUnchanged("c.cs", "class C { }", symbolsProcessed: true, contentProcessed: true, tokenCount: 4, crossReferenced: false);
        SeedUnchanged("d.cs", "class D { }", symbolsProcessed: true, contentProcessed: true, tokenCount: 4, crossReferenced: true);

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.That(result.FilesUnchanged, Is.EqualTo(4));

        // Every back-filled node is rewritten exactly once with its markers
        // resolved; the fully-processed file is left untouched.
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextValues.ReadString(_harness.ReadStoredFile("a.cs").SymbolsProcessed), Is.Not.Null);
            Assert.That(RepoContextValues.ReadString(_harness.ReadStoredFile("a.cs").ContentProcessed), Is.Not.Null);
            Assert.That(RepoContextValues.ReadString(_harness.ReadStoredFile("b.txt").ContentProcessed), Is.Not.Null);
            Assert.That(RepoContextValues.ReadString(_harness.ReadStoredFile("c.cs").CrossReferenced), Is.Not.Null);
        });
    }

    [Test]
    public async Task A_content_only_back_fill_preserves_the_stored_declared_symbol_set()
    {
        // b.cs is selected by the content back-fill only, so this pass performs
        // no fresh symbol extraction for it. The full-node rewrite must carry
        // its stored declared set forward rather than blanking it.
        SeedUnchanged(
            "b.cs", "class B { }",
            symbolsProcessed: true,
            contentProcessed: false,
            tokenCount: -1,
            crossReferenced: true,
            declaredSymbols: ["Ns.B"]);

        await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.That(
            RepoContextValues.ReadString(_harness.ReadStoredFile("b.cs").DeclaredSymbols),
            Is.EqualTo(DeclaredSymbolNames.Encode(["Ns.B"])));
    }

    [Test]
    public async Task A_fully_processed_unchanged_repository_is_a_no_op()
    {
        SeedUnchanged("done.cs", "class Done { }", symbolsProcessed: true, contentProcessed: true, tokenCount: 4, crossReferenced: true);
        var before = _harness.AtomicWrites;

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.FilesUnchanged, Is.EqualTo(1));
            Assert.That(_harness.AtomicWrites, Is.EqualTo(before),
                "A plan with nothing to do and no back-fill must not commit a chunk.");
        });
    }

    // --- Vectorisation arms ---

    [Test]
    public async Task Vectorising_progress_is_logged_once_the_heartbeat_interval_is_crossed()
    {
        _harness.WriteFile("a.cs", "class A { }");
        // Report a count past the heartbeat interval, then one below it, so both
        // the logging and the coalescing arm run.
        _harness.OnIngest = async (report, ct) =>
        {
            await report(150, ct);
            await report(151, ct);
            return 150;
        };

        var result = await _harness.Service.RunAsync(_harness.Request(), _harness.Progress);

        Assert.Multiple(() =>
        {
            Assert.That(result.FilesScanned, Is.EqualTo(1));
            Assert.That(_harness.ProgressUpdates.Any(u => u.FilesEmbedded == 150), Is.True);
            Assert.That(_harness.ProgressUpdates.Any(u => u.FilesEmbedded == 151), Is.True);
        });
    }

    [Test]
    public async Task An_embedded_memory_passage_count_is_reported()
    {
        _harness.WriteFile("a.cs", "class A { }");
        _harness.MemoryIngestResult = 3;

        var result = await _harness.Service.RunAsync(_harness.Request(), progress: null);

        Assert.That(result.FilesScanned, Is.EqualTo(1));
        await _harness.VectorIngestor.Received(1).IngestMemoryAsync(
            RepoId, Arg.Any<IReadOnlyList<string>>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_memory_vectorisation_fault_fails_the_run_after_every_other_arm_has_run()
    {
        // Each vectorisation arm gets its turn; the first failure is then
        // surfaced so the run is reported failed and re-driven, rather than
        // silently reporting a partial view as success.
        _harness.WriteFile("a.cs", "class A { }");
        _harness.MemoryIngestFault = new InvalidOperationException("memory plane down");

        Assert.That(
            async () => await _harness.Service.RunAsync(_harness.Request(), progress: null),
            Throws.InstanceOf<InvalidOperationException>().And.Message.EqualTo("memory plane down"));

        // The structural apply still committed - the arms that succeeded keep
        // their work either way.
        Assert.That(_harness.Store.Keys, Does.Contain(RepoContextKeys.File(RepoId, "a.cs")));
    }

    [Test]
    public void A_file_vectorisation_fault_fails_the_run()
    {
        _harness.WriteFile("a.cs", "class A { }");
        _harness.OnIngest = (_, _) => throw new InvalidOperationException("embedder down");

        Assert.That(
            async () => await _harness.Service.RunAsync(_harness.Request(), progress: null),
            Throws.InstanceOf<InvalidOperationException>().And.Message.EqualTo("embedder down"));
    }

    [Test]
    public void A_cancelled_pass_surfaces_the_cancellation()
    {
        _harness.WriteFile("a.cs", "class A { }");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _harness.Service.RunAsync(_harness.Request(), null, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- The concurrent walk-progress pump ---

    [Test]
    public async Task The_walk_progress_pump_reports_each_distinct_sampled_count()
    {
        // The pump samples on a 500 ms cadence, which no throwaway working tree
        // is slow enough to reach, so it is driven directly. It is a private
        // static helper with no collaborators beyond its four arguments, so
        // invoking it needs no production seam - only reflection.
        var sink = new RecordingSink();
        var count = 0;
        using var walkComplete = new CancellationTokenSource();

        var pump = InvokePump(sink, () => Volatile.Read(ref count), walkComplete.Token, CancellationToken.None);

        Volatile.Write(ref count, 7);
        await WaitForAsync(() => sink.Updates.Any(u => u.FilesScanned == 7));
        // A repeat sample of the same count must not be re-reported.
        await Task.Delay(TimeSpan.FromMilliseconds(700));
        Volatile.Write(ref count, 9);
        await WaitForAsync(() => sink.Updates.Any(u => u.FilesScanned == 9));

        Volatile.Write(ref count, 11);
        walkComplete.Cancel();
        await pump;

        Assert.Multiple(() =>
        {
            Assert.That(sink.Updates.Count(u => u.FilesScanned == 7), Is.EqualTo(1),
                "An unchanged sample must be coalesced, not re-reported every tick.");
            Assert.That(sink.Updates.Select(u => u.FilesScanned), Does.Contain(11),
                "The final count must be reported after the walk signals completion.");
        });
    }

    [Test]
    public async Task The_walk_progress_pump_drops_its_final_report_when_the_run_is_cancelled()
    {
        // Host shutdown or a repository removal cancels the run itself; the
        // walk's partial count is not worth reporting as the run unwinds, and
        // the cancellation must not escape the pump.
        var sink = new RecordingSink();
        using var walkComplete = new CancellationTokenSource();
        using var runCancelled = new CancellationTokenSource();
        runCancelled.Cancel();

        walkComplete.Cancel();
        await InvokePump(sink, () => 4, walkComplete.Token, runCancelled.Token);

        Assert.That(sink.Updates, Is.Empty);
    }

    private static Task InvokePump(
        IRepoIndexProgressSink sink,
        Func<int> currentCount,
        CancellationToken walkComplete,
        CancellationToken cancellationToken)
    {
        var method = typeof(RepoContextBootstrapService).GetMethod(
            "PumpWalkProgressAsync", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.That(method, Is.Not.Null, "PumpWalkProgressAsync was renamed; update this test.");
        return (Task)method!.Invoke(null, [sink, currentCount, walkComplete, cancellationToken])!;
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline && !condition())
        {
            await Task.Delay(10);
        }
    }

    private void SeedUnchanged(
        string relativePath,
        string body,
        bool symbolsProcessed,
        bool contentProcessed,
        long tokenCount,
        bool crossReferenced = false,
        IReadOnlyList<string>? declaredSymbols = null)
    {
        _harness.WriteFile(relativePath, body);
        // An anchor far in the future lets the walk's stat fast-path settle the
        // file as unchanged without a read, which is what puts it in the pure
        // unchanged set the back-fills draw from.
        _harness.SeedStoredFile(
            relativePath, body,
            anchorWallTicks: DateTime.UtcNow.Ticks + TimeSpan.TicksPerDay,
            declaredSymbols: declaredSymbols,
            symbolsProcessed: symbolsProcessed,
            contentProcessed: contentProcessed,
            tokenCount: tokenCount,
            crossReferenced: crossReferenced);
    }

    private sealed class RecordingSink : IRepoIndexProgressSink
    {
        private readonly List<RepoIndexProgressUpdate> _updates = [];

        public IReadOnlyList<RepoIndexProgressUpdate> Updates
        {
            get { lock (_updates) { return _updates.ToArray(); } }
        }

        public ValueTask ReportAsync(RepoIndexProgressUpdate update, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lock (_updates) { _updates.Add(update); }
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Builds a real <see cref="RepoContextBootstrapService"/> over a throwaway
    /// working tree and an in-memory structural store whose range scans honour
    /// the requested window (a store that ignores it makes the resilient
    /// streaming scan re-read forever).
    /// </summary>
    private sealed class BootstrapHarness : IDisposable
    {
        private static readonly IServiceProvider SerializerServices = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();

        private readonly Serializer<FileNode> _fileNodes =
            SerializerServices.GetRequiredService<Serializer<FileNode>>();

        internal BootstrapHarness()
        {
            RepoRoot = Path.Combine(Path.GetTempPath(), "lattice-rcbootstrap-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(RepoRoot);

            Tree = Substitute.For<ILattice>();
            Tree.EntriesAsync(
                Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
                .Returns(call => Enumerate(call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
            Tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    lock (Store)
                    {
                        return Task.FromResult(Store.TryGetValue(call.ArgAt<string>(0), out var v) ? v : null);
                    }
                });
            Tree.SetManyAtomicAsync(
                Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<IReadOnlyList<string>>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    Apply(call.ArgAt<List<KeyValuePair<string, byte[]>>>(0), call.ArgAt<IReadOnlyList<string>>(1));
                    return Task.CompletedTask;
                });

            GrainFactory = Substitute.For<IGrainFactory>();
            GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(_ => Tree);

            SymbolExtractor = Substitute.For<ISymbolExtractor>();
            SymbolExtractor.Supports(Arg.Any<string>())
                .Returns(call => string.Equals(call.ArgAt<string>(0), "csharp", StringComparison.Ordinal));
            SymbolExtractor.Extract(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
                .Returns(_ => Array.Empty<ExtractedSymbol>());

            var tokenCounter = Substitute.For<IRepoContextTokenCounter>();
            tokenCounter.CountTokens(Arg.Any<string>()).Returns(4);

            VectorIngestor = Substitute.For<IRepoContextVectorIngestor>();
            VectorIngestor.IngestAsync(
                Arg.Any<string>(), Arg.Any<string>(),
                Arg.Any<IReadOnlyList<RepoFileEntry>>(), Arg.Any<IReadOnlyList<RepoFileEntry>>(),
                Arg.Any<Func<int, CancellationToken, ValueTask>>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    ChangedOfferedToIngestor = call.ArgAt<IReadOnlyList<RepoFileEntry>>(2);
                    var report = call.ArgAt<Func<int, CancellationToken, ValueTask>>(4);
                    var ct = call.ArgAt<CancellationToken>(5);
                    return OnIngest is null
                        ? new ValueTask<int>(0)
                        : new ValueTask<int>(OnIngest(report, ct));
                });
            VectorIngestor.IngestMemoryAsync(
                Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<CancellationToken>())
                .Returns(_ => MemoryIngestFault is not null
                    ? Task.FromException<int>(MemoryIngestFault)
                    : Task.FromResult(MemoryIngestResult));

            Service = new RepoContextBootstrapService(
                GrainFactory,
                _fileNodes,
                SerializerServices.GetRequiredService<Serializer<RepoNode>>(),
                VectorIngestor,
                new RepoContextSymbolReconciler(
                    GrainFactory,
                    SerializerServices.GetRequiredService<Serializer<SymbolRecord>>(),
                    SerializerServices.GetRequiredService<Serializer<CrossReferenceNode>>(),
                    SymbolExtractor,
                    NullLogger<RepoContextSymbolReconciler>.Instance),
                new RepoContextContentReconciler(
                    GrainFactory,
                    SerializerServices.GetRequiredService<Serializer<ContentRecord>>(),
                    tokenCounter,
                    NullLogger<RepoContextContentReconciler>.Instance),
                SymbolExtractor,
                // No allowed roots, so the guard is inert and the throwaway tree
                // resolves unchanged; the guard has its own tests.
                new RepoContextWorkspaceGuard([]),
                TimeProvider.System,
                new RepoContextIndexingOptions(),
                NullLogger<RepoContextBootstrapService>.Instance);
        }

        internal string RepoRoot { get; }

        internal SortedDictionary<string, byte[]> Store { get; } = new(StringComparer.Ordinal);

        internal ILattice Tree { get; }

        internal IGrainFactory GrainFactory { get; }

        internal ISymbolExtractor SymbolExtractor { get; }

        internal IRepoContextVectorIngestor VectorIngestor { get; }

        internal RepoContextBootstrapService Service { get; }

        internal RecordingSink Progress { get; } = new();

        internal IReadOnlyList<RepoIndexProgressUpdate> ProgressUpdates => Progress.Updates;

        internal int AtomicWrites { get; private set; }

        internal IReadOnlyList<RepoFileEntry> ChangedOfferedToIngestor { get; private set; } = [];

        internal Func<Func<int, CancellationToken, ValueTask>, CancellationToken, Task<int>>? OnIngest { get; set; }

        internal int MemoryIngestResult { get; set; }

        internal Exception? MemoryIngestFault { get; set; }

        internal RepoContextBootstrapRequest Request() => new() { RepoRoot = RepoRoot, RepoId = RepoId };

        internal void WriteFile(string relativePath, string body)
        {
            var absolute = Path.Combine(RepoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(absolute)!);
            File.WriteAllText(absolute, body);
        }

        /// <summary>
        /// Writes the stored file node a prior pass would have left behind, so the
        /// reconcile diff and the back-fill selectors see a pre-existing index.
        /// </summary>
        internal void SeedStoredFile(
            string relativePath,
            string body,
            long anchorWallTicks = 1,
            IReadOnlyList<string>? declaredSymbols = null,
            bool symbolsProcessed = false,
            bool contentProcessed = false,
            long tokenCount = -1,
            bool crossReferenced = false)
        {
            var clock = new HybridLogicalClock { WallClockTicks = anchorWallTicks, Counter = 0 };
            var bytes = System.Text.Encoding.UTF8.GetBytes(body);
            var node = new FileNode
            {
                RepoId = RepoId,
                Path = relativePath,
                Digest = RepoContextValues.Lww(FileDigest.Compute(bytes), clock),
                Language = RepoContextValues.Lww(LanguageClassifier.Classify(relativePath), clock),
                SizeBytes = RepoContextValues.Lww(bytes.LongLength, clock),
                LastIngested = RepoContextValues.Lww("seed", clock),
            };
            if (declaredSymbols is { Count: > 0 })
                node = node with { DeclaredSymbols = RepoContextValues.Lww(DeclaredSymbolNames.Encode(declaredSymbols), clock) };
            if (symbolsProcessed)
                node = node with { SymbolsProcessed = RepoContextValues.Lww("1", clock) };
            if (contentProcessed)
                node = node with { ContentProcessed = RepoContextValues.Lww("1", clock) };
            if (tokenCount >= 0)
                node = node with { TokenCount = RepoContextValues.Lww(tokenCount, clock) };
            if (crossReferenced)
                node = node with { CrossReferenced = RepoContextValues.Lww("1", clock) };

            lock (Store)
            {
                Store[RepoContextKeys.File(RepoId, relativePath)] = _fileNodes.SerializeToArray(node);
            }
        }

        internal FileNode ReadStoredFile(string relativePath)
        {
            lock (Store)
            {
                return _fileNodes.Deserialize(Store[RepoContextKeys.File(RepoId, relativePath)]);
            }
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(RepoRoot, recursive: true);
            }
            catch (IOException)
            {
                // Best-effort cleanup of a throwaway tree; a locked file must not
                // fail a test that already made its assertion.
            }
        }

        private void Apply(List<KeyValuePair<string, byte[]>> upserts, IReadOnlyList<string> deletes)
        {
            lock (Store)
            {
                AtomicWrites++;
                foreach (var (key, value) in upserts)
                {
                    Store[key] = value;
                }

                foreach (var key in deletes)
                {
                    Store.Remove(key);
                }
            }
        }

        private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Enumerate(
            string? startInclusive, string? endExclusive)
        {
            KeyValuePair<string, byte[]>[] page;
            lock (Store)
            {
                page = Store
                    .Where(kv =>
                        (startInclusive is null || string.CompareOrdinal(kv.Key, startInclusive) >= 0)
                        && (endExclusive is null || string.CompareOrdinal(kv.Key, endExclusive) < 0))
                    .ToArray();
            }

            foreach (var entry in page)
            {
                yield return entry;
            }

            await Task.CompletedTask;
        }
    }
}
