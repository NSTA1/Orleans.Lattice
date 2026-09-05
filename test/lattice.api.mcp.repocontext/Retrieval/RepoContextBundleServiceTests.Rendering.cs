using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Covers the bundle service's <b>rendering</b> surface at the detail levels and record
/// shapes the happy-path fixtures never reach: the outline and paths projections, the
/// plain (non-reusable) rendering used for a file whose body cannot be read, the
/// pointer unit every level falls back to, and the per-candidate memoisation that stops
/// an auto degrade re-reading the same record once per level.
/// <para>
/// These are not edge cases in practice. An index mid-ingest routinely carries a
/// structural node whose content projection has not landed yet, and a
/// <c>detail: "paths"</c> or <c>detail: "outline"</c> request with a session is the
/// cheapest and therefore most common way an agent uses this tool.
/// </para>
/// </summary>
public sealed partial class RepoContextBundleServiceTests
{
    // --- Records whose content projection is absent -------------------------------
    //
    // The structural node lands before the content projection does, so a file that is
    // indexed but not yet content-processed is a normal steady state, not a fault. The
    // bundle must still answer for it - degrading to the path - rather than dropping it.

    [Test]
    public async Task Build_at_slices_detail_falls_back_to_the_path_when_the_file_has_no_stored_content()
    {
        const string path = "src/WidgetBundle.cs";
        var service = BuildServiceWithoutContent((path, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget bundle", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].Content, Is.EqualTo(path),
                "With no body to slice, the entry degrades to the path rather than being dropped.");
            Assert.That(result.Entries[0].FullReadTokenCount, Is.EqualTo(4096),
                "The stored whole-file cost is still reported, so the caller can judge a full read.");
            Assert.That(result.Detail, Is.EqualTo("slices"));
        });
    }

    [TestCase(RepoContextContextDetail.Slices, "slices")]
    [TestCase(RepoContextContextDetail.Outline, "outline")]
    [TestCase(RepoContextContextDetail.Paths, "paths")]
    public async Task Build_with_reuse_engaged_renders_a_body_less_file_plainly_at_every_detail_level(
        RepoContextContextDetail detail, string expectedLabel)
    {
        // Without a body the file cannot be versioned, so no receipt can be minted for
        // it and no reuse bookkeeping applies. It must still be delivered - plainly -
        // instead of being silently skipped because the reuse path could not hash it.
        const string path = "src/WidgetBundle.cs";
        var service = BuildServiceWithoutContent((path, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget bundle", 10, 10_000, detail,
            seen: null, known: null, session: "s-plain", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].Content, Is.EqualTo(path));
            Assert.That(result.Entries[0].Units, Is.Empty,
                "An unversionable file yields no reusable units - a receipt would not be honourable.");
            Assert.That(result.Entries[0].ContentHash, Is.Null.Or.Empty,
                "No body means no content hash, so no whole-file possession can ever be claimed.");
            Assert.That(result.Detail, Is.EqualTo(expectedLabel));
        });
    }

    [Test]
    public async Task Build_with_reuse_engaged_renders_a_body_less_file_at_outline_detail_from_its_symbols()
    {
        // The outline projection is independent of the content projection: a file whose
        // body has not landed can still be outlined from its declared symbols, which is
        // strictly more useful than degrading to the bare path.
        const string path = "src/WidgetBundle.cs";
        var service = BuildServiceWithoutContent(
            (path, 4096L),
            symbols: [("Acme.WidgetBundle", "public sealed class WidgetBundle")]);

        var result = await service.BuildAsync(
            RepoId, "widget bundle", 10, 10_000, RepoContextContextDetail.Outline,
            seen: null, known: null, session: "s-plain-outline", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].Content, Is.EqualTo("public sealed class WidgetBundle"));
            Assert.That(result.Entries[0].Content, Is.Not.EqualTo(path),
                "An outline must be preferred over the bare path when symbols are available.");
        });
    }

    // --- The pointer unit -----------------------------------------------------------

    [Test]
    public async Task Build_at_paths_detail_with_reuse_engaged_delivers_one_reusable_pointer_unit()
    {
        // A paths bundle is the cheapest thing this tool can deliver, and it is still
        // reusable: the single pointer unit carries a receipt so a later call in the same
        // session is not charged for re-learning the same path.
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { } // widget lattice bundle";
        var service = BuildService((path, body, 4096L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Paths,
            seen: null, known: null, session: "s-paths", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Detail, Is.EqualTo("paths"));
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            var entry = result.Entries[0];
            Assert.That(entry.Content, Is.EqualTo(path));
            Assert.That(entry.Units, Has.Count.EqualTo(1), "A paths entry is exactly one pointer unit.");
            Assert.That(entry.Units[0].Kind, Is.EqualTo(RepoContextReuse.PointerKind));
            Assert.That(entry.Units[0].TokenCount, Is.EqualTo(Counter.CountTokens(path)),
                "The pointer unit's cost is the path's own exact BPE cost.");
            Assert.That(entry.Units[0].Symbol, Is.Null);
            Assert.That(entry.Units[0].Receipt, Is.Not.Null.And.Not.Empty);
            Assert.That(entry.ContentHash, Is.Not.Null.And.Not.Empty,
                "The file has a body, so it is versioned even though only its path was delivered.");
            Assert.That(entry.FullReadTokenCount, Is.EqualTo(4096),
                "The stored whole-file cost rides along so the caller can price a full read.");
        });
    }

    [Test]
    public async Task Build_at_paths_detail_suppresses_a_pointer_unit_the_session_already_holds()
    {
        // The falsifying half: the pointer's receipt must be honoured on the next call,
        // otherwise the cheapest level is also the only one that charges twice.
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { } // widget lattice bundle";
        var service = BuildService((path, body, 4096L));

        var first = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Paths,
            seen: null, known: null, session: "s-paths-reuse", CancellationToken.None);
        var receipt = first.Entries[0].Units[0].Receipt;

        var second = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Paths,
            seen: null, known: null, session: "s-paths-reuse", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.Entries, Is.Empty,
                "Every unit was already held, so the file is fully reused and delivers nothing.");
            Assert.That(second.Reused.Select(r => r.Receipt), Does.Contain(receipt));
            Assert.That(second.TotalTokens, Is.Zero, "Suppressed content is never charged.");
        });
    }

    [Test]
    public async Task Build_at_outline_detail_with_reuse_engaged_falls_back_to_a_pointer_for_a_symbol_less_file()
    {
        // A file that declares no symbols (a config file, a plain data file) has no
        // outline to project. It must degrade to a reusable pointer, not to an entry
        // with empty content.
        const string path = "src/widget.config";
        const string body = "widget lattice bundle configuration values";
        var service = BuildService((path, body, 32L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline,
            seen: null, known: null, session: "s-outline-empty", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            var entry = result.Entries[0];
            Assert.That(entry.Units, Has.Count.EqualTo(1));
            Assert.That(entry.Units[0].Kind, Is.EqualTo(RepoContextReuse.PointerKind),
                "With no symbols there is nothing to outline, so the unit degrades to a pointer.");
            Assert.That(entry.Content, Is.EqualTo(path));
        });
    }

    // --- The outline projection without reuse ---------------------------------------

    [Test]
    public async Task Build_at_outline_detail_falls_back_to_the_path_for_a_symbol_less_file()
    {
        const string path = "src/widget.config";
        const string body = "widget lattice bundle configuration values";
        var service = BuildService((path, body, 32L));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Detail, Is.EqualTo("outline"));
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].Content, Is.EqualTo(path),
                "An empty outline degrades to the path, never to empty content.");
        });
    }

    [Test]
    public async Task Build_at_outline_detail_joins_every_declared_symbol_on_its_own_line()
    {
        // The outline is a skeleton the caller reads: each declared symbol must be on
        // its own line, in declaration order, so it is legible rather than run together.
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget bundle";
        var service = BuildServiceWithSymbols(
            path, body, 4096L,
            ("Acme.Widget", "public sealed class Widget"),
            ("Acme.Widget.Assemble", "public void Assemble()"));

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Outline, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(
                result.Entries[0].Content,
                Is.EqualTo("public sealed class Widget\npublic void Assemble()"));
            Assert.That(result.Entries[0].Content.Split('\n'), Has.Length.EqualTo(2),
                "Two declared symbols must render as two lines, not one concatenated blob.");
        });
    }

    // --- Per-candidate memoisation across an auto degrade ---------------------------

    [Test]
    public async Task Build_at_auto_detail_reads_each_record_once_across_every_level_it_tries()
    {
        // An auto bundle that does not fit at slices retries at outline and then paths.
        // Each level re-renders the same candidates, so without per-candidate memoisation
        // the same content and structural records would be re-read once per level.
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { public void Assemble() { } } // widget lattice bundle";
        var (service, structural, content) = BuildCountingService(path, body, 4096L);

        // A one-token ceiling admits nothing at any level, so all three levels run.
        var result = await service.BuildAsync(
            RepoId, "widget", 10, 1, RepoContextContextDetail.Auto,
            seen: null, known: null, session: "s-auto", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Is.Empty, "Nothing fits a one-token ceiling.");
            Assert.That(content.Reads, Is.EqualTo(1),
                "The body must be read once and reused across every level the auto degrade tries.");

            // Two reads of the file node, not three: the bundle itself reads it once and
            // memoises it across the slices and paths levels, and the graph service's
            // outline pass reads it once on its own account. Without the memoisation the
            // bundle alone would read it twice, for three in total.
            Assert.That(structural.Reads, Is.EqualTo(2),
                "The stored token count must be read once by the bundle and reused across levels.");
        });
    }

    [Test]
    public async Task Build_at_slices_detail_prices_a_full_read_from_the_body_when_no_token_count_is_stored()
    {
        // A file indexed before the token-count projection existed carries no stored
        // count. The full-read cost must then be measured from the body rather than
        // reported as unknown, or the caller cannot decide whether a full read is worth it.
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { } // widget lattice bundle";
        var service = BuildServiceWithoutTokenCount(path, body);

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices,
            seen: null, known: null, session: "s-nocount", CancellationToken.None);

        var stored = StoredBody(path, body);
        Assert.Multiple(() =>
        {
            Assert.That(result.Entries, Has.Count.EqualTo(1));
            Assert.That(result.Entries[0].FullReadTokenCount, Is.EqualTo(Counter.CountTokens(stored)),
                "With no stored count the whole-file cost is measured from the delivered body.");
        });
    }

    // --- A semantic symbol hit resolves to its declaring file -----------------------

    [Test]
    public async Task Build_resolves_a_semantic_symbol_hit_to_the_file_that_declares_it()
    {
        // Vectors are written per symbol as well as per file, so a semantic search can
        // rank a symbol record. A symbol key carries no path of its own - the bundle has
        // to resolve it through the symbol's recorded declaring file, or every
        // symbol-ranked hit would be silently dropped from the bundle.
        const string path = "src/Widget.cs";
        const string body = "namespace Acme; public sealed class Widget { } // widget lattice bundle";
        const string fqn = "Acme.Widget";
        var service = BuildServiceWithSemanticSymbolHit(path, body, fqn, 4096L);

        var result = await service.BuildAsync(
            RepoId, "widget", 10, 10_000, RepoContextContextDetail.Slices, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("semantic"));
            Assert.That(result.Entries, Has.Count.EqualTo(1),
                "The symbol hit must resolve to a deliverable file, not be discarded.");
            Assert.That(result.Entries[0].Path, Is.EqualTo(path));
            Assert.That(result.Entries[0].Content, Is.EqualTo(StoredBody(path, body)));
        });
    }

    // --- Builders -------------------------------------------------------------------

    /// <summary>
    /// A service whose files have a structural node (and optionally declared symbols)
    /// but <b>no</b> content projection - the shape an index carries between the
    /// structural pass and the content back-fill.
    /// </summary>
    private static RepoContextBundleService BuildServiceWithoutContent(
        (string Path, long TokenCount) file,
        (string FullyQualifiedName, string Signature)[]? symbols = null)
    {
        var names = symbols?.Select(s => s.FullyQualifiedName).ToArray() ?? [];
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = file.Path,
            TokenCount = RepoContextValues.Lww(file.TokenCount, Clock(1)),
            DeclaredSymbols = names.Length == 0
                ? new BoundedRegister()
                : RepoContextValues.Lww(DeclaredSymbolNames.Encode(names), Clock(1)),
        };
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.File(RepoId, file.Path)] = Serializer.SerializeToArray(node),
        };

        var symbolEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        for (var i = 0; i < (symbols?.Length ?? 0); i++)
        {
            var record = new SymbolRecord
            {
                RepoId = RepoId,
                FullyQualifiedName = symbols![i].FullyQualifiedName,
                Signature = RepoContextValues.Lww(symbols[i].Signature, Clock(1)),
                StartLine = RepoContextValues.Lww(i + 1L, Clock(1)),
            };
            symbolEntries[RepoContextKeys.Symbol(RepoId, symbols[i].FullyQualifiedName)] =
                Serializer.SerializeToArray(record);
        }

        return Assemble(
            Tree(structuralEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            Tree(symbolEntries));
    }

    /// <summary>
    /// A service whose single file has a body but no stored token count - the shape of a
    /// file indexed before the token-count projection existed.
    /// </summary>
    private static RepoContextBundleService BuildServiceWithoutTokenCount(string path, string body)
    {
        var node = new FileNode { RepoId = RepoId, Path = path };
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.File(RepoId, path)] = Serializer.SerializeToArray(node),
        };
        var contentEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.Content(RepoId, path)] =
                Serializer.SerializeToArray(ContentRecord.Create(RepoId, path, body, Clock(1))),
        };

        return Assemble(
            Tree(structuralEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            Tree(contentEntries),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)));
    }

    /// <summary>Counts the reads a bundle issues against one backing tree.</summary>
    private sealed class ReadCountingTree
    {
        private int _reads;

        public int Reads => Volatile.Read(ref _reads);

        public ILattice Build(IReadOnlyDictionary<string, byte[]> map, string countedKey)
        {
            var tree = Substitute.For<ILattice>();
            var items = map.Select(kv => new KeyValuePair<string, byte[]>(kv.Key, kv.Value)).ToArray();
            tree.EntriesAsync().ReturnsForAnyArgs(_ => Yield(items));
            tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(ci =>
                {
                    var key = ci.ArgAt<string>(0);
                    if (string.Equals(key, countedKey, StringComparison.Ordinal))
                    {
                        Interlocked.Increment(ref _reads);
                    }

                    return Task.FromResult<byte[]?>(map.TryGetValue(key, out var value) ? value : null);
                });
            return tree;
        }
    }

    private static (RepoContextBundleService Service, ReadCountingTree Structural, ReadCountingTree Content)
        BuildCountingService(string path, string body, long tokenCount)
    {
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = path,
            TokenCount = RepoContextValues.Lww(tokenCount, Clock(1)),
        };
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.File(RepoId, path)] = Serializer.SerializeToArray(node),
        };
        var contentEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.Content(RepoId, path)] =
                Serializer.SerializeToArray(ContentRecord.Create(RepoId, path, body, Clock(1))),
        };

        var structural = new ReadCountingTree();
        var content = new ReadCountingTree();
        var service = Assemble(
            structural.Build(structuralEntries, RepoContextKeys.File(RepoId, path)),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)),
            content.Build(contentEntries, RepoContextKeys.Content(RepoId, path)),
            Tree(new Dictionary<string, byte[]>(StringComparer.Ordinal)));

        return (service, structural, content);
    }

    /// <summary>
    /// A service whose semantic index ranks the file's <b>symbol</b> record rather than
    /// the file record, so the bundle has to resolve the hit through the symbol's
    /// declaring file path.
    /// </summary>
    private static RepoContextBundleService BuildServiceWithSemanticSymbolHit(
        string path, string body, string fullyQualifiedName, long storedTokenCount)
    {
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = path,
            TokenCount = RepoContextValues.Lww(storedTokenCount, Clock(1)),
        };
        var structuralEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.File(RepoId, path)] = Serializer.SerializeToArray(node),
        };
        var contentEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [RepoContextKeys.Content(RepoId, path)] =
                Serializer.SerializeToArray(ContentRecord.Create(RepoId, path, body, Clock(1))),
        };
        var symbolKey = RepoContextKeys.Symbol(RepoId, fullyQualifiedName);
        var symbolEntries = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            [symbolKey] = Serializer.SerializeToArray(new SymbolRecord
            {
                RepoId = RepoId,
                FullyQualifiedName = fullyQualifiedName,
                FilePath = RepoContextValues.Lww(path, Clock(1)),
                Signature = RepoContextValues.Lww("public sealed class Widget", Clock(1)),
            }),
        };

        var grainFactory = Substitute.For<IGrainFactory>();

        // Each substitute must be fully configured BEFORE it is handed to another
        // substitute's Returns(...): configuring one inside another's recording window
        // corrupts NSubstitute's pending-call context.
        var structuralTree = HydratingTree(structuralEntries);
        var memoryTree = HydratingTree(new Dictionary<string, byte[]>(StringComparer.Ordinal));
        var contentTree = HydratingTree(contentEntries);
        var symbolTree = HydratingTree(symbolEntries);
        var sessionTree = MutableTree(new Dictionary<string, byte[]>(StringComparer.Ordinal));

        grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural).Returns(structuralTree);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Memory).Returns(memoryTree);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Content).Returns(contentTree);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol).Returns(symbolTree);
        grainFactory.GetGrain<ILattice>(RepoContextTrees.Session).Returns(sessionTree);

        var store = new RepoContextStore(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            new RepoContextVectorWriter(grainFactory, Serializer, Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory)),
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        var search = new RepoContextSearchService(
            grainFactory,
            Serializer,
            SemanticIndexRanking(symbolKey),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            AvailableEmbedder());

        var graph = new RepoContextGraphService(grainFactory, Serializer, Counter, new RepoContextWorkspaceGuard([]));
        var sessions = new RepoContextSessionStore(grainFactory, Serializer);

        return new RepoContextBundleService(
            search, graph, sessions, grainFactory, Serializer, Counter, NoOpUsageRecorder.Instance);
    }

    // A tree that also answers the versioned read the store's recall path uses, so a
    // semantic match actually hydrates into a hit rather than degrading the index.
    private static ILattice HydratingTree(IReadOnlyDictionary<string, byte[]> map)
    {
        var tree = Tree(map);
        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(
                map.TryGetValue(ci.ArgAt<string>(0), out var value)
                    ? new VersionedValue { Value = value }
                    : new VersionedValue()));
        return tree;
    }

    private static readonly EmbeddingSpace SemanticSpace = new("test-model", 3, normalized: true);

    private static IEmbeddingProvider AvailableEmbedder()
    {
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(SemanticSpace);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Success(SemanticSpace, new[] { new ReadOnlyMemory<float>([1f, 0f, 0f]) }));
        return provider;
    }

    private static IRepoContextSemanticIndex SemanticIndexRanking(params string[] sourceKeys)
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);

        var matches = new List<RepoContextVectorMatch>(sourceKeys.Length);
        for (var i = 0; i < sourceKeys.Length; i++)
        {
            matches.Add(new RepoContextVectorMatch($"vec-{i}", sourceKeys[i], 1d - (i * 0.1)));
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
}
