using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Graph;

/// <summary>
/// Unit coverage for <see cref="RepoContextGraphService"/>, the read-only adapter
/// behind <c>repocontext_outline</c>, <c>repocontext_changed</c>, and
/// <c>repocontext_related</c>, driven against substituted context trees so the
/// projections are exercised without a silo.
/// <para>
/// The neighbourhood resolution is the part worth pinning down. Its edges are
/// keyed by <b>simple</b> (unqualified) type-name, a syntactic approximation, and
/// it deliberately excludes a file from its own dependent set - so the tests below
/// assert what is and is not an edge rather than only that edges exist.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextGraphServiceTests
{
    private const string RepoId = "acme";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private readonly List<string> _tempRoots = [];

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

    private static HybridLogicalClock Clock(long ticks = 1) => new() { WallClockTicks = ticks, Counter = 0 };

    /// <summary>
    /// The four context trees the graph service reads, each a substituted
    /// <see cref="ILattice"/> over an ordinal-ordered dictionary, behind one
    /// substituted grain factory.
    /// </summary>
    private sealed class Trees
    {
        private readonly Dictionary<string, SortedDictionary<string, byte[]>> _byTree = new(StringComparer.Ordinal);

        public Trees()
        {
            GrainFactory = Substitute.For<IGrainFactory>();
            GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call => Tree(call.ArgAt<string>(0)));

            JobGrain = Substitute.For<IRepoIndexJobGrain>();
            JobGrain.GetRequestAsync().Returns(Task.FromResult<RepoIndexJobRequest?>(null));
            GrainFactory.GetGrain<IRepoIndexJobGrain>(Arg.Any<string>()).Returns(JobGrain);
        }

        public IGrainFactory GrainFactory { get; }

        public IRepoIndexJobGrain JobGrain { get; }

        public void Put(string treeName, string key, byte[] value) => Records(treeName)[key] = value;

        private SortedDictionary<string, byte[]> Records(string treeName)
        {
            if (!_byTree.TryGetValue(treeName, out var records))
            {
                records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
                _byTree[treeName] = records;
                _trees[treeName] = Build(records);
            }

            return records;
        }

        private readonly Dictionary<string, ILattice> _trees = new(StringComparer.Ordinal);

        private ILattice Tree(string treeName)
        {
            Records(treeName);
            return _trees[treeName];
        }

        private static ILattice Build(SortedDictionary<string, byte[]> records)
        {
            var tree = Substitute.For<ILattice>();

            tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .ReturnsForAnyArgs(call =>
                    Task.FromResult(records.TryGetValue(call.ArgAt<string>(0), out var value) ? value : null));

            tree.EntriesAsync().ReturnsForAnyArgs(call => Entries(
                records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));

            tree.KeysAsync().ReturnsForAnyArgs(call => Keys(
                records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));

            return tree;
        }

        private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
            SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
        {
            foreach (var entry in Window(records, startInclusive, endExclusive))
            {
                yield return entry;
                await Task.CompletedTask.ConfigureAwait(false);
            }
        }

        private static async IAsyncEnumerable<string> Keys(
            SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
        {
            foreach (var entry in Window(records, startInclusive, endExclusive))
            {
                yield return entry.Key;
                await Task.CompletedTask.ConfigureAwait(false);
            }
        }

        private static List<KeyValuePair<string, byte[]>> Window(
            SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
        {
            var window = new List<KeyValuePair<string, byte[]>>();
            foreach (var entry in records)
            {
                if (startInclusive is not null && string.CompareOrdinal(entry.Key, startInclusive) < 0)
                {
                    continue;
                }

                if (endExclusive is not null && string.CompareOrdinal(entry.Key, endExclusive) >= 0)
                {
                    break;
                }

                window.Add(entry);
            }

            return window;
        }
    }

    private static void PutFile(
        Trees trees,
        string path,
        string digest,
        IEnumerable<string>? declaredSymbols = null,
        long? tokenCount = null)
    {
        var node = new FileNode
        {
            RepoId = RepoId,
            Path = path,
            Digest = RepoContextValues.Lww(digest, Clock()),
            Language = RepoContextValues.Lww("csharp", Clock()),
            SizeBytes = RepoContextValues.Lww(128L, Clock()),
            DeclaredSymbols = RepoContextValues.Lww(
                DeclaredSymbolNames.Encode(declaredSymbols ?? []), Clock()),
            TokenCount = tokenCount is { } value
                ? RepoContextValues.Lww(value, Clock())
                : new BoundedRegister(),
        };

        trees.Put(RepoContextTrees.Structural, RepoContextKeys.File(RepoId, path), Serializer.SerializeToArray(node));
    }

    private static void PutSymbol(
        Trees trees,
        string fqName,
        string? filePath,
        IEnumerable<string>? references = null,
        IEnumerable<string>? declaringFiles = null)
    {
        var record = new SymbolRecord
        {
            RepoId = RepoId,
            FullyQualifiedName = fqName,
            Kind = SymbolKind.Type,
            FilePath = filePath is null ? new BoundedRegister() : RepoContextValues.Lww(filePath, Clock()),
            StartLine = RepoContextValues.Lww(3L, Clock()),
            EndLine = RepoContextValues.Lww(9L, Clock()),
            Signature = RepoContextValues.Lww($"public class {fqName}", Clock()),
        };

        var replica = 0;
        foreach (var reference in references ?? [])
        {
            record.References.Add(Encoding.UTF8.GetBytes(reference), "a", replica++);
        }

        foreach (var declaring in declaringFiles ?? [])
        {
            record.DeclaringFiles.Add(Encoding.UTF8.GetBytes(declaring), "a", replica++);
        }

        trees.Put(RepoContextTrees.Symbol, RepoContextKeys.Symbol(RepoId, fqName), Serializer.SerializeToArray(record));
    }

    private static void PutCrossReference(
        Trees trees, string simpleName, IEnumerable<string>? referrers = null, IEnumerable<string>? tests = null)
    {
        var node = new CrossReferenceNode { RepoId = RepoId, Name = simpleName };
        var replica = 0;
        foreach (var referrer in referrers ?? [])
        {
            node.Referrers.Add(Encoding.UTF8.GetBytes(referrer), "a", replica++);
        }

        foreach (var test in tests ?? [])
        {
            node.Tests.Add(Encoding.UTF8.GetBytes(test), "a", replica++);
        }

        trees.Put(
            RepoContextTrees.CrossReference,
            RepoContextKeys.CrossReference(RepoId, simpleName),
            Serializer.SerializeToArray(node));
    }

    private static void PutContent(Trees trees, string path, string text)
        => trees.Put(
            RepoContextTrees.Content,
            RepoContextKeys.Content(RepoId, path),
            Serializer.SerializeToArray(ContentRecord.Create(RepoId, path, text, Clock())));

    private static RepoContextGraphService Service(
        Trees trees, IRepoContextTokenCounter? counter = null, params string[] allowedRoots)
        => new(
            trees.GrainFactory,
            Serializer,
            counter ?? TokenCounter(42),
            new RepoContextWorkspaceGuard(allowedRoots));

    private static IRepoContextTokenCounter TokenCounter(int result)
    {
        var counter = Substitute.For<IRepoContextTokenCounter>();
        counter.CountTokens(Arg.Any<string>()).Returns(result);
        return counter;
    }

    private string NewWorkspace()
    {
        var root = Path.Combine(Path.GetTempPath(), "rcgs-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    [Test]
    public void Rejects_its_null_arguments()
    {
        var trees = new Trees();
        var guard = new RepoContextWorkspaceGuard([]);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextGraphService(null!, Serializer, TokenCounter(1), guard),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextGraphService(trees.GrainFactory, null!, TokenCounter(1), guard),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextGraphService(trees.GrainFactory, Serializer, null!, guard),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextGraphService(trees.GrainFactory, Serializer, TokenCounter(1), null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_read_only_projections_reject_their_null_arguments()
    {
        var service = Service(new Trees());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await service.OutlineAsync(null!, "a.cs", Ct), Throws.ArgumentNullException);
            Assert.That(async () => await service.OutlineAsync(RepoId, null!, Ct), Throws.ArgumentNullException);
            Assert.That(async () => await service.RelatedAsync(null!, "a.cs", Ct), Throws.ArgumentNullException);
            Assert.That(async () => await service.RelatedAsync(RepoId, null!, Ct), Throws.ArgumentNullException);
            Assert.That(async () => await service.ChangedAsync(null!, "x", Ct), Throws.ArgumentNullException);
            Assert.That(async () => await service.ChangedAsync(RepoId, null!, Ct), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Related_reports_a_file_with_no_stored_node_as_absent()
    {
        var trees = new Trees();
        PutFile(trees, "src/Present.cs", "d1");

        var result = await Service(trees).RelatedAsync(RepoId, "src/Absent.cs", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Exists, Is.False, "an absent file must be distinguishable from an empty one");
            Assert.That(result.Path, Is.EqualTo("src/Absent.cs"));
            Assert.That(result.Imports, Is.Empty);
            Assert.That(result.Dependents, Is.Empty);
            Assert.That(result.Tests, Is.Empty);
        });
    }

    [Test]
    public async Task Related_reports_outbound_imports_inbound_dependents_and_tests()
    {
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B"]);
        PutSymbol(trees, "N.B", "src/B.cs", references: ["Widget", "Gadget"]);
        PutSymbol(trees, "N.A", "src/A.cs");
        PutSymbol(trees, "N.Z", "src/Z.cs");
        PutSymbol(trees, "N.BTests", "test/BTests.cs");
        PutCrossReference(trees, "B", referrers: ["N.Z", "N.A"], tests: ["N.BTests"]);

        var result = await Service(trees).RelatedAsync(RepoId, "src/B.cs", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Exists, Is.True);
            Assert.That(result.Imports, Is.EqualTo(new[] { "Gadget", "Widget" }),
                "outbound imports are the type-names the file's own symbols reference, ordered");
            Assert.That(
                result.Dependents.Select(static d => d.Symbol), Is.EqualTo(new[] { "N.A", "N.Z" }),
                "dependents are ordered by declaring path then symbol, so the answer is stable");
            Assert.That(result.Dependents.Select(static d => d.Path), Is.EqualTo(new[] { "src/A.cs", "src/Z.cs" }));
            Assert.That(result.Tests.Select(static t => t.Symbol), Is.EqualTo(new[] { "N.BTests" }));
            Assert.That(result.Tests.Select(static t => t.Path), Is.EqualTo(new[] { "test/BTests.cs" }));
        });
    }

    [Test]
    public async Task Related_excludes_the_file_from_its_own_dependent_set()
    {
        // A second declaration in the same file - a partial or nested type - is not
        // an inbound dependency of the file on itself.
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B", "N.BHelper"]);
        PutSymbol(trees, "N.B", "src/B.cs");
        PutSymbol(trees, "N.BHelper", "src/B.cs");
        PutSymbol(trees, "N.Other", "src/Other.cs");
        PutCrossReference(trees, "B", referrers: ["N.B", "N.BHelper", "N.Other"]);
        PutCrossReference(trees, "BHelper", referrers: ["N.BHelper"]);

        var result = await Service(trees).RelatedAsync(RepoId, "src/B.cs", Ct);

        Assert.That(
            result.Dependents.Select(static d => d.Symbol), Is.EqualTo(new[] { "N.Other" }),
            "a file may not depend on itself, whether through its own declarations or a sibling in the same file");
    }

    [Test]
    public async Task Related_resolves_a_declaring_file_from_the_declaring_file_set_when_no_path_is_recorded()
    {
        // Older records carry the declaring file only in the multi-valued set, so a
        // dependent must still resolve to a path rather than being reported unplaced.
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B"]);
        PutSymbol(trees, "N.B", "src/B.cs");
        PutSymbol(trees, "N.Legacy", filePath: null, declaringFiles: ["src/Legacy.cs"]);
        PutCrossReference(trees, "B", referrers: ["N.Legacy"]);

        var result = await Service(trees).RelatedAsync(RepoId, "src/B.cs", Ct);

        Assert.That(result.Dependents.Single().Path, Is.EqualTo("src/Legacy.cs"));
    }

    [Test]
    public async Task Related_reports_an_unresolvable_referrer_without_a_path()
    {
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B"]);
        PutSymbol(trees, "N.B", "src/B.cs");
        PutCrossReference(trees, "B", referrers: ["N.Unknown"]);

        var result = await Service(trees).RelatedAsync(RepoId, "src/B.cs", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Dependents.Single().Symbol, Is.EqualTo("N.Unknown"));
            Assert.That(result.Dependents.Single().Path, Is.Null,
                "an edge whose declaring symbol is not indexed is still an edge, just an unplaced one");
        });
    }

    [Test]
    public async Task Outline_falls_back_to_the_content_projection_when_no_token_count_is_indexed()
    {
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B"], tokenCount: null);
        PutSymbol(trees, "N.B", "src/B.cs");
        PutContent(trees, "src/B.cs", "public class B { }");

        var result = await Service(trees, TokenCounter(7)).OutlineAsync(RepoId, "src/B.cs", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Exists, Is.True);
            Assert.That(result.FullReadTokenCount, Is.EqualTo(7),
                "the stored content projection is the fallback for a file with no indexed token count");
            Assert.That(result.Symbols.Single().FullyQualifiedName, Is.EqualTo("N.B"));
        });
    }

    [Test]
    public async Task Outline_reports_an_unknown_token_cost_for_a_file_never_content_processed()
    {
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B"], tokenCount: null);
        PutSymbol(trees, "N.B", "src/B.cs");

        var result = await Service(trees).OutlineAsync(RepoId, "src/B.cs", Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Exists, Is.True);
            Assert.That(result.FullReadTokenCount, Is.Null,
                "null is 'unknown', which a caller must be able to tell from a genuine zero");
        });
    }

    [Test]
    public async Task Outline_prefers_the_indexed_token_count_over_the_content_projection()
    {
        var trees = new Trees();
        PutFile(trees, "src/B.cs", "d-b", ["N.B"], tokenCount: 1234L);
        PutSymbol(trees, "N.B", "src/B.cs");
        PutContent(trees, "src/B.cs", "public class B { }");

        var result = await Service(trees, TokenCounter(7)).OutlineAsync(RepoId, "src/B.cs", Ct);

        Assert.That(result.FullReadTokenCount, Is.EqualTo(1234),
            "the indexed count was computed from the full body; the projection is bounded and would under-report");
    }

    [Test]
    public async Task Changed_reports_the_dependents_of_what_actually_moved()
    {
        // The blast radius: the indexed files that reference the changed ones, so a
        // review can be scoped to what a set of edits can actually affect.
        var workspace = NewWorkspace();
        Directory.CreateDirectory(Path.Combine(workspace, "src"));
        File.WriteAllText(Path.Combine(workspace, "src", "A.cs"), "namespace N; public class A { }");

        var trees = new Trees();
        // A.cs is stored with a digest that cannot match the file on disk, so it drifts.
        PutFile(trees, "src/A.cs", "stale-digest", ["N.A"]);
        // Two distinct referrers, so the dependent ordering comparer is exercised.
        PutSymbol(trees, "N.Consumer", "src/Consumer.cs");
        PutSymbol(trees, "N.Other", "src/Other.cs");
        PutCrossReference(trees, "A", referrers: ["N.Consumer", "N.Other"]);

        var result = await Service(trees, TokenCounter(1), workspace).ChangedAsync(RepoId, workspace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Updated, Does.Contain("src/A.cs"),
                "drift is detected by content digest, without invoking git");
            Assert.That(result.Dependents, Is.EqualTo(new[] { "src/Consumer.cs", "src/Other.cs" }),
                "the reverse cross-reference projection gives the impact set, ordered");
        });
    }

    [Test]
    public async Task Changed_excludes_a_dependent_that_is_itself_part_of_the_change_set()
    {
        // A file already in the change set is not additionally reported as impacted
        // by it; the dependent list is what a reviewer has NOT already been handed.
        var workspace = NewWorkspace();
        Directory.CreateDirectory(Path.Combine(workspace, "src"));
        File.WriteAllText(Path.Combine(workspace, "src", "A.cs"), "namespace N; public class A { }");
        File.WriteAllText(Path.Combine(workspace, "src", "Consumer.cs"), "namespace N; public class Consumer { }");

        var trees = new Trees();
        PutFile(trees, "src/A.cs", "stale-digest", ["N.A"]);
        PutFile(trees, "src/Consumer.cs", "stale-digest-2", ["N.Consumer"]);
        PutSymbol(trees, "N.Consumer", "src/Consumer.cs");
        PutCrossReference(trees, "A", referrers: ["N.Consumer"]);

        var result = await Service(trees, TokenCounter(1), workspace).ChangedAsync(RepoId, workspace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Updated, Is.EqualTo(new[] { "src/A.cs", "src/Consumer.cs" }));
            Assert.That(result.Dependents, Is.Empty);
        });
    }

    [Test]
    public async Task Changed_reports_a_stored_file_missing_from_disk_as_removed()
    {
        var workspace = NewWorkspace();
        Directory.CreateDirectory(Path.Combine(workspace, "src"));
        File.WriteAllText(Path.Combine(workspace, "src", "Kept.cs"), "namespace N; public class Kept { }");

        var trees = new Trees();
        PutFile(trees, "src/Gone.cs", "d-gone", ["N.Gone"]);
        PutSymbol(trees, "N.Consumer", "src/Consumer.cs");
        PutCrossReference(trees, "Gone", referrers: ["N.Consumer"]);

        var result = await Service(trees, TokenCounter(1), workspace).ChangedAsync(RepoId, workspace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Removed, Does.Contain("src/Gone.cs"));
            Assert.That(result.Added, Does.Contain("src/Kept.cs"));
            Assert.That(result.Dependents, Is.EqualTo(new[] { "src/Consumer.cs" }),
                "a deleted file's dependents are exactly what a removal breaks, so they must be reported too");
        });
    }

    [Test]
    public async Task Changed_resolves_a_referrer_shared_by_two_changed_symbols_once()
    {
        // The declaring-file lookup is cached per referrer across the whole walk: a
        // consumer that references two changed types must resolve to a file once,
        // not once per name, or a wide change set re-reads the same symbol records.
        var workspace = NewWorkspace();
        Directory.CreateDirectory(Path.Combine(workspace, "src"));
        File.WriteAllText(Path.Combine(workspace, "src", "Pair.cs"), "namespace N; public class A { } public class B { }");

        var trees = new Trees();
        PutFile(trees, "src/Pair.cs", "stale-digest", ["N.A", "N.B"]);
        PutSymbol(trees, "N.Consumer", "src/Consumer.cs");
        PutCrossReference(trees, "A", referrers: ["N.Consumer"]);
        PutCrossReference(trees, "B", referrers: ["N.Consumer"]);

        var result = await Service(trees, TokenCounter(1), workspace).ChangedAsync(RepoId, workspace, Ct);

        Assert.That(result.Dependents, Is.EqualTo(new[] { "src/Consumer.cs" }),
            "one impacted file, reported once, however many of its references changed");
    }

    [Test]
    public async Task Changed_reports_no_dependents_when_nothing_indexed_references_the_change()
    {
        var workspace = NewWorkspace();
        Directory.CreateDirectory(Path.Combine(workspace, "src"));
        File.WriteAllText(Path.Combine(workspace, "src", "A.cs"), "namespace N; public class A { }");

        var trees = new Trees();
        PutFile(trees, "src/A.cs", "stale-digest", ["N.A"]);

        var result = await Service(trees, TokenCounter(1), workspace).ChangedAsync(RepoId, workspace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Updated, Does.Contain("src/A.cs"));
            Assert.That(result.Dependents, Is.Empty);
        });
    }

    [Test]
    public async Task Changed_declares_no_dependents_for_a_change_set_that_declares_no_symbols()
    {
        var workspace = NewWorkspace();
        File.WriteAllText(Path.Combine(workspace, "notes.txt"), "plain text");

        var trees = new Trees();
        PutFile(trees, "notes.txt", "stale-digest");

        var result = await Service(trees, TokenCounter(1), workspace).ChangedAsync(RepoId, workspace, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Updated, Does.Contain("notes.txt"));
            Assert.That(result.Dependents, Is.Empty,
                "no declared symbol means no simple name to resolve, so the reverse index is never consulted");
        });
    }
}
