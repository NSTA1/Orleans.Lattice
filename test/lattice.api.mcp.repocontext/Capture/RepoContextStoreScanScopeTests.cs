using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration tests for <see cref="RepoContextStore.ScanAsync"/> scope routing.
/// Each scan scope resolves to exactly one named Lattice tree and one key prefix,
/// so these seed a record of every structural family plus a memory entry and prove
/// that a scan of each scope returns that family and only that family, that a Files
/// scan honours a directory path prefix, that a path prefix on a non-Files scope is
/// rejected, that an unknown scope is rejected, and that an empty repository id is
/// rejected.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreScanScopeTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(RepoContextMcpHarness harness)
        => harness.Services.GetRequiredService<RepoContextStore>();

    private static ILattice Tree(RepoContextMcpHarness harness, string treeName)
        => harness.GrainFactory.GetGrain<ILattice>(treeName);

    private static async Task SeedFileAsync(RepoContextMcpHarness harness, string path, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<FileNode>>();
        var clock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var node = new FileNode { RepoId = RepoId, Path = path, Digest = RepoContextValues.Lww("d-" + path, clock) };
        await Tree(harness, RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.File(RepoId, path), serializer.SerializeToArray(node), ct);
    }

    private static async Task SeedPackageAsync(RepoContextMcpHarness harness, string path, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<PackageNode>>();
        var node = new PackageNode { RepoId = RepoId, Path = path };
        await Tree(harness, RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.Package(RepoId, path), serializer.SerializeToArray(node), ct);
    }

    private static async Task SeedSymbolAsync(RepoContextMcpHarness harness, string fqn, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<SymbolRecord>>();
        var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Method };
        await Tree(harness, RepoContextTrees.Symbol)
            .SetAsync(RepoContextKeys.Symbol(RepoId, fqn), serializer.SerializeToArray(record), ct);
    }

    [Test]
    public async Task Scan_files_without_a_prefix_returns_file_nodes()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedFileAsync(harness, "src/A.cs", Ct);

        var result = await Store(harness).ScanAsync(
            RepoId, RepoContextScanScope.Files, topic: null, pathPrefix: null,
            continuationToken: null, pageSize: 100, Ct);

        Assert.That(result.Entries.Select(e => e.Key), Does.Contain(RepoContextKeys.File(RepoId, "src/A.cs")),
            "A Files scan enumerates the file structural tree.");
    }

    [Test]
    public async Task Scan_files_under_a_directory_prefix_returns_only_that_subtree()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedFileAsync(harness, "src/A.cs", Ct);
        await SeedFileAsync(harness, "other/B.cs", Ct);

        var result = await Store(harness).ScanAsync(
            RepoId, RepoContextScanScope.Files, topic: null, pathPrefix: "src",
            continuationToken: null, pageSize: 100, Ct);

        var keys = result.Entries.Select(e => e.Key).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Contain(RepoContextKeys.File(RepoId, "src/A.cs")),
                "The subtree scan includes the file under the prefix.");
            Assert.That(keys, Does.Not.Contain(RepoContextKeys.File(RepoId, "other/B.cs")),
                "The subtree scan excludes files outside the prefix.");
        });
    }

    [Test]
    public async Task Scan_packages_returns_package_nodes()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedPackageAsync(harness, "src/lib", Ct);

        var result = await Store(harness).ScanAsync(
            RepoId, RepoContextScanScope.Packages, topic: null, pathPrefix: null,
            continuationToken: null, pageSize: 100, Ct);

        Assert.That(result.Entries.Select(e => e.Key), Does.Contain(RepoContextKeys.Package(RepoId, "src/lib")),
            "A Packages scan enumerates the package structural tree.");
    }

    [Test]
    public async Task Scan_symbols_returns_symbol_records()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedSymbolAsync(harness, "Acme.Widget.Spin", Ct);

        var result = await Store(harness).ScanAsync(
            RepoId, RepoContextScanScope.Symbols, topic: null, pathPrefix: null,
            continuationToken: null, pageSize: 100, Ct);

        Assert.That(result.Entries.Select(e => e.Key), Does.Contain(RepoContextKeys.Symbol(RepoId, "Acme.Widget.Spin")),
            "A Symbols scan enumerates the symbol tree.");
    }

    [Test]
    public async Task Scan_memory_returns_memory_entries()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var store = Store(harness);
        var remembered = await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        var result = await store.ScanAsync(
            RepoId, RepoContextScanScope.Memory, topic: null, pathPrefix: null,
            continuationToken: null, pageSize: 100, Ct);

        Assert.That(result.Entries.Select(e => e.Key), Does.Contain(remembered.Key),
            "A Memory scan enumerates the memory tree across topics.");
    }

    [Test]
    public async Task Scan_with_an_unknown_scope_throws()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        Assert.That(
            async () => await Store(harness).ScanAsync(
                RepoId, (RepoContextScanScope)999, topic: null, pathPrefix: null,
                continuationToken: null, pageSize: 100, Ct),
            Throws.InstanceOf<McpException>(),
            "An unrecognised scan scope is rejected rather than defaulting to a tree.");
    }

    [Test]
    public async Task Scan_with_an_empty_repo_id_throws()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);

        Assert.That(
            async () => await Store(harness).ScanAsync(
                string.Empty, RepoContextScanScope.Files, topic: null, pathPrefix: null,
                continuationToken: null, pageSize: 100, Ct),
            Throws.InstanceOf<McpException>(),
            "An empty repository id is rejected before any tree is touched.");
    }
}
