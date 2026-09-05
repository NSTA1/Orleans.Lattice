using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Coverage for the parts of <see cref="RepoContextToolHandlers"/> that sit past
/// argument validation: the wire <c>detail</c> parse, the usage roll-up, the
/// caller-error arms of the onboarding path (a path that resolves outside the
/// workspace, a malformed path, a path that does not exist, and a path no
/// repository id can be derived from), and the <c>changed</c> tool's translation
/// of a workspace violation or a missing directory into a clean
/// <see cref="McpException"/>.
/// <para>
/// These are the arms a wire caller actually reaches with a plausible-looking
/// argument, and each must produce a self-contained message rather than a raw
/// framework exception crossing the MCP boundary.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextToolHandlerDispatchTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly IRepoContextTokenCounter Counter =
        new TiktokenRepoContextTokenCounter(new RepoContextIndexingOptions());

    private readonly List<string> _tempRoots = [];

    [TearDown]
    public void CleanUpTempRoots()
    {
        foreach (var root in _tempRoots)
        {
            try
            {
                if (Directory.Exists(root))
                {
                    Directory.Delete(root, recursive: true);
                }
            }
            catch (IOException)
            {
                // A best-effort cleanup must never fail the test that produced it.
            }
        }

        _tempRoots.Clear();
    }

    private string NewWorkspace()
    {
        var root = Path.Combine(Path.GetTempPath(), "rcth-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _tempRoots.Add(root);
        return root;
    }

    private static ILattice EmptyTree()
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));
        return tree;

        static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static IGrainFactory EmptyTrees()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        foreach (var tree in new[]
                 {
                     RepoContextTrees.Structural, RepoContextTrees.Memory, RepoContextTrees.Content,
                     RepoContextTrees.Symbol, RepoContextTrees.Session,
                 })
        {
            // Hoist the tree substitute before wiring it: NSubstitute's
            // thread-local call context throws if a substitute is built and
            // configured INSIDE another substitute's Returns(...).
            var empty = EmptyTree();
            grainFactory.GetGrain<ILattice>(tree).Returns(empty);
        }

        return grainFactory;
    }

    private static RepoContextBundleService BundleService(IGrainFactory grainFactory)
    {
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

        var search = new RepoContextSearchService(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance,
            embeddingProvider: null);

        return new RepoContextBundleService(
            search,
            new RepoContextGraphService(grainFactory, Serializer, Counter, new RepoContextWorkspaceGuard([])),
            new RepoContextSessionStore(grainFactory, Serializer),
            grainFactory,
            Serializer,
            Counter,
            NoOpUsageRecorder.Instance);
    }

    private static Task<ModelContextProtocol.Server.RequestContext<ModelContextProtocol.Protocol.CallToolRequestParams>>
        ContextWith(Action<ServiceCollection> configure)
    {
        var services = new ServiceCollection();
        configure(services);
        return RepoContextRequestContexts.CreateAsync(services.BuildServiceProvider());
    }

    // ---- detail parsing ---------------------------------------------------

    [TestCase("paths", "paths")]
    [TestCase("outline", "outline")]
    [TestCase("slices", "slices")]
    [TestCase("PATHS", "paths")]
    [TestCase("  Outline  ", "outline")]
    [TestCase("SLICES", "slices")]
    public async Task ContextAsync_honours_an_explicit_detail_level(string wire, string expected)
    {
        var grainFactory = EmptyTrees();
        var context = await ContextWith(s => s.AddSingleton(BundleService(grainFactory)));

        var result = await RepoContextToolHandlers.ContextAsync(context, "acme", "where is the widget", detail: wire);

        Assert.That(result.Detail, Is.EqualTo(expected),
            "an explicit detail level must survive case and surrounding whitespace");
    }

    [TestCase(null)]
    [TestCase("auto")]
    [TestCase("AUTO")]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("not-a-level")]
    [TestCase("Paths,Outline")]
    public async Task ContextAsync_treats_an_absent_or_unrecognised_detail_level_as_auto(string? wire)
    {
        // The documented contract: a wire caller can never fault the tool with a
        // bad level - anything unrecognised degrades to auto, whose floor is
        // paths.
        var grainFactory = EmptyTrees();
        var context = await ContextWith(s => s.AddSingleton(BundleService(grainFactory)));

        var result = await RepoContextToolHandlers.ContextAsync(context, "acme", "where is the widget", detail: wire);

        Assert.That(result.Detail, Is.EqualTo("paths"));
    }

    // ---- usage roll-up ----------------------------------------------------

    [Test]
    public async Task Stats_reports_the_recorder_aggregate_and_its_window()
    {
        var recorder = new CapturingUsageRecorder();
        recorder.Record(new RepoContextCallUsage { ResponseTokens = 120, ReplacedReadTokens = 900 });
        recorder.Record(new RepoContextCallUsage { ResponseTokens = 80, ReplacedReadTokens = 100 });
        var context = await ContextWith(s => s.AddSingleton<IRepoContextUsageRecorder>(recorder));

        var stats = RepoContextToolHandlers.Stats(context);

        Assert.Multiple(() =>
        {
            Assert.That(stats.Calls, Is.EqualTo(2));
            Assert.That(stats.ResponseTokens, Is.EqualTo(200));
            Assert.That(stats.ReadsReplacedTokens, Is.EqualTo(1000));
            Assert.That(stats.NetSavedTokens, Is.EqualTo(800), "net saved is replaced minus spent");
            Assert.That(stats.WindowSeconds, Is.EqualTo((long)recorder.Window.TotalSeconds));
        });
    }

    [Test]
    public async Task Stats_reports_a_negative_net_when_discovery_outweighs_replaced_reads()
    {
        // A discovery-heavy window legitimately nets negative; the roll-up must
        // report the signed figure rather than clamping it to zero.
        var recorder = new CapturingUsageRecorder();
        recorder.Record(new RepoContextCallUsage { ResponseTokens = 500, ReplacedReadTokens = 0 });
        var context = await ContextWith(s => s.AddSingleton<IRepoContextUsageRecorder>(recorder));

        var stats = RepoContextToolHandlers.Stats(context);

        Assert.That(stats.NetSavedTokens, Is.EqualTo(-500));
    }

    // ---- onboarding caller errors ----------------------------------------

    [TestCase("/")]
    [TestCase("\\")]
    [TestCase("///")]
    public void AddRepoAsync_rejects_a_path_no_repository_id_can_be_derived_from(string path)
    {
        // A bare root has no final segment to name the repository after, so the
        // caller must be told to supply repoId rather than have records filed
        // under an empty id.
        Assert.That(
            () => RepoContextToolHandlers.AddRepoAsync(null!, path),
            Throws.InstanceOf<McpException>().With.Message.Contains("'repoId'"));
    }

    [Test]
    public async Task AddRepoAsync_derives_the_repository_id_from_the_final_path_segment()
    {
        // The counterpart guarantee for the derivation helper: an ordinary path
        // does yield an id, and the onboarding proceeds far enough to fail on
        // the (absent) directory rather than on the id.
        var workspace = NewWorkspace();
        var missing = Path.Combine(workspace, "not-created");
        var context = await ContextWith(s => s.AddSingleton(new RepoContextWorkspaceGuard([])));

        Assert.That(
            () => RepoContextToolHandlers.AddRepoAsync(context, missing),
            Throws.InstanceOf<McpException>().With.Message.Contains("does not exist"),
            "a derivable id must let the call reach the directory check");
    }

    [Test]
    public async Task BootstrapAsync_rejects_a_root_that_does_not_exist()
    {
        var workspace = NewWorkspace();
        var missing = Path.Combine(workspace, "nope");
        var context = await ContextWith(s => s.AddSingleton(new RepoContextWorkspaceGuard([])));

        Assert.That(
            () => RepoContextToolHandlers.BootstrapAsync(context, missing, "acme"),
            Throws.InstanceOf<McpException>()
                .With.Message.Contains("does not exist or is not a directory"));
    }

    [Test]
    public async Task BootstrapAsync_rejects_a_root_outside_the_mounted_workspace()
    {
        // Fail-closed: the guard refuses, and the refusal must surface as a
        // caller-facing McpException rather than an unhandled violation.
        var workspace = NewWorkspace();
        var outside = NewWorkspace();
        var context = await ContextWith(s => s.AddSingleton(new RepoContextWorkspaceGuard([workspace])));

        Assert.That(
            () => RepoContextToolHandlers.BootstrapAsync(context, outside, "acme"),
            Throws.InstanceOf<McpException>().With.Message.Contains("outside the mounted workspace"));
    }

    [Test]
    public async Task BootstrapAsync_rejects_a_malformed_path()
    {
        // A path the platform cannot even normalise (an embedded NUL) reaches
        // the guard as an ArgumentException, which must be translated rather
        // than escaping as a framework exception.
        var context = await ContextWith(s => s.AddSingleton(new RepoContextWorkspaceGuard([])));

        Assert.That(
            () => RepoContextToolHandlers.BootstrapAsync(context, "/workspace/a\0b", "acme"),
            Throws.InstanceOf<McpException>());
    }

    // ---- changed: caller-error translation -------------------------------

    [Test]
    public async Task ChangedAsync_translates_a_workspace_violation_into_an_mcp_error()
    {
        var workspace = NewWorkspace();
        var outside = NewWorkspace();
        var graph = new RepoContextGraphService(
            EmptyTrees(), Serializer, Counter, new RepoContextWorkspaceGuard([workspace]));
        var context = await ContextWith(s => s.AddSingleton(graph));

        Assert.That(
            () => RepoContextToolHandlers.ChangedAsync(context, "acme", outside),
            Throws.InstanceOf<McpException>().With.Message.Contains("outside the mounted workspace"));
    }

    [Test]
    public async Task ChangedAsync_translates_a_missing_directory_into_an_mcp_error()
    {
        var workspace = NewWorkspace();
        var missing = Path.Combine(workspace, "gone");
        var grainFactory = EmptyTrees();
        // No durable index request, so the walk falls back to the supplied path.
        var jobGrain = JobGrainWithNoRequest();
        grainFactory.GetGrain<IRepoIndexJobGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(jobGrain);
        var graph = new RepoContextGraphService(
            grainFactory, Serializer, Counter, new RepoContextWorkspaceGuard([]));
        var context = await ContextWith(s => s.AddSingleton(graph));

        Assert.That(
            () => RepoContextToolHandlers.ChangedAsync(context, "acme", missing),
            Throws.InstanceOf<McpException>(),
            "a path that no longer exists is a caller error, not an unhandled walk failure");
    }

    private static IRepoIndexJobGrain JobGrainWithNoRequest()
    {
        var grain = Substitute.For<IRepoIndexJobGrain>();
        grain.GetRequestAsync().Returns(Task.FromResult<RepoIndexJobRequest?>(null));
        return grain;
    }
}
