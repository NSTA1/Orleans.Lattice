using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// Shared harness for the app bridge tests: a settable registry projection and a real
/// <see cref="AppRoleGrantEvaluator"/> over an in-image test source, a settable caller, and an in-memory data
/// path behind a substitute grain factory that records every tree it dials.
/// </summary>
/// <remarks>
/// The installed manifest declares a structural tree <c>notes</c>, an adopted tree <c>legacy</c>
/// (<c>legacy-notes</c>) and a structural tree <c>archive</c> no role reaches. Its roles are <c>viewer</c>
/// (read on notes and legacy), <c>editor</c> (read, write and delete on notes) and <c>drafter</c> (read and
/// write under the prefix <c>drafts/</c> and on the key <c>pinned</c> of notes), bound to <c>g-viewers</c>,
/// <c>g-editors</c> and <c>g-drafters</c>.
/// </remarks>
internal sealed class BridgeHarness
{
    public const string Slug = AppsControlHarness.Slug;
    public const long Revision = 7;
    public const string Viewers = "g-viewers";
    public const string Editors = "g-editors";
    public const string Drafters = "g-drafters";
    public const string NotesTree = "a/crm/notes";
    public const string AdoptedTree = "legacy-notes";

    public BridgeHarness(AppManifest? manifest = null)
    {
        Manifest = manifest ?? DefaultManifest();
        Source = new TestCatalogSource(InImageAppSource.SourceKey).Publish(Manifest);
        Sources = new AppSourceSet([Source]);
        Grains.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(call => Tree(call.ArgAt<string>(0)));
    }

    public AppManifest Manifest { get; }

    public TestCatalogSource Source { get; }

    public AppSourceSet Sources { get; }

    public WorkspaceHarness.SettableProjection Projection { get; } = new();

    public WorkspaceHarness.GrantingGate Gate { get; } = new();

    public ConfigurableTenantResolver Tenants { get; } = new();

    public ILatticeMembershipContext? Membership { get; set; } = new SettableMembership();

    public IGrainFactory Grains { get; } = Substitute.For<IGrainFactory>();

    public ManualTime Time { get; } = new();

    public LatticeAppBridgeOptions Options { get; } = new() { RateLimitPermitLimit = 1000 };

    /// <summary>The effective tree ids the data path dialled, in order.</summary>
    public List<string> Dialled { get; } = [];

    /// <summary>The in-memory data behind each effective tree id.</summary>
    public Dictionary<string, SortedDictionary<string, byte[]>> Data { get; } = new(StringComparer.Ordinal);

    /// <summary>A fault the data path throws on every call, or null.</summary>
    public Exception? DataFault { get; set; }

    public LatticeAppBridge Bridge => Create();

    public LatticeAppBridge Create(
        bool withGrains = true,
        bool withTenants = true,
        bool withGate = true,
        bool withProjection = true,
        bool withSource = true,
        AppBridgeRateLimiter? limiter = null) =>
        new(
            new AppRoleGrantEvaluator(withProjection ? Projection : null, withSource ? Sources : null, withGate ? Gate : null),
            withGrains ? Grains : null,
            withTenants ? Tenants : null,
            Membership,
            limiter ?? new AppBridgeRateLimiter(Options, Time));

    public static AppBridgeTarget Target(string tree = "notes", long revision = Revision, string slug = Slug) =>
        new() { AppSlug = slug, InstallRevision = revision, LogicalTree = tree };

    public static AppManifest DefaultManifest(params AppUiBridgeDeclaration[] bridge) => UiTestManifests.WithUi(
        new AppManifest
        {
            Identity = new AppIdentity { Slug = AppSlug.Parse(Slug), Version = AppsControlHarness.V(AppsControlHarness.Version) },
            Trees =
            [
                new AppTreeDeclaration { Name = "notes" },
                new AppTreeDeclaration { Name = "legacy", AdoptedTreeId = AdoptedTree },
                new AppTreeDeclaration { Name = "archive" },
            ],
            Roles =
            [
                new AppRoleDeclaration
                {
                    Name = "viewer",
                    Operations = LatticeOperation.Read,
                    Scopes = [new AppScopeTemplate { Tree = "notes" }, new AppScopeTemplate { Tree = "legacy" }],
                },
                new AppRoleDeclaration
                {
                    Name = "editor",
                    Operations = LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete,
                    Scopes = [new AppScopeTemplate { Tree = "notes" }],
                },
                new AppRoleDeclaration
                {
                    Name = "drafter",
                    Operations = LatticeOperation.Read | LatticeOperation.Write,
                    Scopes =
                    [
                        new AppScopeTemplate { Tree = "notes", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "drafts/" },
                        new AppScopeTemplate { Tree = "notes", Kind = LatticeScopeKind.Key, KeyOrPrefix = "pinned" },
                    ],
                },
            ],
            Subscriptions = [],
            McpTools = [],
        },
        bridge.Length > 0
            ? bridge
            :
            [
                UiTestManifests.Bridge(AppUiBridgeOperations.DataRead),
                UiTestManifests.Bridge(AppUiBridgeOperations.DataWrite, "notes"),
                UiTestManifests.Bridge(AppUiBridgeOperations.DataDelete, "notes"),
            ]);

    public static AppCapabilityCeiling DefaultCeiling(params LatticeScope[] exceptions) => new()
    {
        AllowedOperations = LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete,
        ApprovedExceptionScopes = exceptions.Length > 0 ? exceptions : [LatticeScope.Tree(AdoptedTree)],
    };

    public AppRegistryRecord Record(
        AppRegistryLifecycleState state = AppRegistryLifecycleState.Enabled,
        TenantId? tenant = null,
        AppCapabilityCeiling? ceiling = null,
        AppUiBridgeRequest? consented = null) =>
        AppsControlHarness.Record(
            state,
            AppsControlHarness.Version,
            tenant,
            ceiling ?? DefaultCeiling(),
            [
                AppRoleBinding.Create("viewer", Viewers),
                AppRoleBinding.Create("editor", Editors),
                AppRoleBinding.Create("drafter", Drafters),
            ]) with
        {
            Revision = Revision,
            ConsentedBridge = consented ?? AppUiBridgeRequest.FromManifest(Manifest),
        };

    public BridgeHarness Publish(params AppRegistryRecord[] records)
    {
        Projection.Current = CompiledAppRegistrySnapshot.Compile(records, epoch: 1);
        return this;
    }

    /// <summary>Publishes the default enabled install and signs in <paramref name="subject"/> with <paramref name="groups"/>.</summary>
    public BridgeHarness Installed(string subject, params string[] groups)
    {
        Publish(Record());
        return As(subject, groups);
    }

    public BridgeHarness As(string subject, params string[] groups)
    {
        Membership = new SettableMembership { Subject = new LatticeSubject(subject, new HashSet<string>(groups, StringComparer.Ordinal)) };
        return this;
    }

    public BridgeHarness Seed(string tree, string key, byte[] value)
    {
        Store(tree)[key] = value;
        return this;
    }

    public SortedDictionary<string, byte[]> Store(string tree)
    {
        if (!Data.TryGetValue(tree, out var store))
        {
            store = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
            Data[tree] = store;
        }

        return store;
    }

    /// <summary>Asserts the data path was never dialled, so a refused request touched nothing.</summary>
    public void AssertDataPathUntouched()
    {
        Assert.That(Grains.ReceivedCalls(), Is.Empty, "the data path must not be dialled");
        Assert.That(Dialled, Is.Empty);
    }

    private ILattice Tree(string treeId)
    {
        Dialled.Add(treeId);
        var store = Store(treeId);
        var tree = Substitute.For<ILattice>();
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => DataFault is { } fault
                ? Task.FromException<byte[]?>(fault)
                : Task.FromResult<byte[]?>(store.TryGetValue(call.ArgAt<string>(0), out var value) ? value : null));
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (DataFault is { } fault)
                {
                    return Task.FromException(fault);
                }

                store[call.ArgAt<string>(0)] = call.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });
        tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => DataFault is { } fault
                ? Task.FromException<bool>(fault)
                : Task.FromResult(store.Remove(call.ArgAt<string>(0))));
        tree.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => Entries(store, call.ArgAt<string?>(0), call.ArgAt<string?>(1), call.ArgAt<CancellationToken>(4)));
        return tree;
    }

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        SortedDictionary<string, byte[]> store,
        string? start,
        string? end,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (DataFault is { } fault)
        {
            throw fault;
        }

        foreach (var entry in store.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if ((start is null || string.CompareOrdinal(entry.Key, start) >= 0)
                && (end is null || string.CompareOrdinal(entry.Key, end) < 0))
            {
                await Task.Yield();
                yield return entry;
            }
        }
    }

    /// <summary>A membership context resolving a settable subject.</summary>
    internal sealed class SettableMembership : ILatticeMembershipContext
    {
        public LatticeSubject Subject { get; set; } = new("alice");

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(Subject);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = Subject;
            return true;
        }
    }

    /// <summary>A manually advanced time source; the clock never moves on its own.</summary>
    internal sealed class ManualTime : TimeProvider
    {
        private long _ticks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => _ticks;

        public void Advance(TimeSpan by) => _ticks += by.Ticks;
    }
}
