using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Shared harness for the workspace tests: a settable registry projection, named test sources composed into a
/// real <see cref="AppSourceSet"/>, a granting access gate and a fixed-subject membership context, wired into
/// the real <see cref="AppRoleGrantEvaluator"/> and <see cref="LatticeAppWorkspace"/>.
/// </summary>
internal sealed class WorkspaceHarness
{
    public const string Alice = "alice";

    public static readonly AppSlug Crm = AppSlug.Parse(AppsControlHarness.Slug);

    public WorkspaceHarness()
    {
        Source = new TestCatalogSource(InImageAppSource.SourceKey).Publish(Manifest()).WithUiAssets();
        Sources = new AppSourceSet([Source]);
    }

    public TestCatalogSource Source { get; }

    public AppSourceSet Sources { get; set; }

    public SettableProjection Projection { get; } = new();

    public GrantingGate Gate { get; } = new();

    public ConfigurableTenantResolver Tenants { get; } = new();

    public ILatticeMembershipContext? Membership { get; set; } = new FixedSubjectMembership(Alice);

    public IAppActivationPipeline Pipeline { get; } = Substitute.For<IAppActivationPipeline>();

    public LatticeAppWorkspace Workspace => new(
        new AppRoleGrantEvaluator(Projection, Sources, Gate), Sources, Tenants, Membership, Pipeline);

    public static AppManifest Manifest(string version = AppsControlHarness.Version) =>
        UiTestManifests.WithUi(AppsControlHarness.Manifest(version), UiTestManifests.Bridge(AppUiBridgeOperations.DataRead));

    public static AppRegistryRecord Record(
        AppRegistryLifecycleState state = AppRegistryLifecycleState.Enabled,
        TenantId? tenant = null,
        string version = AppsControlHarness.Version) =>
        AppsControlHarness.Record(state, version, tenant) with { Revision = 7 };

    public WorkspaceHarness Publish(params AppRegistryRecord[] records)
    {
        Projection.Current = CompiledAppRegistrySnapshot.Compile(records, epoch: 1);
        return this;
    }

    /// <summary>Grants Alice read on the app's contacts tree, which holds the manifest's reader role.</summary>
    public WorkspaceHarness GrantReader(TenantId? tenant = null)
    {
        Gate.Grant(Alice, AppsTreeId(tenant ?? TenantId.Default, "contacts"), LatticeOperation.Read);
        return this;
    }

    public static string AppsTreeId(TenantId tenant, string tree) =>
        tenant.IsDefault ? $"a/{AppsControlHarness.Slug}/{tree}" : $"t/{tenant.Value}/a/{AppsControlHarness.Slug}/{tree}";

    /// <summary>A projection whose snapshot a test sets.</summary>
    internal sealed class SettableProjection : IAppRegistryProjection
    {
        public CompiledAppRegistrySnapshot Current { get; set; } = CompiledAppRegistrySnapshot.Empty;

        public long CurrentEpoch => Current.Epoch;

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    /// <summary>A gate allowing exactly the granted (subject, tree, operation) triples.</summary>
    internal sealed class GrantingGate : ILatticeAccessGate
    {
        private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _grants = [];

        public int Requests { get; private set; }

        public void Grant(string subject, string tree, LatticeOperation operation) => _grants.Add((subject, tree, operation));

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Requests++;
            return new(_grants.Contains((request.Subject.SubjectId, request.TreeId, request.Operation))
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not granted"));
        }
    }

    /// <summary>A membership context resolving one fixed subject.</summary>
    internal sealed class FixedSubjectMembership(string subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(new LatticeSubject(subject));

        public bool TryResolveCurrent(out LatticeSubject resolved)
        {
            resolved = new LatticeSubject(subject);
            return true;
        }
    }
}
