using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Shared harness for the workspace tests: a settable registry projection, named test sources composed into a
/// real <see cref="AppSourceSet"/>, and a fixed-subject membership context whose groups a test joins, wired into
/// the real <see cref="AppRoleGrantEvaluator"/> and <see cref="LatticeAppWorkspace"/>. Roles are held by binding,
/// so a test grants one by joining Alice to the group the record binds to it.
/// </summary>
internal sealed class WorkspaceHarness
{
    public const string Alice = "alice";

    public static readonly AppSlug Crm = AppSlug.Parse(AppsControlHarness.Slug);

    public WorkspaceHarness()
    {
        Source = new TestCatalogSource(InImageAppSource.SourceKey).Publish(Manifest()).WithUiAssets();
        Sources = new AppSourceSet([Source]);
        Membership = AliceMembership;
    }

    public TestCatalogSource Source { get; }

    public AppSourceSet Sources { get; set; }

    public SettableProjection Projection { get; } = new();

    /// <summary>
    /// The access gate: the whole policy, including the install's compiled app rules. It allows by default (the
    /// app rules are live and nothing denies), so a test removes a role with an explicit deny.
    /// </summary>
    public GrantingGate Gate { get; } = new() { AllowByDefault = true };

    public ConfigurableTenantResolver Tenants { get; } = new();

    public FixedSubjectMembership AliceMembership { get; } = new(Alice);

    public ILatticeMembershipContext? Membership { get; set; }

    public IAppActivationPipeline Pipeline { get; } = Substitute.For<IAppActivationPipeline>();

    public LatticeAppWorkspace Workspace => new(
        new AppRoleGrantEvaluator(Projection, Sources, Gate), Sources, Tenants, Membership, Pipeline);

    public static AppManifest Manifest(string version = AppsControlHarness.Version) =>
        UiTestManifests.WithUi(AppsControlHarness.Manifest(version), UiTestManifests.Bridge(AppUiBridgeOperations.DataRead));

    public static AppRegistryRecord Record(
        AppRegistryLifecycleState state = AppRegistryLifecycleState.Enabled,
        TenantId? tenant = null,
        string version = AppsControlHarness.Version) =>
        AppsControlHarness.Record(state, version, tenant) with
        {
            Revision = 7,

            // An enabled install's recorded consent covers what its UI requests; the workspace
            // describes only consented grants.
            ConsentedBridge = AppUiBridgeRequest.FromManifest(Manifest(version)),
        };

    public WorkspaceHarness Publish(params AppRegistryRecord[] records)
    {
        Projection.Current = CompiledAppRegistrySnapshot.Compile(records, epoch: 1);
        return this;
    }

    /// <summary>
    /// Joins Alice to <c>g-readers</c>, the group the harness record binds to the manifest's reader role. The
    /// binding lives on the record, so it holds the role in whichever tenant the record is installed in.
    /// </summary>
    public WorkspaceHarness GrantReader()
    {
        AliceMembership.Join(ReadersGroup);
        return this;
    }

    /// <summary>The group <see cref="Record"/> binds to the reader role.</summary>
    public const string ReadersGroup = "g-readers";

    public static string AppsTreeId(TenantId tenant, string tree) =>
        tenant.IsDefault ? $"a/{AppsControlHarness.Slug}/{tree}" : $"t/{tenant.Value}/a/{AppsControlHarness.Slug}/{tree}";

    /// <summary>A projection whose snapshot a test sets.</summary>
    internal sealed class SettableProjection : IAppRegistryProjection
    {
        public CompiledAppRegistrySnapshot Current { get; set; } = CompiledAppRegistrySnapshot.Empty;

        public long CurrentEpoch => Current.Epoch;

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    /// <summary>A gate allowing exactly the granted (subject, tree, operation) triples, or everything not explicitly denied.</summary>
    internal sealed class GrantingGate : ILatticeAccessGate
    {
        private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _grants = [];
        private readonly HashSet<(string Subject, string Tree, LatticeOperation Operation)> _denies = [];

        public int Requests { get; private set; }

        /// <summary>
        /// When true, a request neither granted nor denied is allowed - the policy as it stands once an install's
        /// compiled app rules are live and nothing denies the caller.
        /// </summary>
        public bool AllowByDefault { get; set; }

        public void Grant(string subject, string tree, LatticeOperation operation) => _grants.Add((subject, tree, operation));

        /// <summary>Adds an explicit deny on every bit of <paramref name="operations"/>, which wins over every grant.</summary>
        public void Deny(string subject, string tree, LatticeOperation operations)
        {
            foreach (var value in Enum.GetValues<LatticeOperation>())
            {
                if (value != LatticeOperation.None && (operations & value) == value)
                    _denies.Add((subject, tree, value));
            }
        }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Requests++;
            var triple = (request.Subject.SubjectId, request.TreeId, request.Operation);
            if (_denies.Contains(triple))
                return new(LatticeAccessDecision.Deny("explicitly denied"));
            return new(AllowByDefault || _grants.Contains(triple)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny("not granted"));
        }
    }

    /// <summary>A membership context resolving one fixed subject with the groups a test joined it to.</summary>
    internal sealed class FixedSubjectMembership(string subject) : ILatticeMembershipContext
    {
        private readonly HashSet<string> _groups = new(StringComparer.Ordinal);

        public FixedSubjectMembership Join(string group)
        {
            _groups.Add(group);
            return this;
        }

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(Current());

        public bool TryResolveCurrent(out LatticeSubject resolved)
        {
            resolved = Current();
            return true;
        }

        private LatticeSubject Current() => new(subject, new HashSet<string>(_groups, StringComparer.Ordinal));
    }
}
