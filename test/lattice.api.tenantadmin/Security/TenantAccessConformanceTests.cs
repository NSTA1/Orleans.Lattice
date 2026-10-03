using Orleans.Lattice.Auth;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>
/// The security conformance suite for delegated tenant access administration (epic
/// #4154, issue #4172): one named, end-to-end proof of the epic's security invariants,
/// driven through the public facades and the real access gate on a single-silo cluster
/// with auth, membership and tenancy, and two tenants A and B per case. Each numbered
/// case is its own test, named for the invariant it proves and the decision it
/// enforces (D2, D3, D4, D8, D9, D12, D14, D15, D19, and the T1 and A1 guards).
/// </summary>
/// <remarks>
/// <para>
/// <b>Non-vacuous by construction.</b> Policy and membership changes reach the
/// compiled snapshots asynchronously, so a positive verdict is polled against a
/// deadline, and every "cannot" assertion is preceded by an observation that proves
/// the rule or entry it is about is already live: either the same subject's verdict
/// is first observed in the opposite state, or a sentinel rule written after it is
/// observed in force. A refusal therefore cannot pass merely because a snapshot was
/// stale. The revocation and flag-off cases assert on the very next request, with no
/// poll, because their guarantee is exactly that no window exists.
/// </para>
/// <para>
/// <b>Isolation between tests.</b> Every test uses its own tenants and subjects,
/// prefixed by its case number, so tests are order independent. The delegated access
/// flag is a cluster-wide runtime switch; tests that turn it off restore it, and
/// <see cref="EnableTheFeature"/> turns it back on before every test.
/// </para>
/// <para>
/// Integration category: owned by the epic coordinator's integration run.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed partial class TenantAccessConformanceTests
{
    private const string Operator = TenantAccessConformanceClusterFixture.Operator;

    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    private readonly TenantAccessConformanceClusterFixture _fixture = new();

    [OneTimeSetUp]
    public Task SetUp() => _fixture.InitializeAsync();

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [SetUp]
    public void EnableTheFeature() => TenantAccessConformanceClusterFixture.Switch.Set(true);

    private static IDisposable As(string subjectId, params string[] assertedGroups) =>
        TenantAccessConformanceClusterFixture.As(subjectId, assertedGroups);

    private static string TreeOf(TenantId tenant, string localName) => $"t/{tenant.Value}/{localName}";

    private static string GroupOf(TenantId tenant, string localName) => $"t/{tenant.Value}/{localName}";

    private static TenantRuleDraft TreeRule(
        string ruleId,
        string subjectId,
        string treeName,
        LatticeEffect effect = LatticeEffect.Allow,
        LatticeOperation operations = LatticeOperation.Read,
        TenantSubjectKind subjectKind = TenantSubjectKind.User) => new()
        {
            RuleId = ruleId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            ScopeKind = TenantRuleScopeKind.Tree,
            TreeName = treeName,
            Operations = operations,
            Effect = effect,
        };

    private static TenantRuleDraft TenantWideRule(
        string ruleId,
        string subjectId,
        LatticeEffect effect = LatticeEffect.Allow,
        LatticeOperation operations = LatticeOperation.Read,
        TenantSubjectKind subjectKind = TenantSubjectKind.User) => new()
        {
            RuleId = ruleId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            ScopeKind = TenantRuleScopeKind.TenantWide,
            Operations = operations,
            Effect = effect,
        };

    private Task WaitAllowedAsync(
        string subjectId, TenantId activeTenant, string treeId, string because,
        LatticeOperation operation = LatticeOperation.Read, string key = "k1") =>
        TestPoll.UntilAsync(
            () => _fixture.AllowsAsync(subjectId, activeTenant.Value, treeId, operation, key),
            because,
            Deadline);

    private Task WaitDeniedAsync(
        string subjectId, TenantId activeTenant, string treeId, string because,
        LatticeOperation operation = LatticeOperation.Read, string key = "k1") =>
        TestPoll.UntilAsync(
            async () => !await _fixture.AllowsAsync(subjectId, activeTenant.Value, treeId, operation, key),
            because,
            Deadline);

    private Task<bool> AllowsAsync(
        string subjectId, TenantId activeTenant, string treeId,
        LatticeOperation operation = LatticeOperation.Read, string key = "k1") =>
        _fixture.AllowsAsync(subjectId, activeTenant.Value, treeId, operation, key);

    /// <summary>Runs <paramref name="action"/> and returns what it threw, or <c>null</c>.</summary>
    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
