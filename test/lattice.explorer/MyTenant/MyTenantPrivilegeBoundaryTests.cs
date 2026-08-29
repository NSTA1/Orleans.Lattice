using System.IO;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The privilege boundary between a tenant administrator and a platform
/// operator, enforced as a property of the plugin's source rather than as a
/// claim in its documentation.
/// <para>
/// The epic's tenancy split gives a tenant admin its membership, its residency,
/// its sharing, and a <em>read</em> of its quota usage. Authoring quota ceilings,
/// widening the operator-authorized allowed region set, and the tenant lifecycle
/// (create, suspend, resume, delete) are operator-only. The cluster enforces
/// this - each of those facade members resolves the operator-only
/// <c>TenantAdminAccessAuthorizer</c> or <c>AuthorizeOperatorAsync</c>, while
/// every member this plugin does call resolves the operator-or-live-tenant-admin
/// <c>TenantRegionResidencyAuthorizer</c> - so an added control would not grant
/// anything. It would still be a surface promising a tenant admin something the
/// server will always refuse, which is why it is caught here instead.
/// </para>
/// </summary>
/// <remarks>
/// A source scan rather than a reflection walk, because the point is that the
/// call is never <em>written</em>: the seam exposes all of these on the one
/// <see cref="ITenantAdminService"/> the plugin holds, so nothing in the type
/// system stops a future edit from reaching for one.
/// </remarks>
[TestFixture]
public sealed class MyTenantPrivilegeBoundaryTests
{
    private const string PluginSourceRoot = "src/lattice.explorer/Plugins/MyTenant";

    /// <summary>
    /// The operations the tenant-administration facade reserves for a platform
    /// operator. Every one of them is reachable from the
    /// <see cref="ITenantAdminService"/> the plugin holds, and none of them may
    /// be called by it.
    /// </summary>
    private static readonly string[] OperatorOnlyOperations =
    [
        // Authoring ceilings is an operator action; a tenant admin sees usage
        // against them and nothing more.
        nameof(ITenantAdminService.SetQuotasAsync),

        // Residency is the tenant's to choose; the allowed set it must stay
        // within is the operator's to authorize.
        nameof(ITenantAdminService.AuthorizeAllowedRegionsAsync),

        // The tenant lifecycle.
        nameof(ITenantAdminService.CreateTenantAsync),
        nameof(ITenantAdminService.SuspendTenantAsync),
        nameof(ITenantAdminService.ResumeTenantAsync),
        nameof(ITenantAdminService.DeleteTenantAsync),
    ];

    [Test]
    public void The_plugin_never_calls_an_operator_only_tenant_operation()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var root = Path.Combine(repoRoot, PluginSourceRoot.Replace('/', Path.DirectorySeparatorChar));

        var violations = new List<string>();
        var scanned = 0;

        foreach (var pattern in new[] { "*.cs", "*.razor" })
        {
            foreach (var file in HygieneRepository.EnumerateFiles(root, pattern))
            {
                scanned++;
                var lines = File.ReadAllLines(file);
                for (var i = 0; i < lines.Length; i++)
                {
                    // Only a real call site counts. The names are legitimately
                    // mentioned in prose - this fixture's own rationale lives in
                    // the plugin's XML docs - so an invocation is what is scanned
                    // for, not the bare identifier.
                    foreach (var operation in OperatorOnlyOperations)
                    {
                        if (lines[i].Contains(operation + "(", StringComparison.Ordinal))
                        {
                            violations.Add($"{Relative(repoRoot, file)}:{i + 1}: {lines[i].Trim()}");
                        }
                    }
                }
            }
        }

        // Without this the gate would pass vacuously if the plugin ever moved.
        Assert.That(scanned, Is.GreaterThan(1), "the scan must reach the plugin's sources");

        Assert.That(violations, Is.Empty,
            "The My Tenant plugin is a tenant administrator's surface. Authoring quota ceilings, "
            + "widening the allowed region set, and the tenant lifecycle are platform-operator "
            + "actions: the cluster resolves the operator-only authorizer for each, so a control "
            + "here would promise a tenant admin something that is always refused."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_scanner_detects_a_call_it_is_shown()
    {
        // Battery test for the smoke detector: a change that neutered the match
        // would let the gate above pass on a plugin that had grown a violation.
        const string CallSite = "await _domain.Tenants.SetQuotasAsync(tenantId, limits);";
        const string Prose = "Authoring quotas is an operator action.";

        Assert.Multiple(() =>
        {
            Assert.That(
                CallSite.Contains(nameof(ITenantAdminService.SetQuotasAsync) + "(", StringComparison.Ordinal),
                Is.True);
            Assert.That(
                Prose.Contains(nameof(ITenantAdminService.SetQuotasAsync) + "(", StringComparison.Ordinal),
                Is.False,
                "prose naming the operation must not trip the gate");
        });
    }

    [Test]
    public void Quota_usage_itself_stays_readable()
    {
        // The mirror of the guard: the tenant admin genuinely does get to see
        // consumption against its ceilings, so the boundary above is about
        // authoring them, not about hiding them.
        Assert.That(
            OperatorOnlyOperations,
            Has.None.EqualTo(nameof(ITenantAdminService.GetQuotaUsageAsync)));
    }

    private static string Relative(string repoRoot, string file) =>
        Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
}
