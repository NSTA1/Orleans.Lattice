using System.Collections.Immutable;
using System.Reflection;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

/// <summary>
/// Pins the shape of the four app contracts epic #3807 adds beside
/// <see cref="ILatticeAppsControl"/>: the administrative catalogue, the per-user
/// workspace, the untrusted-UI data bridge, and role re-binding.
/// </summary>
[TestFixture]
public sealed class AppEpicContractTests
{
    private static IEnumerable<TestCaseData> Contracts()
    {
        yield return new TestCaseData(typeof(ILatticeAppCatalog), new[]
        {
            "Task<ImmutableArray<AppSourceSummary>> ListSourcesAsync(CancellationToken cancellationToken = default)",
            "Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)",
            "Task<AppDescriptor> DescribeFromSourceAsync(String sourceKey, String appSlug, String version = default, CancellationToken cancellationToken = default)",
            "Task<AppIconAsset> GetIconAsync(String sourceKey, String appSlug, String version = default, CancellationToken cancellationToken = default)",
            "Task<LatticeAppCatalogCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default)",
        }).SetName("Catalog_contract_exposes_exactly_its_operations");
        yield return new TestCaseData(typeof(ILatticeAppWorkspace), new[]
        {
            "Task<ImmutableArray<WorkspaceAppSummary>> ListMyAppsAsync(CancellationToken cancellationToken = default)",
            "Task<WorkspaceAppDescriptor> DescribeMyAppAsync(String appSlug, CancellationToken cancellationToken = default)",
            "Task<AppIconAsset> GetIconAsync(String appSlug, CancellationToken cancellationToken = default)",
            "Task<AppUiAsset> GetUiAssetAsync(String appSlug, String path, CancellationToken cancellationToken = default)",
        }).SetName("Workspace_contract_exposes_exactly_its_operations");
        yield return new TestCaseData(typeof(ILatticeAppBridge), new[]
        {
            "Task<AppBridgeValue> GetAsync(AppBridgeTarget target, String key, CancellationToken cancellationToken = default)",
            "Task<AppBridgePage> ScanAsync(AppBridgeTarget target, String prefix, Int32 pageSize, String continuation = default, CancellationToken cancellationToken = default)",
            "Task SetAsync(AppBridgeTarget target, String key, ReadOnlyMemory<Byte> value, CancellationToken cancellationToken = default)",
            "Task<Boolean> DeleteAsync(AppBridgeTarget target, String key, CancellationToken cancellationToken = default)",
        }).SetName("Bridge_contract_exposes_exactly_its_operations");
        yield return new TestCaseData(typeof(ILatticeAppRoleBindings), new[]
        {
            "Task<AppRoleBindingsReport> UpdateRoleBindingsAsync(AppRoleBindingsUpdate request, CancellationToken cancellationToken = default)",
        }).SetName("Role_bindings_contract_exposes_exactly_its_operations");
    }

    [TestCaseSource(nameof(Contracts))]
    public void Contract_exposes_exactly_its_operations(Type contract, string[] expected)
    {
        Assert.Multiple(() =>
        {
            Assert.That(contract.IsInterface && contract.IsPublic, Is.True);
            Assert.That(contract.Assembly, Is.EqualTo(typeof(ILatticeAppsControl).Assembly));
            Assert.That(contract.Namespace, Is.EqualTo(typeof(ILatticeAppsControl).Namespace));
            Assert.That(contract.GetInterfaces(), Is.Empty);
            Assert.That(contract.GetMembers(), Has.Length.EqualTo(expected.Length));
            Assert.That(contract.GetMethods().Select(ContractSignature.Render), Is.EqualTo(expected));
        });
    }

    [TestCase(typeof(ILatticeAppCatalog), "DescribeFromSourceAsync")]
    [TestCase(typeof(ILatticeAppCatalog), "GetIconAsync")]
    [TestCase(typeof(ILatticeAppWorkspace), "DescribeMyAppAsync")]
    [TestCase(typeof(ILatticeAppWorkspace), "GetIconAsync")]
    [TestCase(typeof(ILatticeAppWorkspace), "GetUiAssetAsync")]
    [TestCase(typeof(ILatticeAppBridge), "GetAsync")]
    public void Absent_results_are_annotated_nullable(Type contract, string method)
    {
        var returned = new NullabilityInfoContext().Create(contract.GetMethod(method)!.ReturnParameter);

        Assert.That(returned.GenericTypeArguments.Single().ReadState, Is.EqualTo(NullabilityState.Nullable));
    }

    [TestCase(typeof(ILatticeAppCatalog), "ListAvailableAsync")]
    [TestCase(typeof(ILatticeAppWorkspace), "ListMyAppsAsync")]
    [TestCase(typeof(ILatticeAppBridge), "ScanAsync")]
    public void Collection_results_are_never_null(Type contract, string method)
    {
        var returned = new NullabilityInfoContext().Create(contract.GetMethod(method)!.ReturnParameter);

        Assert.That(returned.GenericTypeArguments.Single().ReadState, Is.Not.EqualTo(NullabilityState.Nullable));
    }

    [Test]
    public void Bridge_addresses_data_only_by_app_revision_and_logical_tree()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(AppBridgeTarget).GetProperties().Select(p => p.Name),
                Is.EqualTo(new[] { "AppSlug", "InstallRevision", "LogicalTree" }));
            foreach (var method in typeof(ILatticeAppBridge).GetMethods())
            {
                var parameters = method.GetParameters();
                Assert.That(parameters[0].ParameterType, Is.EqualTo(typeof(AppBridgeTarget)), method.Name);
                Assert.That(parameters.Select(p => p.Name!), Has.None.Contains("tree").IgnoreCase, method.Name);
            }
        });
    }

    [Test]
    public void Bridge_has_no_lifecycle_verbs()
    {
        var verbs = new[] { "Install", "Enable", "Disable", "Uninstall", "Consent", "Bind", "Upgrade" };

        Assert.That(typeof(ILatticeAppBridge).GetMethods().Select(m => m.Name),
            Has.None.Matches<string>(name => verbs.Any(v => name.Contains(v, StringComparison.Ordinal))));
    }

    [Test]
    public void New_contracts_carry_no_app_engine_dependency()
    {
        var engine = typeof(ILatticeAppCatalog).Assembly.GetReferencedAssemblies().Select(a => a.Name);

        Assert.That(engine, Does.Not.Contain("Orleans.Lattice.Apps"));
    }

    [Test]
    public void Every_async_operation_takes_an_optional_trailing_cancellation_token()
    {
        foreach (var method in new[] { typeof(ILatticeAppCatalog), typeof(ILatticeAppWorkspace), typeof(ILatticeAppBridge) }
                     .SelectMany(t => t.GetMethods()))
        {
            var last = method.GetParameters()[^1];
            Assert.That(last.ParameterType, Is.EqualTo(typeof(CancellationToken)), method.Name);
            Assert.That(last.HasDefaultValue, Is.True, method.Name);
            Assert.That(typeof(Task).IsAssignableFrom(method.ReturnType), Is.True, method.Name);
        }
    }

    [Test]
    public void Listing_results_use_immutable_arrays()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ILatticeAppCatalog).GetMethod("ListSourcesAsync")!.ReturnType,
                Is.EqualTo(typeof(Task<ImmutableArray<AppSourceSummary>>)));
            Assert.That(typeof(ILatticeAppWorkspace).GetMethod("ListMyAppsAsync")!.ReturnType,
                Is.EqualTo(typeof(Task<ImmutableArray<WorkspaceAppSummary>>)));
        });
    }
}
