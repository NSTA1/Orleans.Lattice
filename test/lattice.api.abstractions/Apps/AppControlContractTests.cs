using System.Reflection;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Api.Abstractions.Tests.Apps;

[TestFixture]
public sealed class AppControlContractTests
{
    private static IEnumerable<TestCaseData> Methods()
    {
        yield return new("InstallAsync", typeof(AppLifecycleResult), new[] { typeof(AppInstallRequest) });
        yield return new("EnableAsync", typeof(AppLifecycleResult), new[] { typeof(string) });
        yield return new("DisableAsync", typeof(AppLifecycleResult), new[] { typeof(string) });
        yield return new("UninstallAsync", typeof(AppLifecycleResult), new[] { typeof(string) });
        yield return new("ListAsync", typeof(AppCatalog), Type.EmptyTypes);
        yield return new("DescribeAsync", typeof(AppDescriptor), new[] { typeof(string), typeof(string) });
        yield return new("GetConsentAsync", typeof(AppConsentReport), new[] { typeof(string) });
        yield return new("UpdateConsentAsync", typeof(AppConsentReport), new[] { typeof(AppConsentUpdate) });
        yield return new("GetCapabilitiesAsync", typeof(LatticeAppsCapabilities), Type.EmptyTypes);
    }

    [TestCaseSource(nameof(Methods))]
    public void Operation_exposes_expected_payload_and_optional_cancellation(string name, Type result, Type[] arguments)
    {
        var method = typeof(ILatticeAppsControl).GetMethod(name)!;
        Assert.That(method, Is.Not.Null);
        var parameters = method.GetParameters();
        Assert.Multiple(() =>
        {
            Assert.That(method.ReturnType, Is.EqualTo(typeof(Task<>).MakeGenericType(result)));
            Assert.That(parameters[..^1].Select(p => p.ParameterType), Is.EqualTo(arguments));
            Assert.That(parameters[^1].ParameterType, Is.EqualTo(typeof(CancellationToken)));
            Assert.That(parameters[^1].HasDefaultValue, Is.True);
        });
    }

    [Test]
    public void Contract_remains_public_and_has_no_app_engine_dependency()
    {
        Assert.Multiple(() =>
        {
            Assert.That(typeof(ILatticeAppsControl).IsPublic, Is.True);
            Assert.That(typeof(ILatticeAppsControl).IsInterface, Is.True);
            Assert.That(typeof(ILatticeAppsControl).GetMethods(), Has.Length.EqualTo(9));
            Assert.That(typeof(ILatticeAppsControl).Assembly.GetReferencedAssemblies().Select(a => a.Name),
                Does.Not.Contain("Orleans.Lattice.Apps"));
            Assert.That(typeof(ILatticeAppsControl).GetMethod("DescribeAsync")!.GetParameters()[1].DefaultValue, Is.Null);
        });
    }

    [Test]
    public void Exception_scope_contract_carries_only_named_tree_targets()
    {
        Assert.That(typeof(AppExceptionScope).GetProperties().Select(p => p.Name),
            Is.EquivalentTo(new[] { "Kind", "App", "Tree", "AdoptedTreeId", "KeyOrPrefix" }));
    }

    [Test]
    public void Alias_table_is_short_unique_complete_and_disjoint_from_other_contracts()
    {
        var aliases = typeof(ApiAppsTypeAliases).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.Name != nameof(ApiAppsTypeAliases.AliasPrefix))
            .Select(f => (string)f.GetRawConstantValue()!).ToArray();
        var types = typeof(ILatticeAppsControl).Assembly.GetTypes();
        var usage = types.SelectMany(t => t.GetCustomAttributes<AliasAttribute>()
            .Select(a => (Type: t, Value: a.Alias))).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(ApiAppsTypeAliases.AliasPrefix, Is.EqualTo("oia."));
            Assert.That(aliases, Has.Length.EqualTo(39));
            Assert.That(aliases.Distinct().Count(), Is.EqualTo(aliases.Length));
            Assert.That(aliases, Is.All.StartsWith(ApiAppsTypeAliases.AliasPrefix));
            Assert.That(aliases.Select(a => a.Length), Is.All.LessThanOrEqualTo(6));
            Assert.That(usage.Where(a => a.Type.Namespace == typeof(ILatticeAppsControl).Namespace).Select(a => a.Value),
                Is.EquivalentTo(aliases));
        });
        foreach (var alias in aliases)
            Assert.That(usage.Count(a => a.Value == alias), Is.EqualTo(1), alias);
        foreach (var sibling in types.Where(t => t.Name.EndsWith("TypeAliases", StringComparison.Ordinal)
                     && t != typeof(ApiAppsTypeAliases)))
        {
            if (sibling.GetField("AliasPrefix")?.GetRawConstantValue() is not string prefix)
                continue;
            Assert.That(ApiAppsTypeAliases.AliasPrefix.StartsWith(prefix, StringComparison.Ordinal)
                || prefix.StartsWith(ApiAppsTypeAliases.AliasPrefix, StringComparison.Ordinal),
                Is.False, sibling.FullName);
        }
    }
}
