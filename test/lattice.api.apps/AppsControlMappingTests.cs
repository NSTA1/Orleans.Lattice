using System.Collections.Immutable;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class AppsControlMappingTests
{
    private static AppCapabilityCeilingDescriptor Ceiling(params AppExceptionScope[] scopes) =>
        new() { AllowedOperations = LatticeOperation.Read, ApprovedExceptionScopes = [.. scopes] };

    private static IEnumerable<TestCaseData> InvalidScopes()
    {
        yield return new TestCaseData(null!).SetName("null scope");
        yield return new TestCaseData(new AppExceptionScope()).SetName("neither target");
        yield return new TestCaseData(new AppExceptionScope { App = "billing", Tree = "ledger", AdoptedTreeId = "legacy" }).SetName("both targets");
        yield return new TestCaseData(new AppExceptionScope { App = "billing" }).SetName("app without tree");
        yield return new TestCaseData(new AppExceptionScope { Tree = "ledger" }).SetName("tree without app");
        yield return new TestCaseData(new AppExceptionScope { App = "Billing", Tree = "ledger" }).SetName("bad app slug");
        yield return new TestCaseData(new AppExceptionScope { App = "billing", Tree = "a/b" }).SetName("tree path");
        yield return new TestCaseData(new AppExceptionScope { App = "billing", Tree = "*" }).SetName("tree wildcard");
        yield return new TestCaseData(new AppExceptionScope { AdoptedTreeId = "legacy", KeyOrPrefix = "k" }).SetName("tree scope with key");
        yield return new TestCaseData(new AppExceptionScope { AdoptedTreeId = "legacy", Kind = LatticeScopeKind.Key }).SetName("key scope without key");
        yield return new TestCaseData(new AppExceptionScope { AdoptedTreeId = "legacy", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "" }).SetName("prefix scope with empty prefix");
        yield return new TestCaseData(new AppExceptionScope { AdoptedTreeId = "legacy", Kind = (LatticeScopeKind)99 }).SetName("unknown kind");
    }

    [TestCaseSource(nameof(InvalidScopes))]
    public void ToEngineCeiling_rejects_invalid_scope_shapes(AppExceptionScope scope)
    {
        var ceiling = new AppCapabilityCeilingDescriptor { ApprovedExceptionScopes = ImmutableArray.Create(scope) };

        Assert.Throws<ArgumentException>(() => AppsControlMapping.ToEngineCeiling(ceiling, TenantId.Default));
    }

    [Test]
    public void ToEngineCeiling_null_ceiling_throws()
    {
        Assert.Throws<ArgumentException>(() => AppsControlMapping.ToEngineCeiling(null, TenantId.Default));
    }

    [Test]
    public void ToEngineCeiling_default_scope_array_is_empty()
    {
        var engine = AppsControlMapping.ToEngineCeiling(new AppCapabilityCeilingDescriptor { ApprovedExceptionScopes = default }, TenantId.Default);

        Assert.That(engine.ApprovedExceptionScopes, Is.Empty);
    }

    [Test]
    public void Ceiling_round_trips_through_engine_form()
    {
        var wire = Ceiling(
            new AppExceptionScope { App = "billing", Tree = "ledger" },
            new AppExceptionScope { App = "billing", Tree = "ledger", Kind = LatticeScopeKind.Prefix, KeyOrPrefix = "inv/" },
            new AppExceptionScope { AdoptedTreeId = "legacy-contacts", Kind = LatticeScopeKind.Key, KeyOrPrefix = "k" });

        var engine = AppsControlMapping.ToEngineCeiling(wire, TenantId.Parse("acme"));
        var back = AppsControlMapping.ToWireCeiling(engine);

        Assert.That(engine.ApprovedExceptionScopes, Is.EqualTo(new[]
        {
            LatticeScope.Tree("a/billing/ledger"),
            LatticeScope.Prefix("a/billing/ledger", "inv/"),
            LatticeScope.Key("legacy-contacts", "k"),
        }));
        Assert.That(back.AllowedOperations, Is.EqualTo(wire.AllowedOperations));
        Assert.That(back.ApprovedExceptionScopes, Is.EqualTo(wire.ApprovedExceptionScopes));
    }

    [Test]
    public void ToWireCeiling_malformed_app_tree_id_is_echoed_as_adopted()
    {
        var engine = new AppCapabilityCeiling { ApprovedExceptionScopes = [LatticeScope.Tree("a/billing")] };

        Assert.That(AppsControlMapping.ToWireCeiling(engine).ApprovedExceptionScopes.Single().AdoptedTreeId, Is.EqualTo("a/billing"));
    }

    [TestCase("parse", true)]
    [TestCase("a_b-9", true)]
    [TestCase("9a", false)]
    [TestCase("A", false)]
    [TestCase("", false)]
    [TestCase(null, false)]
    public void IsLocalTreeName_matches_manifest_grammar(string? name, bool expected)
    {
        Assert.That(AppsControlMapping.IsLocalTreeName(name), Is.EqualTo(expected));
        Assert.That(AppsControlMapping.IsLocalTreeName(new string('a', 129)), Is.False);
    }

    [Test]
    public void ToWireState_maps_every_registry_state()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsControlMapping.ToWireState(AppRegistryLifecycleState.Installed), Is.EqualTo(AppLifecycleState.Installed));
            Assert.That(AppsControlMapping.ToWireState(AppRegistryLifecycleState.Enabled), Is.EqualTo(AppLifecycleState.Enabled));
            Assert.That(AppsControlMapping.ToWireState(AppRegistryLifecycleState.Disabled), Is.EqualTo(AppLifecycleState.Disabled));
            Assert.That(AppsControlMapping.ToWireState(AppRegistryLifecycleState.Uninstalled), Is.EqualTo(AppLifecycleState.Uninstalled));
            Assert.Throws<InvalidOperationException>(() => AppsControlMapping.ToWireState((AppRegistryLifecycleState)42));
        });
    }

    [Test]
    public void ToWireState_uninstalled_app_is_never_failed()
    {
        var failed = AppsControlHarness.Status(AppsControlHarness.Outcome(
            AppActivationOperation.Uninstall, AppRegistryLifecycleState.Uninstalled, AppActivationFailure.Faulted));

        Assert.That(AppsControlMapping.ToWireState(AppRegistryLifecycleState.Uninstalled, failed), Is.EqualTo(AppLifecycleState.Uninstalled));
    }

    [Test]
    public void ToEngineBindings_default_is_empty()
    {
        Assert.That(AppsControlMapping.ToEngineBindings(default), Is.Empty);
    }

    [Test]
    public void ParseSlug_and_ParseVersion_reject_malformed_input_with_parameter_name()
    {
        var slug = Assert.Throws<ArgumentException>(() => AppsControlMapping.ParseSlug("x", "appSlug"));
        var version = Assert.Throws<ArgumentException>(() => AppsControlMapping.ParseVersion("1.0", "version"));

        Assert.That(slug!.ParamName, Is.EqualTo("appSlug"));
        Assert.That(version!.ParamName, Is.EqualTo("version"));
    }
}
