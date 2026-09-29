using System.Collections.Immutable;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class AppsControlMappingTests
{
    private static AppCapabilityCeilingDescriptor Ceiling(params AppExceptionScope[] scopes) =>
        new() { AllowedOperations = LatticeOperation.Read, ApprovedExceptionScopes = [.. scopes] };

    private static AppProvenance Provenance =>
        new() { Source = "in-image", Publisher = "first-party", Reference = "ref" };

    private static AppManifest EmptyManifest() =>
        new()
        {
            Identity = new AppIdentity
            {
                Slug = AppSlug.Parse(AppsControlHarness.Slug),
                Version = AppVersion.Parse(AppsControlHarness.Version),
            },
            Trees = [],
            Roles = [],
            Subscriptions = [],
            McpTools = [],
            Replication = null,
            Schema = null,
        };

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
    public void ToEngineBindings_accepts_several_distinct_roles()
    {
        // The duplicate-role scan only completes normally when a later binding clears every
        // earlier one. Every existing case either supplies a single binding (so the scan never
        // runs) or a duplicate (so it throws on the first comparison), leaving the pass arm of
        // the scan unexercised.
        var engine = AppsControlMapping.ToEngineBindings(
        [
            new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-readers" },
            new AppRoleBindingDescriptor { RoleName = "writer", GroupId = "g-writers" },
            new AppRoleBindingDescriptor { RoleName = "admin", GroupId = "g-admins" },
        ]);

        Assert.That(engine.Select(b => b.RoleName), Is.EqualTo(new[] { "reader", "writer", "admin" }));
        Assert.That(engine.Select(b => b.GroupId), Is.EqualTo(new[] { "g-readers", "g-writers", "g-admins" }));
    }

    [Test]
    public void ToEngineBindings_still_rejects_a_duplicate_role_after_several_distinct_ones()
    {
        // Pins the scan's reject arm at an index the pass arm above has to walk past, so a scan
        // that stopped comparing after the first binding could not satisfy both tests at once.
        var duplicate = Assert.Throws<ArgumentException>(() => AppsControlMapping.ToEngineBindings(
        [
            new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-readers" },
            new AppRoleBindingDescriptor { RoleName = "writer", GroupId = "g-writers" },
            new AppRoleBindingDescriptor { RoleName = "reader", GroupId = "g-others" },
        ]));

        Assert.That(duplicate!.Message, Does.Contain("already bound"));
    }

    [Test]
    public void ToWireBindings_maps_an_absent_or_empty_binding_list_to_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsControlMapping.ToWireBindings(null), Is.Empty);
            Assert.That(AppsControlMapping.ToWireBindings([]), Is.Empty);
        });
    }

    [Test]
    public void ToWireBindings_maps_a_populated_binding_list()
    {
        // Anti-vacuity for the two empty cases above: a mapper that always returned empty would
        // satisfy them both, and only this case separates "absent maps to empty" from "maps nothing".
        var wire = AppsControlMapping.ToWireBindings(
            [AppRoleBinding.Create("reader", "g-readers"), AppRoleBinding.Create("writer", "g-writers")]);

        Assert.That(wire.Select(b => b.RoleName), Is.EqualTo(new[] { "reader", "writer" }));
        Assert.That(wire.Select(b => b.GroupId), Is.EqualTo(new[] { "g-readers", "g-writers" }));
    }

    [Test]
    public void ToDescriptor_maps_a_manifest_declaring_nothing_to_empty_collections()
    {
        // Every optional manifest collection carries its own verbatim absent-or-empty guard, and
        // a manifest declaring none of them is the only input that reaches all of them at once.
        var descriptor = AppsControlMapping.ToDescriptor(
            EmptyManifest(), Provenance, AppLifecycleState.Installed, record: null);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Trees, Is.Empty);
            Assert.That(descriptor.Roles, Is.Empty);
            Assert.That(descriptor.Subscriptions, Is.Empty);
            Assert.That(descriptor.McpTools, Is.Empty);
            Assert.That(descriptor.Replication, Is.Empty);
            Assert.That(descriptor.Schema, Is.Empty);
            Assert.That(descriptor.RoleBindings, Is.Empty);
            Assert.That(descriptor.Ceiling, Is.Null);
            Assert.That(descriptor.Slug, Is.EqualTo(AppsControlHarness.Slug));
        });
    }

    [Test]
    public void ToDescriptor_maps_a_populated_manifest_to_populated_collections()
    {
        // Anti-vacuity for the empty-manifest case: these are the same six guards, taken on the
        // other side, so a mapper that dropped every collection cannot pass both.
        var descriptor = AppsControlMapping.ToDescriptor(
            AppsControlHarness.Manifest(), Provenance, AppLifecycleState.Enabled, record: null);

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Trees, Is.Not.Empty);
            Assert.That(descriptor.Roles, Is.Not.Empty);
            Assert.That(descriptor.Subscriptions, Is.Not.Empty);
            Assert.That(descriptor.McpTools, Is.Not.Empty);
            Assert.That(descriptor.Replication, Is.Not.Empty);
            Assert.That(descriptor.Schema, Is.Not.Empty);
        });
    }

    [Test]
    public void ToDescriptor_reports_no_ownership_conflict_when_none_were_supplied()
    {
        // The conflict lookup is only consulted per declared tree, so reaching its absent-list
        // guard needs a manifest that declares at least one tree and a null conflict list.
        var descriptor = AppsControlMapping.ToDescriptor(
            AppsControlHarness.Manifest(), Provenance, AppLifecycleState.Installed, record: null, conflicts: null);

        Assert.That(descriptor.Trees, Is.Not.Empty);
        Assert.That(descriptor.Trees.Select(t => t.OwnershipConflict), Is.All.Null);
    }

    [Test]
    public void ToDescriptor_reports_a_conflict_against_the_tree_it_names()
    {
        // Anti-vacuity for the case above, and it pins the per-tree match: a lookup that always
        // returned null would pass that test, and one that ignored the tree name would report the
        // conflict against both trees.
        var descriptor = AppsControlMapping.ToDescriptor(
            AppsControlHarness.Manifest(),
            Provenance,
            AppLifecycleState.Installed,
            record: null,
            conflicts: [new AppTreeOwnershipConflict(
                "contacts", AppTreeOwnershipConflictReason.OwnedByAnotherApp, AppSlug.Parse("billing"), "owned by a/billing/contacts")]);

        var contacts = descriptor.Trees.Single(t => t.Name == "contacts");
        var legacy = descriptor.Trees.Single(t => t.Name == "legacy");

        Assert.Multiple(() =>
        {
            Assert.That(legacy.OwnershipConflict, Is.Null);
            Assert.That(contacts.OwnershipConflict, Is.Not.Null);
            Assert.That(contacts.OwnershipConflict, Does.Not.Contain("a/billing"), "the conflict message is sanitized");
        });
    }

    [Test]
    public void ToDescriptor_maps_a_record_holding_no_role_bindings_to_empty()
    {
        var descriptor = AppsControlMapping.ToDescriptor(
            AppsControlHarness.Manifest(),
            Provenance,
            AppLifecycleState.Installed,
            AppsControlHarness.Record(AppRegistryLifecycleState.Installed, bindings: []));

        Assert.That(descriptor.RoleBindings, Is.Empty);
        Assert.That(descriptor.Ceiling, Is.Not.Null, "a record with no bindings still carries its ceiling");
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
