using Bunit;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The rule editor's tenant capability scope (issue #5582): choosing it asks for a
/// tenant, offers Telemetry and nothing else, and saves a rule at
/// <see cref="LatticeScope.TenantCapability(TenantId)"/>.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessRuleEditorTenantCapabilityTests : AccessTestContext
{
    [Test]
    public void The_scope_list_offers_one_tenants_telemetry()
    {
        var cut = RenderEditor();

        Assert.That(
            AccessForms.Field(cut, "Scope").QuerySelectorAll("option").Select(option => option.GetAttribute("value")),
            Does.Contain(AccessRuleDraft.TenantCapabilityScope));
    }

    [Test]
    public void Choosing_the_tenant_capability_scope_asks_for_a_tenant_and_offers_only_telemetry()
    {
        var cut = RenderEditor();

        AccessForms.Choose(cut, "Scope", AccessRuleDraft.TenantCapabilityScope);

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.HasField(cut, "Tenant"), Is.True);
            Assert.That(AccessForms.HasField(cut, "Tree"), Is.False, "a tenant capability names no tree");
            Assert.That(cut.Find("[data-lt-operation=\"telemetry\"]").HasAttribute("disabled"), Is.False);
            Assert.That(cut.Find("[data-lt-operation=\"appinstall\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-operation=\"read\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("[data-lt-operation=\"admin\"]").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void A_tenant_telemetry_rule_is_saved_at_the_tenant_capability_scope()
    {
        LatticeAuthorizationRule? saved = null;
        var cut = RenderEditor(onSaved: rule => saved = rule);
        AccessForms.Type(cut, "Rule id", "acme-telemetry");
        AccessForms.Choose(cut, "Subject kind", "group");
        AccessForms.Type(cut, "Subject", "acme-observers");
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.TenantCapabilityScope);
        AccessForms.Type(cut, "Tenant", "acme");
        cut.Find("[data-lt-operation=\"telemetry\"]").Change(true);

        cut.Find("form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(saved, Is.Not.Null);
            Assert.That(saved!.Scope, Is.EqualTo(LatticeScope.TenantCapability(TenantId.Parse("acme"))));
            Assert.That(saved.Operations, Is.EqualTo(LatticeOperation.Telemetry));
            Assert.That(saved.Subject, Is.EqualTo(LatticeSubjectSelector.Group("acme-observers")));
        });
    }

    [Test]
    public void The_default_tenant_is_refused_before_anything_is_sent()
    {
        var cut = RenderEditor();
        AccessForms.Type(cut, "Rule id", "default-telemetry");
        AccessForms.Choose(cut, "Subject kind", "group");
        AccessForms.Type(cut, "Subject", "observers");
        AccessForms.Choose(cut, "Scope", AccessRuleDraft.TenantCapabilityScope);
        AccessForms.Type(cut, "Tenant", "default");
        cut.Find("[data-lt-operation=\"telemetry\"]").Change(true);

        cut.Find("form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(AccessForms.ErrorOf(cut, "Tenant"), Does.StartWith("The default tenant has no tenant capability scope"));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.PutRuleAsync)));
        });
    }

    private IRenderedComponent<AccessRuleEditor> RenderEditor(Action<LatticeAuthorizationRule>? onSaved = null) =>
        Render<AccessRuleEditor>(parameters => parameters
            .Add(editor => editor.Model, Admin.Model)
            .Add(editor => editor.OnSaved, saved => onSaved?.Invoke(saved)));
}
