using Bunit;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The roles section's way into changing an app's role bindings (issue #3884): an
/// <c>AppInstall</c> holder is linked to the installed version's review, where the change
/// is made; anyone else sees no such link.
/// </summary>
public sealed partial class AppPageTests
{
    [Test]
    public void An_app_install_holder_is_linked_from_the_roles_section_to_change_role_bindings()
    {
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/roles");

        var link = cut.Find("a[data-lt-rebind]");
        Assert.Multiple(() =>
        {
            Assert.That(link.TextContent, Is.EqualTo("Change role bindings"));
            Assert.That(link.GetAttribute("href"), Is.EqualTo("apps/catalogue/in-image/crm"));
        });
    }

    [Test]
    public void A_role_holder_without_app_install_is_not_linked_to_change_role_bindings()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/roles");

        Assert.That(cut.FindAll("a[data-lt-rebind]"), Is.Empty);
    }
}
