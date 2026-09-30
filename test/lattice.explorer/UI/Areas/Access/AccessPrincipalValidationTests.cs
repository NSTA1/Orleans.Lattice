using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// The fail-closed directory pre-check and the circuit's memoised access model.
/// </summary>
[TestFixture]
public sealed class AccessPrincipalValidationTests
{
    [Test]
    public async Task Without_a_directory_every_id_is_accepted_unvalidated()
    {
        var admin = new FakeAuthAdmin();

        var withModel = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "anyone", DirectoryPrincipalKind.User);
        var withoutModel = await AccessPrincipalValidation.ValidateAsync(admin, null, "anyone", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(withModel, Is.Null);
            Assert.That(withoutModel, Is.Null);
            Assert.That(admin.Calls, Is.Empty, "no directory is asked when none is configured");
        });
    }

    [Test]
    public async Task With_a_directory_an_id_must_resolve_to_the_expected_kind()
    {
        var admin = new FakeAuthAdmin().WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User);

        var unresolved = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "ghost", DirectoryPrincipalKind.User);
        var mismatch = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "alice", DirectoryPrincipalKind.Group);
        var match = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "alice", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(unresolved, Is.EqualTo("ghost is not a user in the identity directory (Microsoft Entra ID)."));
            Assert.That(mismatch, Is.EqualTo("alice is a user in the identity directory (Microsoft Entra ID), not a group."));
            Assert.That(match, Is.Null);
        });
    }

    [Test]
    [TestCase("static", "static roster")]
    [TestCase("entra", "Microsoft Entra ID")]
    [TestCase("ldap", "ldap")]
    [TestCase("", "unnamed")]
    public void A_refusal_names_the_directory_it_consulted(string provider, string expected)
    {
        var model = new FakeAuthAdmin().Model with { DirectoryAvailable = true, DirectoryProviderId = provider };

        Assert.That(AccessPrincipalValidation.DirectoryName(model), Is.EqualTo(expected));
    }

    [Test]
    public async Task The_access_model_is_read_once_and_an_unreadable_one_is_unknown()
    {
        var admin = new FakeAuthAdmin();
        var catalog = new AccessCatalog(admin);

        var first = await catalog.GetAccessModelAsync(CancellationToken.None);
        var second = await catalog.GetAccessModelAsync(CancellationToken.None);

        var failing = new FakeAuthAdmin();
        failing.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), new InvalidOperationException());
        var unknown = await new AccessCatalog(failing).GetAccessModelAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first));
            Assert.That(admin.Calls.Count(call => call == nameof(FakeAuthAdmin.GetAccessModelAsync)), Is.EqualTo(1));
            Assert.That(unknown, Is.Null);
        });
    }

    [Test]
    public void A_cancelled_model_read_propagates()
    {
        var admin = new FakeAuthAdmin();
        admin.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), new OperationCanceledException());

        Assert.ThrowsAsync<OperationCanceledException>(() => new AccessCatalog(admin).GetAccessModelAsync(CancellationToken.None));
    }
}
