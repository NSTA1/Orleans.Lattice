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
    public async Task A_group_stored_here_is_a_member_source_unless_the_cluster_requires_directory_groups()
    {
        var admin = new FakeAuthAdmin().WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User).WithGroup("data-stewards");

        var stored = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "data-stewards", DirectoryPrincipalKind.Group);
        var neither = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "ghost", DirectoryPrincipalKind.Group);
        var required = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model with { DirectoryValidationRequired = true }, "data-stewards", DirectoryPrincipalKind.Group);
        var user = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "data-stewards", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.Null);
            Assert.That(neither, Is.EqualTo("ghost is not a group in the identity directory (Microsoft Entra ID) or a group stored here."));
            Assert.That(required, Is.EqualTo("data-stewards is not a group in the identity directory (Microsoft Entra ID)."));
            Assert.That(user, Is.EqualTo("data-stewards is not a user in the identity directory (Microsoft Entra ID)."), "a stored group is never a user");
        });
    }

    [Test]
    public async Task A_reserved_tenant_group_id_is_refused_with_or_without_a_directory()
    {
        var admin = new FakeAuthAdmin().WithGroup("t/acme/ops");

        var member = await AccessPrincipalValidation.ValidateAsync(admin, admin.Model, "t/acme/ops", DirectoryPrincipalKind.Group);
        var created = await AccessPrincipalValidation.ValidateNewGroupAsync(admin, null, "t/acme/ops");

        Assert.Multiple(() =>
        {
            Assert.That(member, Is.EqualTo(AccessPrincipalValidation.TenantGroupMessage("t/acme/ops")));
            Assert.That(created, Is.EqualTo(AccessPrincipalValidation.TenantGroupMessage("t/acme/ops")));
            Assert.That(admin.Calls, Is.Empty, "nothing is looked up for a reserved id");
        });
    }

    [Test]
    public async Task A_new_group_must_not_be_a_directory_user_and_must_be_a_directory_group_only_where_required()
    {
        var admin = new FakeAuthAdmin()
            .WithPrincipal("alice", "Alice", DirectoryPrincipalKind.User)
            .WithPrincipal("ops", "Operations", DirectoryPrincipalKind.Group);
        var required = admin.Model with { DirectoryValidationRequired = true };

        var directoryGroup = await AccessPrincipalValidation.ValidateNewGroupAsync(admin, admin.Model, "ops");
        var storedOnly = await AccessPrincipalValidation.ValidateNewGroupAsync(admin, admin.Model, "data-stewards");
        var user = await AccessPrincipalValidation.ValidateNewGroupAsync(admin, admin.Model, "alice");
        var notRequiredDirectoryGroup = await AccessPrincipalValidation.ValidateNewGroupAsync(admin, required, "data-stewards");
        var noModel = await AccessPrincipalValidation.ValidateNewGroupAsync(admin, null, "anything");

        Assert.Multiple(() =>
        {
            Assert.That(directoryGroup, Is.Null);
            Assert.That(storedOnly, Is.Null);
            Assert.That(user, Is.EqualTo("alice is a user in the identity directory (Microsoft Entra ID), not a group."));
            Assert.That(notRequiredDirectoryGroup,
                Is.EqualTo("data-stewards is not a group in the identity directory (Microsoft Entra ID), and this cluster stores only directory groups."));
            Assert.That(noModel, Is.Null);
            Assert.That(async () => await AccessPrincipalValidation.ValidateNewGroupAsync(null!, null, "x"), Throws.ArgumentNullException);
            Assert.That(async () => await AccessPrincipalValidation.ValidateNewGroupAsync(admin, null, null!), Throws.ArgumentNullException);
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
