using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for the Access-area directory view value types the membership
/// service folds a directory read into: the cached unavailable / safe snapshots
/// and the <see cref="AccessModelView.FromDescriptor(AccessModelDescriptor)"/>
/// mapping and its null guard.
/// </summary>
[TestFixture]
public sealed class DirectoryViewTests
{
    [Test]
    public void DirectorySearchView_Unavailable_is_a_clean_empty_success()
    {
        var view = DirectorySearchView.Unavailable;

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.Available, Is.False);
            Assert.That(view.Principals, Is.Empty);
            Assert.That(view.NextPageToken, Is.Null);
        });
    }

    [Test]
    public void AccessModelView_Unavailable_is_a_failed_unknown_snapshot()
    {
        var view = AccessModelView.Unavailable;

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.False);
            Assert.That(view.Status, Is.EqualTo(AccessOperationStatus.Failed));
            Assert.That(view.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Unknown));
            Assert.That(view.DirectoryAvailable, Is.False);
        });
    }

    [Test]
    public void AccessModelView_FromDescriptor_maps_every_field()
    {
        var view = AccessModelView.FromDescriptor(new AccessModelDescriptor
        {
            AuthenticationMode = AccessAuthenticationMode.Basic,
            RulesEnforced = true,
            DirectoryAvailable = true,
            DirectoryProviderId = "static",
            DirectoryExplanation = "Pick from the roster.",
            LocalMembershipEffective = true,
        });

        Assert.Multiple(() =>
        {
            Assert.That(view.IsSuccess, Is.True);
            Assert.That(view.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Basic));
            Assert.That(view.RulesEnforced, Is.True);
            Assert.That(view.DirectoryAvailable, Is.True);
            Assert.That(view.DirectoryProviderId, Is.EqualTo("static"));
            Assert.That(view.DirectoryExplanation, Is.EqualTo("Pick from the roster."));
            Assert.That(view.LocalMembershipEffective, Is.True);
        });
    }

    [Test]
    public void AccessModelView_FromDescriptor_null_descriptor_throws()
    {
        Assert.That(() => AccessModelView.FromDescriptor(null!), Throws.ArgumentNullException);
    }
}
