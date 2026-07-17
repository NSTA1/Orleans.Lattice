using Grpc.Core;
using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.Tests.Schema;

[TestFixture]
public class SchemaAdminFaultTests
{
    [Test]
    public void ToDenied_null_throws()
    {
        Assert.That(() => SchemaAdminFault.ToDenied(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ToDenied_permission_denied_translates_to_typed_denial()
    {
        var ex = new RpcException(new Status(StatusCode.PermissionDenied, "no schema authority"));

        var denied = SchemaAdminFault.ToDenied(ex);

        Assert.Multiple(() =>
        {
            Assert.That(denied, Is.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(denied.Message, Is.EqualTo("no schema authority"));
            Assert.That(denied.InnerException, Is.SameAs(ex));
        });
    }

    [Test]
    public void ToDenied_unauthenticated_translates_to_typed_denial()
    {
        var ex = new RpcException(new Status(StatusCode.Unauthenticated, "sign in required"));

        var denied = SchemaAdminFault.ToDenied(ex);

        Assert.Multiple(() =>
        {
            Assert.That(denied, Is.InstanceOf<LatticeAuthorizationDeniedException>());
            Assert.That(denied.InnerException, Is.SameAs(ex));
        });
    }

    [Test]
    public void DenialMessage_null_throws()
    {
        Assert.That(() => SchemaAdminFault.DenialMessage(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DenialMessage_uses_the_exception_message_when_present()
    {
        var ex = new LatticeAuthorizationDeniedException("you cannot do that");

        Assert.That(SchemaAdminFault.DenialMessage(ex), Is.EqualTo("you cannot do that"));
    }

    [Test]
    public void DenialMessage_falls_back_to_default_when_blank()
    {
        var ex = new LatticeAuthorizationDeniedException("   ");

        Assert.That(SchemaAdminFault.DenialMessage(ex), Is.EqualTo(SchemaAdminFault.DefaultDenialMessage));
    }

    [Test]
    public void FailureMessage_null_throws()
    {
        Assert.That(() => SchemaAdminFault.FailureMessage(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void FailureMessage_includes_status_code_and_detail()
    {
        var ex = new RpcException(new Status(StatusCode.Unavailable, "endpoint gone"));

        var message = SchemaAdminFault.FailureMessage(ex);

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("Unavailable"));
            Assert.That(message, Does.Contain("endpoint gone"));
        });
    }
}
