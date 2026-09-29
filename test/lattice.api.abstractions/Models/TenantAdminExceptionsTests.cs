using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the hand-written constructors of the tenant-administration group's
/// typed exceptions, and pins the fail-closed contract each one encodes.
/// </summary>
/// <remarks>
/// <para>
/// These three types carry the tenant-administration facade's entire observable
/// refusal surface, and <b>every</b> transport binding has to map each of them to
/// its own fault vocabulary. A binding can only do that from the typed identity
/// and the captured context, so the captured properties are contract, not
/// diagnostics.
/// </para>
/// <para>
/// The serialization round-trip fixture instantiates typed exceptions through
/// <see cref="System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(System.Type)"/>,
/// which bypasses constructors entirely, so nothing else in the suite executes
/// the message-composing constructors or the property assignments below.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TenantAdminExceptionsTests
{
    [Test]
    public void TenantNotFound_tenantId_ctor_composes_message_and_captures_the_tenant()
    {
        var ex = new TenantNotFoundException("acme");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.Message, Does.Contain("acme"));
            Assert.That(ex.Message, Does.Contain("not registered"));
        });
    }

    [Test]
    public void TenantNotFound_message_ctor_uses_the_custom_message_and_keeps_the_tenant()
    {
        var ex = new TenantNotFoundException("acme", "explicit text");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.Message, Is.EqualTo("explicit text"));
            Assert.That(
                ex.Message,
                Does.Not.Contain("not registered"),
                "the custom overload must replace the composed message, not append to it");
        });
    }

    [Test]
    public void TenantAlreadyExists_tenantId_ctor_composes_message_and_captures_the_tenant()
    {
        var ex = new TenantAlreadyExistsException("acme");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.Message, Does.Contain("acme"));
            Assert.That(ex.Message, Does.Contain("already exists"));
        });
    }

    [Test]
    public void TenantAlreadyExists_message_ctor_uses_the_custom_message_and_keeps_the_tenant()
    {
        var ex = new TenantAlreadyExistsException("acme", "explicit text");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.Message, Is.EqualTo("explicit text"));
            Assert.That(ex.Message, Does.Not.Contain("already exists"));
        });
    }

    [Test]
    public void TenantAlreadyExists_and_TenantNotFound_are_distinct_types_not_one_generic_failure()
    {
        // Create is deliberately not an idempotent upsert: a caller has to be
        // able to tell "created" from "already present" without parsing prose.
        Assert.That(
            new TenantAlreadyExistsException("acme"),
            Is.Not.InstanceOf<TenantNotFoundException>());
        Assert.That(
            new TenantNotFoundException("acme"),
            Is.Not.InstanceOf<TenantAlreadyExistsException>());
    }

    [Test]
    public void ReservedTenantOperation_ctor_composes_message_and_captures_tenant_and_operation()
    {
        var ex = new ReservedTenantOperationException("default", "suspend");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TenantId, Is.EqualTo("default"));
            Assert.That(ex.Operation, Is.EqualTo("suspend"));
            Assert.That(ex.Message, Does.Contain("default"));
            Assert.That(ex.Message, Does.Contain("suspend"));
            Assert.That(ex.Message, Does.Contain("reserved default tenant"));
        });
    }

    [Test]
    public void ReservedTenantOperation_reports_the_rejected_operation_it_was_given()
    {
        // The operation is carried, not inferred: the same reserved-tenant guard
        // refuses several lifecycle verbs and a binding maps the message through.
        var suspend = new ReservedTenantOperationException("default", "suspend");
        var delete = new ReservedTenantOperationException("default", "delete");

        Assert.Multiple(() =>
        {
            Assert.That(suspend.Operation, Is.EqualTo("suspend"));
            Assert.That(delete.Operation, Is.EqualTo("delete"));
            Assert.That(suspend.Message, Does.Not.Contain("delete"));
            Assert.That(delete.Message, Does.Not.Contain("suspend"));
        });
    }

    [Test]
    public void Every_tenant_admin_exception_derives_directly_from_the_base_exception_type()
    {
        // Deriving directly from System.Exception is what keeps a same-silo deep
        // copy safe if any of these is ever marked serializable: Orleans
        // registers a copier for Exception but not for its BCL subclasses.
        Assert.Multiple(() =>
        {
            Assert.That(typeof(TenantNotFoundException).BaseType, Is.EqualTo(typeof(Exception)));
            Assert.That(typeof(TenantAlreadyExistsException).BaseType, Is.EqualTo(typeof(Exception)));
            Assert.That(typeof(ReservedTenantOperationException).BaseType, Is.EqualTo(typeof(Exception)));
        });
    }
}
