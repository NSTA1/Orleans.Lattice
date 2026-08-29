using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit tests for the tenant access-administration (admin-subject) contract model
/// types: the read-only <see cref="TenantAdminSubjectReport"/> projection, the
/// <see cref="TenantAdminSubjectChangeResult"/> mutation result, and the
/// <see cref="TenantLastAdminSubjectException"/> orphan guard. Pure value-shape
/// assertions - no cluster, no transport.
/// </summary>
[TestFixture]
public sealed class TenantAdminSubjectModelTests
{
    [Test]
    public void TenantAdminSubjectReport_carries_its_tenant_and_subjects()
    {
        var report = new TenantAdminSubjectReport
        {
            TenantId = "acme",
            Subjects = ["alice@example.com", "bob@example.com"],
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Subjects, Is.EqualTo(new[] { "alice@example.com", "bob@example.com" }));
        });
    }

    [Test]
    public void TenantAdminSubjectReport_supports_an_empty_subject_set() =>
        Assert.That(new TenantAdminSubjectReport { TenantId = "acme", Subjects = [] }.Subjects, Is.Empty);

    [Test]
    public void TenantAdminSubjectChangeResult_carries_its_change_and_resulting_set()
    {
        var result = new TenantAdminSubjectChangeResult
        {
            TenantId = "acme",
            SubjectId = "carol@example.com",
            Changed = true,
            Subjects = ["alice@example.com", "carol@example.com"],
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.SubjectId, Is.EqualTo("carol@example.com"));
            Assert.That(result.Changed, Is.True);
            Assert.That(result.Subjects, Is.EqualTo(new[] { "alice@example.com", "carol@example.com" }));
        });
    }

    [Test]
    public void TenantAdminSubjectChangeResult_records_an_idempotent_no_op()
    {
        var result = new TenantAdminSubjectChangeResult
        {
            TenantId = "acme",
            SubjectId = "alice@example.com",
            Changed = false,
            Subjects = ["alice@example.com"],
        };

        Assert.That(result.Changed, Is.False);
    }

    [Test]
    public void TenantAdminSubjectChangeResult_is_a_value_record()
    {
        var left = new TenantAdminSubjectChangeResult
        {
            TenantId = "acme", SubjectId = "carol@example.com", Changed = true, Subjects = [],
        };
        var right = left with { };

        Assert.That(left, Is.EqualTo(right));
    }

    [Test]
    public void TenantLastAdminSubjectException_carries_the_tenant_and_subject_ids()
    {
        var exception = new TenantLastAdminSubjectException("acme", "alice@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(exception.TenantId, Is.EqualTo("acme"));
            Assert.That(exception.SubjectId, Is.EqualTo("alice@example.com"));
            Assert.That(exception.Message, Does.Contain("acme"));
            Assert.That(exception.Message, Does.Contain("alice@example.com"));
            Assert.That(exception, Is.InstanceOf<Exception>());
            Assert.That(exception.GetType().BaseType, Is.EqualTo(typeof(Exception)),
                "a serializable-convention exception must derive directly from System.Exception");
        });
    }
}
