using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// <see cref="ShellFaultMessageTemplate"/>: a facade exception's message shape,
/// learned from the exception itself, recognises exactly that message and recovers
/// the arguments it was rendered with.
/// </summary>
[TestFixture]
public sealed class ShellFaultMessageTemplateTests
{
    [Test]
    public void A_one_argument_message_matches_and_recovers_its_argument()
    {
        var template = ShellFaultMessageTemplate.Create(static tenant => new TenantAccessAdministrationDisabledException(tenant).Message);

        var matched = template.TryMatch(new TenantAccessAdministrationDisabledException("globex").Message, out var tenant, out var second);

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(tenant, Is.EqualTo("globex"));
            Assert.That(second, Is.Empty);
        });
    }

    [Test]
    public void A_two_argument_message_recovers_both_whatever_order_they_appear_in()
    {
        // The last-admin message names the subject before the tenant.
        var template = ShellFaultMessageTemplate.Create(static (tenant, subject) => new TenantLastAdminSubjectException(tenant, subject).Message);

        var matched = template.TryMatch(new TenantLastAdminSubjectException("globex", "t/globex/admins").Message, out var tenant, out var subject);

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(tenant, Is.EqualTo("globex"));
            Assert.That(subject, Is.EqualTo("t/globex/admins"));
        });
    }

    [Test]
    public void Another_message_does_not_match()
    {
        var template = ShellFaultMessageTemplate.Create(static (tenant, operation) => new ReservedTenantOperationException(tenant, operation).Message);

        Assert.Multiple(() =>
        {
            Assert.That(template.TryMatch(null, out _, out _), Is.False);
            Assert.That(template.TryMatch(string.Empty, out _, out _), Is.False);
            Assert.That(template.TryMatch("The tenant is suspended.", out _, out _), Is.False);
            Assert.That(template.TryMatch(new TenantAccessAdministrationDisabledException("globex").Message, out _, out _), Is.False);
            Assert.That(template.TryMatch("Operation 'x' is not permitted", out var first, out var second), Is.False);
            Assert.That(first, Is.Empty);
            Assert.That(second, Is.Empty);
        });
    }

    [Test]
    public void A_message_that_does_not_carry_its_arguments_is_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ShellFaultMessageTemplate.Create(static _ => "fixed"), Throws.ArgumentException);
            Assert.That(() => ShellFaultMessageTemplate.Create((Func<string, string>)null!), Throws.ArgumentNullException);
            Assert.That(() => ShellFaultMessageTemplate.Create((Func<string, string, string>)null!), Throws.ArgumentNullException);
        });
    }
}
