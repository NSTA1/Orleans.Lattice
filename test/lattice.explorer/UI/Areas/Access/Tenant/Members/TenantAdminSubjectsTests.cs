using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Members;

/// <summary>
/// Issue #4162: the reader of a tenant's administrator entries, which the Members
/// page lists read-only as implicit members. Absent or failing, its answer is
/// unknown, never an empty list.
/// </summary>
[TestFixture]
public sealed class TenantAdminSubjectsTests
{
    [Test]
    public async Task The_entries_are_read_in_ordinal_order()
    {
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        access.ListAdminSubjectsAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantAdminSubjectReport { TenantId = "acme", Subjects = ["zed", "Ann", "bob"] });

        var subjects = await Reader(access).ReadAsync("acme", CancellationToken.None);

        Assert.That(subjects, Is.EqualTo(new[] { "Ann", "bob", "zed" }));
    }

    [Test]
    public async Task A_head_that_serves_no_facade_answers_unknown()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        Assert.That(await new TenantAdminSubjects(provider).ReadAsync("acme", CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task ReadAsync_unresolvable_facade_answers_unknown()
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton<ILatticeTenantAccessAdmin>(ShellFacades.Key,
            (_, _) => throw new InvalidOperationException("Missing facade dependency"));
        using var provider = services.BuildServiceProvider();

        Assert.That(await new TenantAdminSubjects(provider).ReadAsync("acme", CancellationToken.None), Is.Null);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task ReadAsync_missing_report_or_subjects_answers_unknown(bool missingReport)
    {
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        access.ListAdminSubjectsAsync("acme", Arg.Any<CancellationToken>())
            .Returns(missingReport ? null! : new TenantAdminSubjectReport { TenantId = "acme", Subjects = null! });

        Assert.That(await Reader(access).ReadAsync("acme", CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task ReadAsync_empty_subjects_are_known_and_forward_the_requested_tenant_and_token()
    {
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        using var cancellation = new CancellationTokenSource();
        access.ListAdminSubjectsAsync("globex", cancellation.Token)
            .Returns(new TenantAdminSubjectReport { TenantId = "globex", Subjects = [] });

        var subjects = await Reader(access).ReadAsync("globex", cancellation.Token);

        Assert.That(subjects, Is.Not.Null.And.Empty);
        await access.Received(1).ListAdminSubjectsAsync("globex", cancellation.Token);
    }

    [Test]
    public async Task ReadAsync_reads_again_and_sorts_a_copy_without_mutating_the_report()
    {
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        string[] original = ["zed", "Ann", "bob"];
        access.ListAdminSubjectsAsync("acme", Arg.Any<CancellationToken>()).Returns(
            new TenantAdminSubjectReport { TenantId = "acme", Subjects = original },
            new TenantAdminSubjectReport { TenantId = "acme", Subjects = ["new-admin"] });
        var reader = Reader(access);

        var first = await reader.ReadAsync("acme", CancellationToken.None);
        var second = await reader.ReadAsync("acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new[] { "Ann", "bob", "zed" }));
            Assert.That(first, Is.Not.SameAs(original));
            Assert.That(original, Is.EqualTo(new[] { "zed", "Ann", "bob" }));
            Assert.That(second, Is.EqualTo(new[] { "new-admin" }));
        });
        await access.Received(2).ListAdminSubjectsAsync("acme", CancellationToken.None);
    }

    [Test]
    public async Task A_refused_read_answers_unknown()
    {
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        access.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantAdminSubjectReport>(new LatticeAuthorizationDeniedException("no")));

        Assert.That(await Reader(access).ReadAsync("acme", CancellationToken.None), Is.Null);
    }

    [Test]
    public void A_cancellation_is_not_swallowed()
    {
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        access.ListAdminSubjectsAsync(default!, default).ReturnsForAnyArgs(
            Task.FromException<TenantAdminSubjectReport>(new OperationCanceledException()));

        Assert.That(() => Reader(access).ReadAsync("acme", CancellationToken.None), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void Missing_arguments_are_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantAdminSubjects(null!), Throws.ArgumentNullException);
            Assert.That(() => Reader(Substitute.For<ILatticeTenantAccessAdmin>()).ReadAsync(null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }

    private static TenantAdminSubjects Reader(ILatticeTenantAccessAdmin access)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(ShellFacades.Key, access);
        return new TenantAdminSubjects(services.BuildServiceProvider());
    }
}
