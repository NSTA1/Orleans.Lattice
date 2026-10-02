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
