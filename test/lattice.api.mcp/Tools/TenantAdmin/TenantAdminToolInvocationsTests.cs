using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAdminToolInvocations"/>, the pure adapter layer
/// between the tenant-admin MCP tools and the <see cref="ILatticeTenantAdmin"/>
/// facade. Proves each of the four lifecycle operations delegates to the facade
/// and shapes the compact MCP DTO (stringifying the lifecycle status), that a null
/// facade is rejected, and that the fail-closed denial the facade gate raises
/// surfaces unchanged (the MCP layer adds no authorization path of its own). All
/// deterministic against a substituted facade - no cluster, no ordering-by-timing.
/// </summary>
[TestFixture]
public sealed class TenantAdminToolInvocationsTests
{
    [Test]
    public async Task Create_delegates_to_the_facade_and_shapes_the_result()
    {
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.CreateTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantCreationResult { TenantId = "acme", Status = TenantLifecycleStatus.Active });

        var result = await TenantAdminToolInvocations.CreateTenantAsync(admin, "acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
        });
        await admin.Received(1).CreateTenantAsync("acme", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Suspend_delegates_to_the_facade_and_shapes_the_result()
    {
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.SuspendTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusChangeResult
            {
                TenantId = "acme",
                PreviousStatus = TenantLifecycleStatus.Active,
                NewStatus = TenantLifecycleStatus.Suspended,
                Changed = true,
            });

        var result = await TenantAdminToolInvocations.SuspendTenantAsync(admin, "acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.PreviousStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(result.NewStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
            Assert.That(result.Changed, Is.True);
        });
    }

    [Test]
    public async Task Suspend_of_an_already_suspended_tenant_reports_no_change()
    {
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.SuspendTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusChangeResult
            {
                TenantId = "acme",
                PreviousStatus = TenantLifecycleStatus.Suspended,
                NewStatus = TenantLifecycleStatus.Suspended,
                Changed = false,
            });

        var result = await TenantAdminToolInvocations.SuspendTenantAsync(admin, "acme", CancellationToken.None);

        Assert.That(result.Changed, Is.False);
    }

    [Test]
    public async Task Resume_delegates_to_the_facade_and_shapes_the_result()
    {
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.ResumeTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusChangeResult
            {
                TenantId = "acme",
                PreviousStatus = TenantLifecycleStatus.Suspended,
                NewStatus = TenantLifecycleStatus.Active,
                Changed = true,
            });

        var result = await TenantAdminToolInvocations.ResumeTenantAsync(admin, "acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.PreviousStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
            Assert.That(result.NewStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(result.Changed, Is.True);
        });
    }

    [Test]
    public async Task Delete_delegates_to_the_facade_and_reports_the_cascaded_tree_count()
    {
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.DeleteTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 3 });

        var result = await TenantAdminToolInvocations.DeleteTenantAsync(admin, "acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.CascadedTreeCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task SetQuotas_delegates_to_the_facade_and_shapes_the_result()
    {
        var quotas = new TenantQuotasDescriptor
        {
            MaxBytes = 2_000_000,
            MaxKeys = 10_000,
            MaxOpsPerSecond = 500,
            BurstPercent = 15,
        };
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.SetTenantQuotasAsync("acme", quotas, Arg.Any<CancellationToken>())
            .Returns(new TenantQuotasUpdateResult { TenantId = "acme", Quotas = quotas });

        var result = await TenantAdminToolInvocations.SetTenantQuotasAsync(admin, "acme", quotas, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.MaxBytes, Is.EqualTo(2_000_000));
            Assert.That(result.MaxKeys, Is.EqualTo(10_000));
            Assert.That(result.MaxOpsPerSecond, Is.EqualTo(500));
            Assert.That(result.BurstPercent, Is.EqualTo(15));
            Assert.That(result.IsUnbounded, Is.False);
        });
        await admin.Received(1).SetTenantQuotasAsync("acme", quotas, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Unauthorized_caller_is_denied_fail_closed_on_every_operation()
    {
        var admin = Substitute.For<ILatticeTenantAdmin>();
        admin.CreateTenantAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantCreationResult>>(_ => throw new LatticeAuthorizationDeniedException());
        admin.SuspendTenantAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantStatusChangeResult>>(_ => throw new LatticeAuthorizationDeniedException());
        admin.ResumeTenantAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantStatusChangeResult>>(_ => throw new LatticeAuthorizationDeniedException());
        admin.DeleteTenantAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantDeletionResult>>(_ => throw new LatticeAuthorizationDeniedException());
        admin.SetTenantQuotasAsync(Arg.Any<string>(), Arg.Any<TenantQuotasDescriptor>(), Arg.Any<CancellationToken>())
            .Returns<Task<TenantQuotasUpdateResult>>(_ => throw new LatticeAuthorizationDeniedException());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await TenantAdminToolInvocations.CreateTenantAsync(admin, "acme", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await TenantAdminToolInvocations.SuspendTenantAsync(admin, "acme", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await TenantAdminToolInvocations.ResumeTenantAsync(admin, "acme", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await TenantAdminToolInvocations.DeleteTenantAsync(admin, "acme", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await TenantAdminToolInvocations.SetTenantQuotasAsync(admin, "acme", TenantQuotasDescriptor.Unbounded, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public void Null_admin_is_rejected_on_every_invocation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await TenantAdminToolInvocations.CreateTenantAsync(null!, "acme", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TenantAdminToolInvocations.SuspendTenantAsync(null!, "acme", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TenantAdminToolInvocations.ResumeTenantAsync(null!, "acme", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TenantAdminToolInvocations.DeleteTenantAsync(null!, "acme", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TenantAdminToolInvocations.SetTenantQuotasAsync(null!, "acme", TenantQuotasDescriptor.Unbounded, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }
}
