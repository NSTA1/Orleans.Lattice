using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAccessDataPurge"/>, the access-data purge step
/// of the tenant deletion pipeline (epic #4154, D12): it runs the tenant-tier rule
/// purge and then the tenant group purge under system origin, reports what each
/// removed, completes on a re-run after a crash between the two, treats the
/// reserved default tenant as a no-op, and fails closed when the stores it needs
/// are not resolvable.
/// </summary>
[TestFixture]
public sealed class TenantAccessDataPurgeTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    /// <summary>Records the order and origin of each step it serves.</summary>
    private sealed class Steps
    {
        public List<string> Calls { get; } = new();

        public List<bool> Origins { get; } = new();

        public List<TenantId> Tenants { get; } = new();

        public int Rules { get; set; } = 3;

        public TenantMembershipPurgeResult Membership { get; set; } = new(2, 5);

        public Exception? MembershipFault { get; set; }

        public TenantAccessDataPurge Create() => new(
            (tenant, _) =>
            {
                Record("rules", tenant);
                return Task.FromResult(Rules);
            },
            (tenant, _) =>
            {
                Record("membership", tenant);
                return MembershipFault is { } fault
                    ? Task.FromException<TenantMembershipPurgeResult>(fault)
                    : Task.FromResult(Membership);
            });

        private void Record(string step, TenantId tenant)
        {
            Calls.Add(step);
            Origins.Add(LatticeSystemOrigin.IsActive);
            Tenants.Add(tenant);
        }
    }

    [Test]
    public async Task PurgeAsync_purges_rules_then_membership_and_reports_both()
    {
        var steps = new Steps();

        var result = await steps.Create().PurgeAsync(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(new TenantAccessPurgeResult(RulesRemoved: 3, GroupsRemoved: 2, EdgesRemoved: 5)));
            Assert.That(steps.Calls, Is.EqualTo(new[] { "rules", "membership" }));
            Assert.That(steps.Tenants, Is.EqualTo(new[] { Acme, Acme }));
        });
    }

    [Test]
    public async Task PurgeAsync_runs_both_steps_under_system_origin_without_leaking_it()
    {
        var steps = new Steps();

        await steps.Create().PurgeAsync(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(steps.Origins, Is.EqualTo(new[] { true, true }));
            Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the purge's system-origin scope must not leak");
        });
    }

    [Test]
    public async Task PurgeAsync_rerun_after_a_crash_between_the_steps_completes()
    {
        var steps = new Steps { MembershipFault = new InvalidOperationException("crash between the purges") };

        Assert.That(
            async () => await steps.Create().PurgeAsync(Acme),
            Throws.InvalidOperationException.With.Message.EqualTo("crash between the purges"));
        Assert.That(LatticeSystemOrigin.IsActive, Is.False, "a faulted purge must not leak system origin");

        // The rules went before the crash, so the resumed run finds none and
        // finishes the groups.
        steps.MembershipFault = null;
        steps.Rules = 0;
        var resumed = await steps.Create().PurgeAsync(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(resumed, Is.EqualTo(new TenantAccessPurgeResult(0, 2, 5)));
            Assert.That(steps.Calls, Is.EqualTo(new[] { "rules", "membership", "rules", "membership" }));
        });
    }

    [Test]
    public async Task PurgeAsync_for_the_default_tenant_is_a_no_op()
    {
        var steps = new Steps();

        var result = await steps.Create().PurgeAsync(TenantId.Default);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(default(TenantAccessPurgeResult)));
            Assert.That(steps.Calls, Is.Empty);
        });
    }

    [Test]
    public void PurgeAsync_with_the_no_tenant_value_throws_before_any_step()
    {
        var steps = new Steps();

        Assert.That(async () => await steps.Create().PurgeAsync(default), Throws.ArgumentException);
        Assert.That(steps.Calls, Is.Empty);
    }

    [Test]
    public void PurgeAsync_observes_cancellation_before_any_step()
    {
        var steps = new Steps();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await steps.Create().PurgeAsync(Acme, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(steps.Calls, Is.Empty);
    }

    [Test]
    public void PurgeAsync_passes_the_cancellation_token_to_each_step()
    {
        using var cts = new CancellationTokenSource();
        var seen = new List<CancellationToken>();
        var purge = new TenantAccessDataPurge(
            (_, ct) =>
            {
                seen.Add(ct);
                return Task.FromResult(0);
            },
            (_, ct) =>
            {
                seen.Add(ct);
                return Task.FromResult(default(TenantMembershipPurgeResult));
            });

        Assert.That(async () => await purge.PurgeAsync(Acme, cts.Token), Throws.Nothing);
        Assert.That(seen, Is.EqualTo(new[] { cts.Token, cts.Token }));
    }

    [Test]
    public void Constructor_null_steps_throw()
    {
        Func<TenantId, CancellationToken, Task<int>> rules = (_, _) => Task.FromResult(0);
        Func<TenantId, CancellationToken, Task<TenantMembershipPurgeResult>> membership =
            (_, _) => Task.FromResult(default(TenantMembershipPurgeResult));

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantAccessDataPurge(null!, membership), Throws.ArgumentNullException);
            Assert.That(() => new TenantAccessDataPurge(rules, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void FromServices_null_provider_throws()
    {
        Assert.That(() => TenantAccessDataPurge.FromServices(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void FromServices_resolves_lazily_so_construction_never_touches_the_stores()
    {
        var services = Substitute.For<IServiceProvider>();

        _ = TenantAccessDataPurge.FromServices(services);

        Assert.That(services.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void FromServices_without_the_policy_rule_store_fails_closed_before_any_removal()
    {
        using var services = new ServiceCollection().BuildServiceProvider();

        var purge = TenantAccessDataPurge.FromServices(services);

        Assert.That(async () => await purge.PurgeAsync(Acme), Throws.InvalidOperationException);
    }

    [Test]
    public void FromServices_membership_step_fails_closed_when_the_directory_does_not_support_tenant_scope()
    {
        using var services = new ServiceCollection()
            .AddSingleton(Substitute.For<ILatticeMembershipDirectory>())
            .BuildServiceProvider();

        var purge = TenantAccessDataPurge.FromServices(services);

        Assert.That(
            async () => await purge.PurgeMembership(Acme, CancellationToken.None),
            Throws.InvalidOperationException.With.Message.Contains("tenant-scoped membership operations"));
    }
}
