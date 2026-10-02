using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.Policy.TenantPolicyTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Policy;

/// <summary>
/// Concurrent puts: the store holds every racer's write until all of them have
/// passed their checks, so each test exercises the interleaving the pre-write check
/// alone cannot defend. The assertions hold for every order in which the released
/// writes and re-scans then run.
/// </summary>
public sealed partial class LatticeTenantPolicyAdminTests
{
    [Test]
    public async Task Concurrent_puts_of_new_rules_at_the_cap_never_leave_the_cap_exceeded()
    {
        const int Cap = 2;
        var harness = new Harness(new TenantQuotas { MaxTenantRules = Cap });
        harness.Store.Seed(TenantRule(Tenant, "existing", "orders"));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Store.WriteGate = release.Task;
        var facade = harness.Create();
        var held = harness.Store.WaitForHeldWritesAsync(3);

        var puts = new[] { "r1", "r2", "r3" }
            .Select(id => facade.PutRuleAsync(Tenant, Draft(ruleId: id)))
            .ToArray();
        await held;
        release.SetResult();

        var outcomes = await Task.WhenAll(puts.Select(Outcome));

        var stored = harness.Store.All.Count(r => r.RuleId.StartsWith("tenant:acme:", StringComparison.Ordinal));
        Assert.Multiple(() =>
        {
            Assert.That(stored, Is.LessThanOrEqualTo(Cap), "the cap holds once the racers finish");
            Assert.That(outcomes.Count(o => o is null), Is.LessThanOrEqualTo(Cap - 1), "at most the remaining headroom succeeds");
            Assert.That(outcomes.Where(o => o is not null), Is.All.TypeOf<LatticeQuotaExceededException>(), "every other racer is refused by the cap");
            Assert.That(stored, Is.EqualTo(1 + outcomes.Count(o => o is null)), "exactly the successful racers' rules remain");
        });
    }

    [Test]
    public async Task Concurrent_puts_of_one_new_local_id_to_different_trees_leave_one_copy()
    {
        var harness = new Harness();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Store.WriteGate = release.Task;
        var facade = harness.Create();
        var held = harness.Store.WaitForHeldWritesAsync(2);

        var puts = new[]
        {
            facade.PutRuleAsync(Tenant, Draft(ruleId: "r1", treeName: "b-tree")),
            facade.PutRuleAsync(Tenant, Draft(ruleId: "r1", treeName: "a-tree")),
        };
        await held;
        release.SetResult();
        await Task.WhenAll(puts);

        var copies = harness.Store.All.Where(r => r.RuleId == "tenant:acme:r1").ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(copies, Has.Length.EqualTo(1), "racers settle on a single stored copy");
            Assert.That(copies.Single().Scope.TreeId, Is.EqualTo("t/acme/a-tree"), "the deterministic tie-break keeps the smallest tree id");
            Assert.That(harness.Store.TenantWriteOrigins, Is.All.True);
        });
    }

    [Test]
    public async Task A_put_that_finds_the_cap_exceeded_after_its_write_withdraws_only_its_own_rule()
    {
        // A sequential stand-in for a racer that landed between the check and the
        // write: the rule appears while this put is held, so its re-count is over the cap.
        var harness = new Harness(new TenantQuotas { MaxTenantRules = 1 });
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Store.WriteGate = release.Task;
        var facade = harness.Create();
        var held = harness.Store.WaitForHeldWritesAsync(1);

        var put = facade.PutRuleAsync(Tenant, Draft(ruleId: "late"));
        await held;
        harness.Store.Seed(TenantRule(Tenant, "racer", "invoices"));
        release.SetResult();

        Assert.That(async () => await put, Throws.TypeOf<LatticeQuotaExceededException>());
        Assert.That(harness.Store.All.Select(r => r.RuleId), Is.EqualTo(new[] { "tenant:acme:racer" }));
    }

    private static async Task<Exception?> Outcome(Task task)
    {
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
