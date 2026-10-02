using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>
/// Concurrent cap cases (D13): racing additions at one below a cap never leave the
/// tenant over it. Each cap is a read-check-write kept by verify-and-compensate, so the
/// invariant under test holds for every interleaving: once the racers finish, the count
/// is within the cap and equals the prefill plus exactly the additions that reported
/// success (a refused racer withdrew its own write, and no admitted one was lost).
/// </summary>
public sealed partial class TenantAccessConformanceTests
{
    private const int Cap = 3;
    private const int Racers = 8;

    [Test]
    public async Task Caps_concurrent_group_creations_at_one_below_MaxGroups_leave_the_count_within_the_cap()
    {
        const string Admin = "cap-g-alice";
        var t = await _fixture.SeedTenantAsync("cap-g", Admin, new TenantQuotas { MaxKeys = 1000, MaxGroups = Cap });
        using (As(Admin))
        {
            for (var i = 0; i < Cap - 1; i++)
            {
                await _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = $"pre{i}" });
            }
        }

        var admitted = await RaceAsync(Admin, i => _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = $"race{i}" }));

        await AssertWithinCapAsync(Admin, t, admitted, posture => posture.Groups);
    }

    [Test]
    public async Task Caps_concurrent_edge_additions_at_one_below_MaxMembershipEdges_leave_the_count_within_the_cap()
    {
        const string Admin = "cap-e-alice";
        var t = await _fixture.SeedTenantAsync("cap-e", Admin, new TenantQuotas { MaxKeys = 1000, MaxMembershipEdges = Cap });
        using (As(Admin))
        {
            await _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = "eng" });
            for (var i = 0; i < Cap - 1; i++)
            {
                await _fixture.Directory.AddGroupMemberAsync(t.Value, "eng", $"cap-e-pre{i}");
            }
        }

        var admitted = await RaceAsync(Admin, i => _fixture.Directory.AddGroupMemberAsync(t.Value, "eng", $"cap-e-race{i}"));

        await AssertWithinCapAsync(Admin, t, admitted, posture => posture.MembershipEdges);
    }

    [Test]
    public async Task Caps_concurrent_member_additions_at_one_below_MaxMemberSubjects_leave_the_count_within_the_cap()
    {
        const string Admin = "cap-m-alice";
        var t = await _fixture.SeedTenantAsync("cap-m", Admin, new TenantQuotas { MaxKeys = 1000, MaxMemberSubjects = Cap });
        using (As(Admin))
        {
            for (var i = 0; i < Cap - 1; i++)
            {
                await _fixture.Directory.AddMemberAsync(t.Value, $"cap-m-pre{i}");
            }
        }

        var admitted = await RaceAsync(Admin, i => _fixture.Directory.AddMemberAsync(t.Value, $"cap-m-race{i}"));

        await AssertWithinCapAsync(Admin, t, admitted, posture => posture.MemberSubjects);
    }

    [Test]
    public async Task Caps_concurrent_rule_creations_at_one_below_MaxTenantRules_leave_the_count_within_the_cap()
    {
        const string Admin = "cap-r-alice";
        var t = await _fixture.SeedTenantAsync("cap-r", Admin, new TenantQuotas { MaxKeys = 1000, MaxTenantRules = Cap });
        using (As(Admin))
        {
            for (var i = 0; i < Cap - 1; i++)
            {
                await _fixture.Policy.PutRuleAsync(t.Value, TreeRule($"pre{i}", Admin, $"tree{i}"));
            }
        }

        var admitted = await RaceAsync(Admin, i => _fixture.Policy.PutRuleAsync(t.Value, TreeRule($"race{i}", Admin, $"race{i}")));

        await AssertWithinCapAsync(Admin, t, admitted, posture => posture.TenantRules);
    }

    /// <summary>
    /// Starts <see cref="Racers"/> additions together as <paramref name="admin"/> and
    /// returns how many were admitted. A refusal must be the cap's; anything else fails.
    /// </summary>
    private static async Task<int> RaceAsync(string admin, Func<int, Task> add)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var racers = Enumerable.Range(0, Racers).Select(i => Task.Run(async () =>
        {
            await start.Task;
            using (As(admin))
            {
                try
                {
                    await add(i);
                    return true;
                }
                catch (LatticeQuotaExceededException)
                {
                    return false;
                }
            }
        })).ToArray();

        start.SetResult();
        var outcomes = await Task.WhenAll(racers);
        return outcomes.Count(admittedOne => admittedOne);
    }

    private async Task AssertWithinCapAsync(
        string admin, TenantId tenant, int admitted, Func<TenantAccessPosture, TenantQuotaDimensionUsage> dimension)
    {
        TenantAccessPosture posture;
        using (As(admin))
        {
            posture = await _fixture.Policy.GetPostureAsync(tenant.Value);
        }

        var usage = dimension(posture);
        Assert.Multiple(() =>
        {
            Assert.That(usage.Limit, Is.EqualTo(Cap));
            Assert.That(usage.Usage, Is.LessThanOrEqualTo(Cap), "the racers never leave the tenant over its cap");
            Assert.That(usage.Usage, Is.EqualTo(Cap - 1 + admitted), "every admitted addition is kept and every refused one withdrawn");
            Assert.That(admitted, Is.LessThan(Racers), "the cap refused at least one racer");
        });
    }
}
