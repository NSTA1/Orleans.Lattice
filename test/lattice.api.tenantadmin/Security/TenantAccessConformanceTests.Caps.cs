using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin.Tests.Security;

/// <summary>
/// Concurrent cap cases (D13): racing additions at one below a cap never leave the
/// tenant over it. Each cap is a read-check-write kept by verify-and-compensate, so the
/// invariant under test holds for every interleaving: once the racers finish, the count
/// is within the cap and equals the prefill plus exactly the additions that reported
/// success (a refused racer withdrew its own write, and no admitted one was lost). The
/// member-set cap writes the tenant registry record, whose bounded optimistic-concurrency
/// retry can be exhausted by eight writers to one record; such a racer fails closed with
/// nothing written and is counted as refused.
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

        var admitted = (await RaceAsync(Admin, i => _fixture.Directory.UpsertGroupAsync(t.Value, new TenantGroupDescriptor { Name = $"race{i}" }))).Admitted;

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

        var admitted = (await RaceAsync(Admin, i => _fixture.Directory.AddGroupMemberAsync(t.Value, "eng", $"cap-e-race{i}"))).Admitted;

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

        var outcome = await RaceAsync(
            Admin, i => _fixture.Directory.AddMemberAsync(t.Value, $"cap-m-race{i}"), registryRetryExhaustionRefuses: true);

        // A racer refused by registry retry exhaustion counts as refused only if its
        // write never landed. PutMergeAsync throws only after every conditional write
        // lost, so an exhaustion on the add itself leaves nothing; an exhaustion on the
        // compensating withdrawal would leave the entry behind, which is not a refusal,
        // and is named here rather than folded into the count.
        TenantRecord record;
        using (LatticeSystemOrigin.Enter())
        {
            record = (await _fixture.Registry.GetAsync(t))!;
        }

        Assert.That(
            outcome.Exhausted.Where(i => record.HasMemberSubject($"cap-m-race{i}")),
            Is.Empty,
            "a racer that failed with TenantRegistryConcurrencyException left its entry in the member set "
            + "(its compensating withdrawal, not its add, exhausted the registry retry budget)");

        await AssertWithinCapAsync(Admin, t, outcome.Admitted, posture => posture.MemberSubjects);
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

        var admitted = (await RaceAsync(Admin, i => _fixture.Policy.PutRuleAsync(t.Value, TreeRule($"race{i}", Admin, $"race{i}")))).Admitted;

        await AssertWithinCapAsync(Admin, t, admitted, posture => posture.TenantRules);
    }

    /// <summary>
    /// Starts <see cref="Racers"/> additions together as <paramref name="admin"/> and
    /// reports how many were admitted. A refusal must be the cap's; anything else fails.
    /// When <paramref name="registryRetryExhaustionRefuses"/> is set, a racer that throws
    /// <see cref="TenantRegistryConcurrencyException"/> (the tenant registry's bounded
    /// optimistic-concurrency retry, exhausted by competing writes to one tenant record)
    /// is also counted as refused, and its index is reported so the caller can confirm
    /// its write did not land. Only the member-set cap writes the tenant record; the
    /// group, edge and rule caps write the membership and policy trees, which have no
    /// bounded retry, so they leave the flag off and such an exception fails the test.
    /// </summary>
    private static async Task<RaceOutcome> RaceAsync(string admin, Func<int, Task> add, bool registryRetryExhaustionRefuses = false)
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
                    return RacerResult.Admitted;
                }
                catch (LatticeQuotaExceededException)
                {
                    return RacerResult.Refused;
                }
                catch (TenantRegistryConcurrencyException) when (registryRetryExhaustionRefuses)
                {
                    return RacerResult.Exhausted;
                }
            }
        })).ToArray();

        start.SetResult();
        var results = await Task.WhenAll(racers);
        return new RaceOutcome(
            results.Count(r => r == RacerResult.Admitted),
            Enumerable.Range(0, Racers).Where(i => results[i] == RacerResult.Exhausted).ToArray());
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

    private enum RacerResult
    {
        Admitted,
        Refused,
        Exhausted,
    }

    /// <summary>A race's admitted count, and the indices of racers refused by registry retry exhaustion.</summary>
    private sealed record RaceOutcome(int Admitted, IReadOnlyList<int> Exhausted);
}
