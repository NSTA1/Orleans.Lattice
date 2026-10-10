using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

public sealed partial class LatticeTenantAdmissionControllerTests
{
    [Test]
    public async Task IsTreeCreateAdmitted_concurrent_checks_can_overshoot_but_next_live_check_refuses()
    {
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged,
            registry: RegistryWith(maxTreeCount: 1));
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var countCalls = 0;
        long registered = 0;
        ValueTask<long> Count(CancellationToken _)
        {
            countCalls++;
            return new ValueTask<long>(gate.Task);
        }

        var first = controller.IsTreeCreateAdmittedAsync(Acme, "one", Count).AsTask();
        var second = controller.IsTreeCreateAdmittedAsync(Acme, "two", Count).AsTask();
        Assert.That(countCalls, Is.EqualTo(2));
        gate.SetResult(registered);
        Assert.That(await first, Is.True);
        Assert.That(await second, Is.True);
        registered += 2;
        Assert.That(async () => await controller.IsTreeCreateAdmittedAsync(
            Acme, "three", _ => new ValueTask<long>(registered)),
            Throws.TypeOf<LatticeQuotaExceededException>());
    }
}
