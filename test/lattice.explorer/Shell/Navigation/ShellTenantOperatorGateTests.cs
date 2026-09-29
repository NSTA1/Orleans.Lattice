using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// The Shell's platform-operator gate: a caller is an operator exactly when the
/// Access area proves visible for it, and every other answer, a fault, a
/// time-out or the area's absence reads as "not an operator".
/// </summary>
[TestFixture]
public sealed class ShellTenantOperatorGateTests
{
    [Test]
    public async Task A_visible_access_area_proves_operator_standing()
    {
        var gate = Gate(new FakeArea("access", "Access") { Availability = _ => ValueTask.FromResult(AreaAvailability.Visible) });

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.True);
    }

    [Test]
    public async Task A_hidden_access_area_is_not_operator_standing()
    {
        var gate = Gate(new FakeArea("access", "Access") { Availability = _ => ValueTask.FromResult(AreaAvailability.Hidden) });

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task An_unavailable_access_area_is_not_operator_standing()
    {
        var gate = Gate(new FakeArea("access", "Access") { Availability = _ => ValueTask.FromResult(AreaAvailability.Unavailable("Sign in.")) });

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task A_faulted_probe_is_not_operator_standing()
    {
        var gate = Gate(new FakeArea("access", "Access") { Availability = _ => throw new InvalidOperationException("boom") });

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task No_access_area_is_not_operator_standing()
    {
        var gate = Gate(new FakeArea("data", "Data") { Availability = _ => ValueTask.FromResult(AreaAvailability.Visible) });

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task A_probe_that_outlasts_the_availability_timeout_is_not_operator_standing()
    {
        var area = new FakeArea("access", "Access") { Availability = _ => new ValueTask<AreaAvailability>(new TaskCompletionSource<AreaAvailability>().Task) };
        var gate = Gate(area, new ExplorerChromeOptions { AvailabilityTimeout = TimeSpan.FromMilliseconds(20) }, TimeProvider.System);

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public void The_callers_cancellation_propagates()
    {
        var gate = Gate(new FakeArea("access", "Access") { Availability = _ => ValueTask.FromResult(AreaAvailability.Visible) });

        Assert.That(async () => await gate.IsPlatformOperatorAsync(new CancellationToken(canceled: true)), Throws.InstanceOf<OperationCanceledException>());
    }

    private static ShellTenantOperatorGate Gate(FakeArea area, ExplorerChromeOptions? options = null, TimeProvider? time = null)
    {
        var services = new ServiceCollection().AddSingleton<IExplorerArea>(area).BuildServiceProvider();
        return new ShellTenantOperatorGate(services, options ?? new ExplorerChromeOptions(), time ?? TimeProvider.System);
    }
}
