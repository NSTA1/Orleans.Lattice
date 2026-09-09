using Orleans.Hosting;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the Orleans runtime contract that decides the COVERAGE of the leaf's
/// issue #2280 deactivation observation: <c>OnDeactivateAsync</c> does
/// <b>not</b> run when <c>OnActivateAsync</c> throws.
/// <para>
/// This is load-bearing, not incidental. Issue #2280's population is a cold WAL
/// replay that is CANCELLED before it finishes, and a cancelled replay leaves
/// <c>BPlusLeafGrain.OnActivateAsync</c> by throwing
/// <see cref="OperationCanceledException"/> - failures on that path propagate
/// deliberately (issue #1535). If the deactivation hook did not run for those
/// activations, an instrument sited only in that hook would report zero for the
/// entire population it was built to measure, at every rate of occurrence
/// including the highest. That is why the leaf carries a SECOND observation
/// site on the activation path, and why the deactivation histogram is
/// documented as a lower bound rather than a census.
/// </para>
/// <para>
/// The measurement is stated against a POSITIVE CONTROL. Without one, a zero
/// deactivation count is ambiguous between "the hook did not run" and "the hook
/// cannot be observed by this probe at all", so the control demonstrates the
/// counter fires on a graceful teardown before any absence is claimed from it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class BPlusLeafGrainActivationFailureHookContractTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<Configurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Deactivation_hook_runs_on_a_graceful_teardown()
    {
        // POSITIVE CONTROL for the two absence tests below. Establishes that
        // the probe's deactivation counter can fire at all, so a zero there is
        // a real absence rather than an unobservable one.
        const string key = "probe-graceful";
        var grain = _cluster.Client.GetGrain<IActivationFailureProbeGrain>(key);

        await grain.PingAsync();
        await grain.DeactivateSelfAsync();
        await WaitForDeactivationAsync(key);

        Assert.That(ActivationFailureProbeGrain.ActivationCount(key), Is.EqualTo(1));
        Assert.That(ActivationFailureProbeGrain.DeactivationCount(key), Is.EqualTo(1),
            "The probe must be able to observe its own graceful deactivation, otherwise the absences "
            + "asserted below prove nothing.");
    }

    [Test]
    public async Task Deactivation_hook_does_not_run_when_activation_throws()
    {
        const string key = "probe-" + ActivationFailureProbeGrain.FaultingKey;
        var grain = _cluster.Client.GetGrain<IActivationFailureProbeGrain>(key);

        Assert.That(async () => await grain.PingAsync(), Throws.Exception,
            "The failing activation must surface as a fault to the caller.");

        await WaitForDeactivationAsync(key);

        Assert.That(ActivationFailureProbeGrain.ActivationCount(key), Is.GreaterThanOrEqualTo(1),
            "The activation must have entered OnActivateAsync for the absence below to mean anything.");
        Assert.That(ActivationFailureProbeGrain.DeactivationCount(key), Is.Zero,
            "OnDeactivateAsync must NOT run when OnActivateAsync throws. An instrument sited only in "
            + "that hook is therefore structurally blind to failed activations, which is exactly the "
            + "population issue #2280 is about.");
    }

    [Test]
    public async Task Deactivation_hook_does_not_run_when_activation_is_cancelled()
    {
        // Cancellation is tested separately from a generic fault because the
        // runtime could plausibly treat a cancelled activation as an ordinary
        // teardown, and cancellation - not a generic fault - is issue #2280's
        // actual mechanism.
        const string key = "probe-" + ActivationFailureProbeGrain.CancellingKey;
        var grain = _cluster.Client.GetGrain<IActivationFailureProbeGrain>(key);

        Assert.That(async () => await grain.PingAsync(), Throws.Exception);

        await WaitForDeactivationAsync(key);

        Assert.That(ActivationFailureProbeGrain.ActivationCount(key), Is.GreaterThanOrEqualTo(1));
        Assert.That(ActivationFailureProbeGrain.DeactivationCount(key), Is.Zero,
            "A cancelled activation must not reach OnDeactivateAsync either, so the cold-replay "
            + "cancellation path is invisible to the deactivation hook.");
    }

    /// <summary>
    /// Deactivation is asynchronous with respect to the caller, so an immediate
    /// read could see a zero that the runtime was merely slow to produce. This
    /// waits for a non-zero count and only then gives up, so a reported zero
    /// has been given every chance to become non-zero.
    /// </summary>
    private static async Task WaitForDeactivationAsync(string key)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (ActivationFailureProbeGrain.DeactivationCount(key) > 0)
                return;
            await Task.Delay(100);
        }
    }

    private sealed class Configurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
            => siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
    }
}
