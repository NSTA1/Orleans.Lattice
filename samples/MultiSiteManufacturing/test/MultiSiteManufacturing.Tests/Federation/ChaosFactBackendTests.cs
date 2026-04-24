using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// Verifies the <see cref="ChaosFactBackend"/> decorator: nominal
/// pass-through, transient failure injection, write amplification, and
/// read-path pass-through.
/// </summary>
[TestFixture]
public class ChaosFactBackendTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Emit_passes_through_when_config_is_nominal()
    {
        var baseline = _fixture.NewBaselineBackend();
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory);
        // Ensure the grain is nominal (fresh activations already are,
        // but be explicit for test isolation when keys collide).
        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(BackendChaosConfig.Nominal);

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-96001");
        await decorator.EmitAsync(Step(serial, 1, ProcessStage.Forge, ProcessSite.OhioForge));

        var facts = await baseline.GetFactsAsync(serial);
        Assert.That(facts, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Emit_throws_ChaosTransientFailureException_when_fault_rate_is_one()
    {
        var baseline = _fixture.NewBaselineBackend();
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory);
        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(new BackendChaosConfig { TransientFailureRate = 1.0 });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-96002");

        Assert.ThrowsAsync<ChaosTransientFailureException>(() =>
            decorator.EmitAsync(Step(serial, 1, ProcessStage.Forge, ProcessSite.OhioForge)));

        // Reset for neighbouring tests that share the same backend name.
        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(BackendChaosConfig.Nominal);

        var facts = await baseline.GetFactsAsync(serial);
        Assert.That(facts, Is.Empty, "Failed emit must not reach the inner backend.");
    }

    [Test]
    public async Task Emit_applies_write_amplification_when_rate_is_one()
    {
        var baseline = _fixture.NewBaselineBackend();
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory);
        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(new BackendChaosConfig { WriteAmplificationRate = 1.0 });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-96003");
        var fact = Step(serial, 1, ProcessStage.Forge, ProcessSite.OhioForge);

        await decorator.EmitAsync(fact);

        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(BackendChaosConfig.Nominal);

        var facts = await baseline.GetFactsAsync(serial);
        // Baseline appends on every EmitAsync regardless of fact id,
        // so write amplification surfaces as a duplicate fact — exactly
        // the scenario downstream dedup logic must tolerate.
        Assert.That(facts, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Read_paths_are_unaffected_by_chaos_config()
    {
        var baseline = _fixture.NewBaselineBackend();
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory);

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-96004");
        await baseline.EmitAsync(Step(serial, 1, ProcessStage.Forge, ProcessSite.OhioForge));

        // Turn on every knob — reads must still pass through unchanged.
        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(new BackendChaosConfig
            {
                JitterMsMin = 0,
                JitterMsMax = 0,
                TransientFailureRate = 1.0,
                WriteAmplificationRate = 1.0,
            });

        var state = await decorator.GetStateAsync(serial);
        var facts = await decorator.GetFactsAsync(serial);
        var parts = await decorator.ListPartsAsync();

        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(BackendChaosConfig.Nominal);

        Assert.Multiple(() =>
        {
            Assert.That(state, Is.EqualTo(ComplianceState.Nominal));
            Assert.That(facts, Has.Count.EqualTo(1));
            Assert.That(parts, Does.Contain(serial));
        });
    }

    [Test]
    public async Task Name_is_inherited_from_inner_backend()
    {
        var baseline = _fixture.NewBaselineBackend();
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory);
        await Task.CompletedTask;
        Assert.That(decorator.Name, Is.EqualTo("baseline"));
    }

    [Test]
    public async Task Emit_shuffles_buffered_facts_when_reorder_window_is_configured()
    {
        var baseline = _fixture.NewBaselineBackend();
        // Seed-2 produces a deterministic shuffle that is not the identity
        // permutation for N=5 under Fisher–Yates, so arrival order at the
        // inner backend differs from emission order — that's the whole
        // point of the reorder buffer.
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory, () => new Random(2));

        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(new BackendChaosConfig { ReorderWindowMs = 50 });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-96010");

        // Emit facts tagged 1..5 concurrently so every call joins the
        // same pending batch (first call opens the 50 ms window).
        var tasks = new Task[5];
        for (var i = 0; i < 5; i++)
        {
            var stage = (ProcessStage)(i % Enum.GetValues<ProcessStage>().Length);
            tasks[i] = decorator.EmitAsync(
                Step(serial, i + 1, stage, ProcessSite.OhioForge));
        }
        await Task.WhenAll(tasks);

        // Reset for neighbouring tests.
        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(BackendChaosConfig.Nominal);

        var stored = await baseline.GetFactsAsync(serial);
        Assert.That(stored, Has.Count.EqualTo(5), "All buffered facts must be flushed.");

        // Storage order must be a permutation (not necessarily the
        // emission order) of the emitted HLCs.
        var storedHlcs = stored.Select(f => f.Hlc).ToList();
        var distinctHlcs = storedHlcs.Distinct().Count();
        Assert.That(distinctHlcs, Is.EqualTo(5), "Every buffered fact must be persisted exactly once.");

        var isInHlcOrder = storedHlcs
            .Zip(storedHlcs.Skip(1), (a, b) => a < b)
            .All(x => x);
        Assert.That(
            isInHlcOrder, Is.False,
            "Seeded RNG should produce a non-identity shuffle — if this ever flakes, pick a different deterministic seed.");
    }

    [Test]
    public async Task Emit_with_reorder_window_still_honours_transient_failure_rate()
    {
        var baseline = _fixture.NewBaselineBackend();
        var decorator = new ChaosFactBackend(baseline, _fixture.GrainFactory);

        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(new BackendChaosConfig
            {
                ReorderWindowMs = 25,
                TransientFailureRate = 1.0,
            });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-96011");

        Assert.ThrowsAsync<ChaosTransientFailureException>(() =>
            decorator.EmitAsync(Step(serial, 1, ProcessStage.Forge, ProcessSite.OhioForge)));

        await _fixture.GrainFactory.GetGrain<IBackendChaosGrain>(baseline.Name)
            .ConfigureAsync(BackendChaosConfig.Nominal);

        var facts = await baseline.GetFactsAsync(serial);
        Assert.That(facts, Is.Empty, "Failed emit from the buffered-flush path must not reach the inner backend.");
    }
}
