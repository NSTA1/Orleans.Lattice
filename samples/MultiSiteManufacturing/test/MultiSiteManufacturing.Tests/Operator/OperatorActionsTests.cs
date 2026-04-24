using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Operator;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Operator;

/// <summary>
/// Verifies the <see cref="OperatorActions"/> facade: each public method
/// emits the correct <see cref="Fact"/> kind, stamps a monotonic HLC via
/// <see cref="OperatorClock"/>, and lands at the canonical
/// <see cref="ProcessSite"/> for its step.
/// </summary>
[TestFixture]
public class OperatorActionsTests
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

    private (OperatorActions Actions, OperatorClock Clock, Host.Federation.FederationRouter Router) NewSut()
    {
        var (router, _, _) = _fixture.NewRouter();
        var clock = new OperatorClock();
        return (new OperatorActions(router, clock), clock, router);
    }

    [Test]
    public async Task CreatePartAsync_emits_ProcessStepCompleted_at_canonical_site()
    {
        var (actions, _, router) = NewSut();

        var serial = await actions.CreatePartAsync(
            new PartFamily("HPT-BLD-S1"), ProcessStage.Forge, OperatorId.Demo, default);

        var facts = await router.GetBackend("lattice").GetFactsAsync(serial);
        Assert.That(facts, Has.Count.EqualTo(1));
        var fact = facts[0];
        Assert.Multiple(() =>
        {
            Assert.That(fact, Is.InstanceOf<ProcessStepCompleted>());
            Assert.That(((ProcessStepCompleted)fact).Stage, Is.EqualTo(ProcessStage.Forge));
            Assert.That(fact.Site, Is.EqualTo(ProcessSite.OhioForge));
            Assert.That(fact.Operator, Is.EqualTo(OperatorId.Demo));
            Assert.That(serial.Value, Does.StartWith("HPT-BLD-S1-"));
        });
    }

    [Test]
    public async Task CompleteProcessStepAsync_maps_stage_to_canonical_site()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99101");

        await actions.CompleteProcessStepAsync(
            serial, ProcessStage.NDT, OperatorId.Demo, heatLot: "LOT-42");

        var facts = await router.GetBackend("lattice").GetFactsAsync(serial);
        var step = (ProcessStepCompleted)facts.Single();
        Assert.Multiple(() =>
        {
            Assert.That(step.Stage, Is.EqualTo(ProcessStage.NDT));
            Assert.That(step.Site, Is.EqualTo(ProcessSite.ToulouseNdtLab));
            Assert.That(step.HeatLot, Is.EqualTo("LOT-42"));
        });
    }

    [Test]
    public async Task RecordInspectionAsync_emits_InspectionRecorded()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99102");

        await actions.RecordInspectionAsync(
            serial, Inspection.CMM, InspectionOutcome.Pass,
            ProcessSite.ToulouseNdtLab, OperatorId.Demo);

        var fact = (InspectionRecorded)(await router.GetBackend("lattice").GetFactsAsync(serial)).Single();
        Assert.Multiple(() =>
        {
            Assert.That(fact.Inspection, Is.EqualTo(Inspection.CMM));
            Assert.That(fact.Outcome, Is.EqualTo(InspectionOutcome.Pass));
            Assert.That(fact.Site, Is.EqualTo(ProcessSite.ToulouseNdtLab));
        });
    }

    [Test]
    public async Task RaiseNonConformanceAsync_emits_NonConformanceRaised()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99103");

        await actions.RaiseNonConformanceAsync(
            serial, "NC-2025-0001", "CRACK", NcSeverity.Major,
            ProcessSite.CincinnatiMrb, OperatorId.Demo);

        var fact = (NonConformanceRaised)(await router.GetBackend("lattice").GetFactsAsync(serial)).Single();
        Assert.Multiple(() =>
        {
            Assert.That(fact.NcNumber, Is.EqualTo("NC-2025-0001"));
            Assert.That(fact.DefectCode, Is.EqualTo("CRACK"));
            Assert.That(fact.Severity, Is.EqualTo(NcSeverity.Major));
        });
    }

    [Test]
    public async Task DispositionMrbAsync_emits_MrbDisposition_at_cincinnati()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99104");

        await actions.DispositionMrbAsync(
            serial, "NC-2025-0002", MrbDispositionKind.UseAsIs, OperatorId.Demo);

        var fact = (MrbDisposition)(await router.GetBackend("lattice").GetFactsAsync(serial)).Single();
        Assert.Multiple(() =>
        {
            Assert.That(fact.NcNumber, Is.EqualTo("NC-2025-0002"));
            Assert.That(fact.Disposition, Is.EqualTo(MrbDispositionKind.UseAsIs));
            Assert.That(fact.Site, Is.EqualTo(ProcessSite.CincinnatiMrb));
        });
    }

    [Test]
    public async Task CompleteReworkAsync_emits_ReworkCompleted()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99105");

        await actions.CompleteReworkAsync(
            serial, "Weld repair", retestPassed: true, OperatorId.Demo);

        var fact = (ReworkCompleted)(await router.GetBackend("lattice").GetFactsAsync(serial)).Single();
        Assert.Multiple(() =>
        {
            Assert.That(fact.ReworkOperation, Is.EqualTo("Weld repair"));
            Assert.That(fact.RetestPassed, Is.True);
        });
    }

    [Test]
    public async Task SignOffFaiAsync_emits_FinalAcceptance_at_bristol()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99106");

        await actions.SignOffFaiAsync(
            serial, "FAI-42", "inspector:jdoe", certificateIssued: true, OperatorId.Demo);

        var fact = (FinalAcceptance)(await router.GetBackend("lattice").GetFactsAsync(serial)).Single();
        Assert.Multiple(() =>
        {
            Assert.That(fact.FaiReportId, Is.EqualTo("FAI-42"));
            Assert.That(fact.InspectorId, Is.EqualTo("inspector:jdoe"));
            Assert.That(fact.CertificateIssued, Is.True);
            Assert.That(fact.Site, Is.EqualTo(ProcessSite.BristolFai));
        });
    }

    [Test]
    public void OperatorClock_Next_is_strictly_monotonic_under_contention()
    {
        var clock = new OperatorClock();
        var bag = new System.Collections.Concurrent.ConcurrentBag<HybridLogicalClock>();

        Parallel.For(0, 500, _ => bag.Add(clock.Next()));

        var sorted = bag.ToList();
        sorted.Sort((a, b) => a.CompareTo(b));
        Assert.That(sorted, Has.Count.EqualTo(500));
        for (int i = 1; i < sorted.Count; i++)
        {
            Assert.That(sorted[i] > sorted[i - 1], Is.True,
                $"HLC at index {i} was not strictly greater than predecessor");
        }
    }

    [Test]
    public async Task OperatorActions_stamp_monotonic_hlc_across_calls()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99107");

        await actions.CompleteProcessStepAsync(serial, ProcessStage.Forge, OperatorId.Demo);
        await actions.CompleteProcessStepAsync(serial, ProcessStage.HeatTreat, OperatorId.Demo);
        await actions.CompleteProcessStepAsync(serial, ProcessStage.Machining, OperatorId.Demo);

        var factsRaw = await router.GetBackend("lattice").GetFactsAsync(serial);
        var facts = factsRaw.ToList();
        facts.Sort((a, b) => a.Hlc.CompareTo(b.Hlc));
        Assert.Multiple(() =>
        {
            Assert.That(facts[0].Hlc < facts[1].Hlc, Is.True);
            Assert.That(facts[1].Hlc < facts[2].Hlc, Is.True);
        });
    }

    [Test]
    public async Task RaceAsync_emits_NCR_Inspection_and_MRB_in_strict_HLC_order()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99300");

        var result = await actions.RaceAsync(serial, OperatorId.Demo);

        var facts = await router.GetBackend("lattice").GetFactsAsync(serial);

        // Sort by HLC — that's the lattice fold order — and assert the
        // trio kind/site contract.
        var inHlcOrder = facts.OrderBy(f => f.Hlc.WallClockTicks)
                              .ThenBy(f => f.Hlc.Counter)
                              .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(facts, Has.Count.EqualTo(3));
            Assert.That(result.Forwarded, Is.EqualTo(3));
            Assert.That(result.Held, Is.EqualTo(0));
            Assert.That(result.Site, Is.EqualTo(ProcessSite.CincinnatiMrb));

            Assert.That(inHlcOrder[0], Is.InstanceOf<NonConformanceRaised>());
            Assert.That(((NonConformanceRaised)inHlcOrder[0]).Severity, Is.EqualTo(NcSeverity.Minor));
            Assert.That(inHlcOrder[0].Site, Is.EqualTo(ProcessSite.ToulouseNdtLab));

            Assert.That(inHlcOrder[1], Is.InstanceOf<InspectionRecorded>());
            Assert.That(((InspectionRecorded)inHlcOrder[1]).Outcome, Is.EqualTo(InspectionOutcome.Pass));
            Assert.That(inHlcOrder[1].Site, Is.EqualTo(ProcessSite.StuttgartCmmLab));

            Assert.That(inHlcOrder[2], Is.InstanceOf<MrbDisposition>());
            Assert.That(((MrbDisposition)inHlcOrder[2]).Disposition, Is.EqualTo(MrbDispositionKind.UseAsIs));
            Assert.That(inHlcOrder[2].Site, Is.EqualTo(ProcessSite.CincinnatiMrb));

            // NCR and MRB carry the same ncNumber — that's the linkage that
            // lets UseAsIs actually demote the flag under HLC-ordered fold.
            Assert.That(
                ((MrbDisposition)inHlcOrder[2]).NcNumber,
                Is.EqualTo(((NonConformanceRaised)inHlcOrder[0]).NcNumber));
        });
    }

    [Test]
    public async Task RaceAsync_yields_Nominal_state_on_lattice_backend()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99301");

        await actions.RaceAsync(serial, OperatorId.Demo);

        // The three facts are emitted concurrently (see
        // <see cref="OperatorActions.RaceAsync"/>) so baseline's
        // arrival-order fold is race-dependent and may legitimately end
        // up Flagged. Lattice, however, folds in HLC order and is
        // deterministic: UseAsIs demotes Flagged back to Nominal.
        var latticeState = await router.GetBackend("lattice").GetStateAsync(serial);

        Assert.That(latticeState, Is.EqualTo(ComplianceState.Nominal));
    }

    [Test]
    public async Task FixAsync_emits_single_UseAsIs_MrbDisposition_at_CincinnatiMrb()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99400");

        var result = await actions.FixAsync(serial, OperatorId.Demo);

        var facts = await router.GetBackend("lattice").GetFactsAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(facts, Has.Count.EqualTo(1));
            Assert.That(result.Forwarded, Is.True);
            Assert.That(result.Held, Is.False);
            Assert.That(result.Site, Is.EqualTo(ProcessSite.CincinnatiMrb));

            Assert.That(facts[0], Is.InstanceOf<MrbDisposition>());
            Assert.That(((MrbDisposition)facts[0]).Disposition, Is.EqualTo(MrbDispositionKind.UseAsIs));
            Assert.That(facts[0].Site, Is.EqualTo(ProcessSite.CincinnatiMrb));
        });
    }

    [Test]
    public async Task FixAsync_restores_agreement_when_part_is_Flagged_on_baseline()
    {
        var (actions, _, router) = NewSut();
        var serial = new PartSerialNumber("HPT-BLD-S1-2025-99401");

        // Seed a Flagged state on both backends by racing the trio; under
        // a deterministic single-threaded emission the baseline fold may
        // still end up Flagged because arrival order is NCR-Pass-MRB and
        // that's a valid demotion path — so to be robust we explicitly
        // raise a minor NCR and leave it unresolved.
        await actions.RaiseNonConformanceAsync(
            serial,
            ncNumber: "NC-FIX-TEST",
            defectCode: "D1",
            severity: NcSeverity.Minor,
            site: ProcessSite.ToulouseNdtLab,
            op: OperatorId.Demo);

        var baselineBefore = await router.GetBackend("baseline").GetStateAsync(serial);
        var latticeBefore = await router.GetBackend("lattice").GetStateAsync(serial);
        Assert.That(baselineBefore, Is.EqualTo(ComplianceState.FlaggedForReview));
        Assert.That(latticeBefore, Is.EqualTo(ComplianceState.FlaggedForReview));

        // Apply the single-fact fix: UseAsIs demotes Flagged -> Nominal in
        // both folds (arrival-order and HLC-order) because a singleton
        // batch cannot be reordered.
        var result = await actions.FixAsync(serial, OperatorId.Demo);

        var baselineAfter = await router.GetBackend("baseline").GetStateAsync(serial);
        var latticeAfter = await router.GetBackend("lattice").GetStateAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(result.Forwarded, Is.True);
            Assert.That(baselineAfter, Is.EqualTo(ComplianceState.Nominal));
            Assert.That(latticeAfter, Is.EqualTo(ComplianceState.Nominal));
            Assert.That(baselineAfter, Is.EqualTo(latticeAfter),
                "Fix must restore baseline ↔ lattice agreement");
        });
    }
}
