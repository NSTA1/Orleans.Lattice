namespace Orleans.Lattice.Tests.Formal;

internal sealed partial class RefinementDetectorMappingTests
{
    [TestCase("Yes: `WalSaturationSamplerTests.SampleOnceAsync_attributes_each_cause_in_priority_order_and_floor_only_None`.", false)]
    [TestCase("Yes: `BPlusLeafGrainTests.LeafPartialHydrationEnabled_defaults_to_on_so_an_upgraded_deployment_adopts_bounded_hydration`.", false)]
    [TestCase("None: no production detector.", true)]
    [TestCase("Partial: only one path is detected.", true)]
    [TestCase("None: no production detector (#4990).", false)]
    [TestCase("Partial: only one path is detected (#4990).", false)]
    public void Gap_citation_uses_the_coverage_verdict_not_a_detector_identifier(string detector, bool uncitedGap)
    {
        using var synthetic = SyntheticSpecModule.Create();
        var prefix = "| `Step` | `AtomicWriteGrain.RecordTerminalDecisionAsync`. | ";
        synthetic.Replace("Refinement.md",
            prefix + $"Yes: `{SyntheticSpecModule.Detector}`. |",
            prefix + detector + " |");
        var module = synthetic.Discover();
        Every_test_named_as_a_detector_exists(module);

        if (uncitedGap)
            Assert.Throws<AssertionException>(() => Every_row_reporting_a_gap_cites_an_issue(module));
        else
            Assert.DoesNotThrow(() => Every_row_reporting_a_gap_cites_an_issue(module));
    }
}
