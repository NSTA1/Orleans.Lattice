namespace Orleans.Lattice.Tests.Formal;

public sealed partial class TlcModelCheckTests
{
    [TestCase("Error: Invariant TypeOK is violated.", true, 1)]
    [TestCase("Error: Invariant TypeOK is violated by the initial state:", true, 1)]
    [TestCase("Error: Invariant TypeOK is violated by the initial state:\r\n", true, 1)]
    [TestCase("Error: Invariant Other is violated by the initial state:", false, 1)]
    [TestCase("Error: Evaluating invariant TypeOK failed.", false, 0)]
    [TestCase("Error: Invariant TypeOK is violated because evaluation failed.", false, 0)]
    [TestCase("Trace: Error: Invariant TypeOK is violated.", false, 0)]
    [TestCase("Error: Invariant TypeOK is violated.\nError: Invariant Other is violated by the initial state:", true, 2)]
    public void Invariant_banner_matching_keeps_target_and_violation_count_strict(
        string output, bool expected, int count)
    {
        var mutation = new SpecMutation
        {
            Name = "TypeOKProbe",
            Module = "TypeOKProbe",
            Target = "TypeOK",
            PropertyClass = SpecPropertyClass.Invariant,
            Summary = "Probe exact invariant violation reporting.",
            Edits = [],
        };

        Assert.Multiple(() =>
        {
            Assert.That(HasExpectedViolation(output, mutation), Is.EqualTo(expected));
            Assert.That(CountViolationLines(output), Is.EqualTo(count));
        });
    }

    [TestCase(SpecPropertyClass.Action, "Error: Action property Probe is violated.", true)]
    [TestCase(SpecPropertyClass.Action, "Error: Action property Other is violated.", false)]
    [TestCase(SpecPropertyClass.Action, "Error: Invariant Probe is violated by the initial state:", false)]
    [TestCase(SpecPropertyClass.Temporal, "Error: Temporal properties were violated.", true)]
    [TestCase(SpecPropertyClass.Temporal, "Error: Evaluating temporal property Probe failed.", false)]
    public void Action_and_temporal_banner_matching_preserves_classification(
        SpecPropertyClass propertyClass, string output, bool expected)
    {
        var mutation = new SpecMutation
        {
            Name = "Probe",
            Module = "Probe",
            Target = "Probe",
            PropertyClass = propertyClass,
            Summary = "Probe exact violation class reporting.",
            Edits = [],
        };

        Assert.That(HasExpectedViolation(output, mutation), Is.EqualTo(expected));
    }

    [Test]
    public void An_initial_state_invariant_violation_is_a_valid_paired_counterexample()
    {
        using var synthetic = SyntheticSpecModule.Create();
        var module = synthetic.Discover();
        var mutation = module.LoadMutations().Single() with
        {
            Edits = [new("Init == x = 0", "Init == x = 3")],
        };

        Each_property_fires_under_its_mutation_and_not_on_the_base(module, mutation);
    }
}
