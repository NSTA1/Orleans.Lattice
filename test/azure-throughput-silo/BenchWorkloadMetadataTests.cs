using NUnit.Framework;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo.Tests;

/// <summary>
/// Guards the single shared <c>BENCH_WORKLOAD_MODE</c> mapping that both the
/// silo and the Orleans-client producer resolve through.
/// </summary>
/// <remarks>
/// The round-trip test is the load-bearing one. Both hosts label their output
/// from the workload the harness *asked* for, not from the workload they
/// actually parsed, and the parser falls back to <c>set-many</c> on anything
/// unrecognised. A new mode added to the enum but missed in either mapping
/// therefore produces no error at all: the run completes, emits a plausible
/// throughput number, and files it under a workload it never executed.
/// Enumerating the enum rather than listing cases by hand is what makes the
/// guard cover modes that do not exist yet.
/// </remarks>
[TestFixture]
public sealed class BenchWorkloadMetadataTests
{
    [Test]
    public void ParseWorkloadMode_round_trips_every_enum_value_through_its_formatted_name()
    {
        Assert.Multiple(() =>
        {
            foreach (BenchWorkloadMode mode in Enum.GetValues<BenchWorkloadMode>())
            {
                var formatted = BenchWorkloadMetadata.FormatWorkloadMode(mode);
                Assert.That(
                    BenchWorkloadMetadata.ParseWorkloadMode(formatted),
                    Is.EqualTo(mode),
                    $"'{formatted}' did not parse back to {mode}. Add the spelling to ParseWorkloadMode, "
                        + "or the run will silently fall back to set-many and be reported under the wrong workload.");
            }
        });
    }

    [Test]
    public void ParseWorkloadMode_accepts_run_together_and_mixed_case_spellings()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BenchWorkloadMetadata.ParseWorkloadMode("SetManyAtomic2"), Is.EqualTo(BenchWorkloadMode.SetManyAtomic2));
            Assert.That(BenchWorkloadMetadata.ParseWorkloadMode("  CROSS-TREE-ATOMIC-64  "), Is.EqualTo(BenchWorkloadMode.CrossTreeAtomic64));
            Assert.That(BenchWorkloadMetadata.ParseWorkloadMode("set"), Is.EqualTo(BenchWorkloadMode.SetPoint));
            Assert.That(BenchWorkloadMetadata.ParseWorkloadMode("get"), Is.EqualTo(BenchWorkloadMode.GetPoint));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("not-a-workload")]
    public void ParseWorkloadMode_falls_back_to_set_many_for_unusable_input(string? raw)
    {
        Assert.That(BenchWorkloadMetadata.ParseWorkloadMode(raw), Is.EqualTo(BenchWorkloadMode.SetMany));
    }
}
