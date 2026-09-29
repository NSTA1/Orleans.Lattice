using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Compiles legacy implementers without the survey member and exercises the loud default.
/// </summary>
[TestFixture]
public sealed class OrphanedLeafSurveyCompatibilityTests
{
    [TestCase(typeof(ILattice))]
    [TestCase(typeof(ILatticeTreeAdmin))]
    public void Legacy_implementer_without_survey_compiles_and_reports_unsupported(Type contract)
    {
        // Generate only the old required members, regardless of whether survey
        // accidentally becomes abstract. Such a regression must fail compilation.
        var legacyType = LegacyContractImplementer.Compile(contract, "SurveyOrphanedLeavesAsync");
        Assert.That(legacyType.GetMethod("SurveyOrphanedLeavesAsync"), Is.Null);
        var legacy = Activator.CreateInstance(legacyType)!;
        var error = contract == typeof(ILattice)
            ? Assert.Throws<NotSupportedException>(() => { _ = ((ILattice)legacy).SurveyOrphanedLeavesAsync(); })
            : Assert.Throws<NotSupportedException>(() => { _ = ((ILatticeTreeAdmin)legacy).SurveyOrphanedLeavesAsync("tree"); });
        Assert.That(error!.Message, Does.Contain("SurveyOrphanedLeavesAsync"));
    }
}
