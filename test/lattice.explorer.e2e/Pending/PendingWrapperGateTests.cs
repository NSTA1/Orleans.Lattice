using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Reports;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;

/// <summary>
/// Lists every pending wrapper written in the journeys in the CI job summary (#5361). I1 (#5400)
/// requires the list to be empty and turns <see cref="PendingWrapperGate.Hard"/> on; until then
/// the list is reported. A wrapper the scan cannot read always fails, so none hides from the list.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class PendingWrapperGateTests
{
    /// <summary>Scans the journeys, publishes the list, and fails on an unreadable wrapper (and, once hard, on any).</summary>
    [Test]
    public void Every_pending_wrapper_is_listed()
    {
        var written = PendingWrappers.Scan(JourneyPaths.ProjectDirectory, JourneyPaths.RepositoryRoot, out var unreadable);
        JobSummary.Append(PendingWrappers.ToMarkdown(written, PendingWrappers.RanThisRun, PendingWrapperGate.Hard));

        Assert.That(unreadable, Is.Empty,
            "A KnownDefect or KnownGap must name its issue and summary as string literals, so the pending-wrapper list can read it: " + string.Join(", ", unreadable));
        if (PendingWrapperGate.Hard)
        {
            Assert.That(written, Is.Empty, "I1 requires zero pending wrappers: " + string.Join(", ", written.Select(wrapper => $"{wrapper.Kind}({wrapper.Issue}) at {wrapper.File}:{wrapper.Line}")));
        }
    }
}
