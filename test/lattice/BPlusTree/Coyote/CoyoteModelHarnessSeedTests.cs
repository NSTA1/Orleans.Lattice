using System.Text;
using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Tests for the <c>seed</c> of <see cref="CoyoteModelHarness.Explore"/> and for
/// <see cref="CoyoteModelHarness.GuardSeed"/> (issue #4727). The model fails its
/// first run with a report naming every choice that run made, so the report
/// identifies the explored path.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class CoyoteModelHarnessSeedTests
{
    [Test]
    public void Explore_with_the_same_seed_explores_the_same_runs_on_every_call()
    {
        var first = CoyoteModelHarness.Explore(new ChoicePathModel(), iterations: 1, seed: CoyoteModelHarness.GuardSeed);

        for (var call = 0; call < 5; call++)
        {
            var again = CoyoteModelHarness.Explore(new ChoicePathModel(), iterations: 1, seed: CoyoteModelHarness.GuardSeed);

            Assert.That(again.BugReports, Is.EqualTo(first.BugReports));
            Assert.That(again.ReproducibleTrace, Is.EqualTo(first.ReproducibleTrace));
        }

        Assert.That(first.BugsFound, Is.EqualTo(1));
    }

    [Test]
    public void Explore_feeds_the_seed_to_the_exploration_strategy()
    {
        var paths = new HashSet<string>(StringComparer.Ordinal);
        for (uint seed = 1; seed <= 16; seed++)
        {
            var result = CoyoteModelHarness.Explore(new ChoicePathModel(), iterations: 1, seed: seed);
            paths.Add(string.Join("\n", result.BugReports));
        }

        Assert.That(
            paths,
            Has.Count.GreaterThan(1),
            "every seed explored the same path, so the seed does not reach the exploration strategy.");
    }

    private sealed class ChoicePathModel : ICoyoteModel
    {
        public void Run(ICoyoteRuntime runtime)
        {
            var path = new StringBuilder("[ChoicePath] ", 32);
            for (var choice = 0; choice < 16; choice++)
            {
                path.Append(runtime.RandomBoolean() ? '1' : '0');
            }

            Specification.Assert(false, path.ToString());
        }
    }
}
