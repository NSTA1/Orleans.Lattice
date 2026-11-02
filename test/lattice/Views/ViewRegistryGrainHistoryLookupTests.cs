using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// <see cref="IViewRegistryGrain.ListAccumulativeBySourceAsync"/>, the lookup a
/// history read uses to find its tree's durable history view on a silo whose catalog
/// never held it (#5666). It answers only the tree's candidates, so the read neither
/// transfers nor allocates the cluster-wide registration snapshot (#5713).
/// </summary>
[TestFixture]
public class ViewRegistryGrainHistoryLookupTests
{
    [Test]
    public async Task Only_accumulative_non_aggregation_views_of_the_source_are_returned()
    {
        var grain = Create(
            Registration("hist", "src", accumulative: true),
            Registration("plain", "src", accumulative: false),
            Registration("agg", "src", accumulative: true, aggregation: true),
            Registration("other", "src-2", accumulative: true),
            Registration("hist-2", "src", accumulative: true));

        var matches = await grain.ListAccumulativeBySourceAsync("src");

        Assert.That(matches.Select(m => m.ViewName), Is.EquivalentTo(new[] { "hist", "hist-2" }));
    }

    [Test]
    public async Task A_source_with_no_candidate_gets_an_empty_answer()
    {
        var grain = Create(Registration("hist", "src", accumulative: true));

        Assert.That(await grain.ListAccumulativeBySourceAsync("elsewhere"), Is.Empty);
    }

    [Test]
    public void The_lookup_does_not_grow_with_the_number_of_unrelated_views()
    {
        var growth = AllocationProbe.Growth(
            static views =>
            {
                var registrations = new RuntimeViewRegistration[views + 1];
                registrations[0] = Registration("hist", "src", accumulative: true);
                for (var i = 1; i <= views; i++)
                {
                    registrations[i] = Registration($"v{i}", $"other-{i}", accumulative: true);
                }

                return Create(registrations);
            },
            static (grain, _) =>
            {
                AllocationProbe.ScalarSink += grain.ListAccumulativeBySourceAsync("src").GetAwaiter().GetResult().Length;
            },
            smallSize: 4,
            largeSize: 1_004);

        Assert.That(growth, Is.Zero, "the lookup must not copy every registration");
    }

    private static ViewRegistryGrain Create(params RuntimeViewRegistration[] registrations)
    {
        var state = new FakePersistentState<ViewRegistryState>();
        foreach (var registration in registrations)
        {
            state.State.Registrations[registration.ViewName] = registration;
        }

        return new ViewRegistryGrain(Substitute.For<IGrainContext>(), state);
    }

    private static RuntimeViewRegistration Registration(string name, string source, bool accumulative, bool aggregation = false) =>
        new()
        {
            ViewName = name,
            SourceTreeId = source,
            ProjectionTypeName = "Test.Projection",
            ProjectionVersion = "1",
            Accumulative = accumulative,
            IsAggregation = aggregation,
        };
}
