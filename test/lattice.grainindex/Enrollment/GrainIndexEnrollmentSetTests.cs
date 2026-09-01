using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="GrainIndexEnrollmentSet{TState}"/>: which declared indexes
/// a state type sees, which of those a given grain class is actually tracked by,
/// and the memoisation that keeps the second question off the activation path.
/// </summary>
[TestFixture]
public sealed class GrainIndexEnrollmentSetTests
{
    private static ServiceProvider Declaring(Action<StubSiloBuilder> declare)
    {
        var builder = new StubSiloBuilder();
        declare(builder);
        builder.Services.AddOptions();
        builder.Services.AddLogging();
        return builder.Services.BuildServiceProvider();
    }

    private static GrainIndexEnrollmentSet<TState> SetFor<TState>(ServiceProvider provider) =>
        new(
            provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
            Substitute.For<IGrainFactory>(),
            new RecordingEnrollmentStore());

    [Test]
    public void A_state_type_no_index_projects_yields_an_empty_set()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("declared").Include(x => x.Age)));

        Assert.That(SetFor<IndexedTestState>(provider).IsEmpty, Is.True,
            "A state object whose type nothing projects must skip enrolment entirely.");
    }

    [Test]
    public void A_declared_index_over_the_state_type_is_offered()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("subjects").Include(x => x.Age)));

        var set = SetFor<IndexedTestState>(provider);

        Assert.Multiple(() =>
        {
            Assert.That(set.IsEmpty, Is.False);
            Assert.That(
                set.For(EnrollmentTestIndex.GrainInstance()).Select(e => e.IndexName),
                Is.EqualTo(new[] { "subjects" }));
        });
    }

    [Test]
    public void Only_the_indexes_whose_grain_interface_the_grain_implements_track_it()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("by-string").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("by-guid").Include(x => x.Age)));

        var set = SetFor<IndexedTestState>(provider);

        Assert.Multiple(() =>
        {
            Assert.That(
                set.For(EnrollmentTestIndex.GrainInstance()).Select(e => e.IndexName),
                Is.EqualTo(new[] { "by-string" }));
            Assert.That(
                set.For(Substitute.For<ITestGuidKeyedGrain>()).Select(e => e.IndexName),
                Is.EqualTo(new[] { "by-guid" }));
        });
    }

    [Test]
    public void A_grain_matching_no_declared_interface_is_not_tracked()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("subjects").Include(x => x.Age)));

        Assert.That(SetFor<IndexedTestState>(provider).For(Substitute.For<ITestGuidKeyedGrain>()), Is.Empty);
    }

    [Test]
    public void A_missing_grain_instance_is_tracked_by_nothing()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("subjects").Include(x => x.Age)));

        Assert.That(SetFor<IndexedTestState>(provider).For(null), Is.Empty);
    }

    [Test]
    public void The_narrowing_is_memoised_so_a_grain_class_pays_for_it_once()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("subjects").Include(x => x.Age)));

        var set = SetFor<IndexedTestState>(provider);
        var grain = EnrollmentTestIndex.GrainInstance();

        Assert.That(set.For(grain), Is.SameAs(set.For(grain)),
            "Re-filtering per activation would put an interface test and an allocation on a path "
            + "every tracked grain crosses.");
    }

    [Test]
    public void An_index_declared_over_a_different_state_type_is_not_offered()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("subjects").Include(x => x.Age))
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("others").Include(x => x.Age)));

        Assert.That(
            SetFor<IndexedTestState>(provider).For(EnrollmentTestIndex.GrainInstance()).Select(e => e.IndexName),
            Is.EqualTo(new[] { "subjects" }));
    }

    [Test]
    public void A_set_built_from_explicit_enrollers_offers_exactly_those()
    {
        var enroller = EnrollmentTestIndex.Enroller(new RecordingEnrollmentStore());
        var set = new GrainIndexEnrollmentSet<IndexedTestState>([enroller]);

        Assert.Multiple(() =>
        {
            Assert.That(set.IsEmpty, Is.False);
            Assert.That(set.For(EnrollmentTestIndex.GrainInstance()), Is.EqualTo(new[] { enroller }));
        });
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        using var provider = Declaring(static builder => builder
            .AddGrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                static cfg => cfg.WithName("subjects").Include(x => x.Age)));

        var declarations = provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>();
        var options = provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexEnrollmentSet<IndexedTestState>(
                    null!, options, Substitute.For<IGrainFactory>(), new RecordingEnrollmentStore()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexEnrollmentSet<IndexedTestState>(
                    declarations, null!, Substitute.For<IGrainFactory>(), new RecordingEnrollmentStore()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexEnrollmentSet<IndexedTestState>(
                    declarations, options, null!, new RecordingEnrollmentStore()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexEnrollmentSet<IndexedTestState>(
                    declarations, options, Substitute.For<IGrainFactory>(), null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexEnrollmentSet<IndexedTestState>(null!),
                Throws.ArgumentNullException);
        });
    }
}
