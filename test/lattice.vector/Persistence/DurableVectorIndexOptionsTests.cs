using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class DurableVectorIndexOptionsTests
{
    [Test]
    public void The_defaults_are_usable_once_a_dimensionality_is_set()
    {
        var options = new DurableVectorIndexOptions { Index = new VectorIndexOptions { Dimensions = 8 } };

        Assert.Multiple(() =>
        {
            Assert.That(options.KeyPrefix, Is.EqualTo("vidx/"));
            Assert.That(options.MaxItemsPerChunk, Is.EqualTo(1_024));
            Assert.That(options.IngestBatchSize, Is.EqualTo(4_096));
            Assert.That(options.KeyReservationBlock, Is.EqualTo(1_024));
            Assert.That(options.Validate, Throws.Nothing);
        });
    }

    [Test]
    public void Validation_refuses_options_with_no_dimensionality()
    {
        Assert.That(new DurableVectorIndexOptions().Validate, Throws.ArgumentException);
    }

    [Test]
    public void Validation_refuses_options_with_no_index_configuration()
    {
        var options = new DurableVectorIndexOptions { Index = null! };

        Assert.That(options.Validate, Throws.ArgumentException);
    }

    [Test]
    public void A_null_key_prefix_is_refused()
    {
        Assert.That(() => new DurableVectorIndexOptions { KeyPrefix = null! }, Throws.ArgumentNullException);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void A_non_positive_sizing_knob_is_refused(int value)
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new DurableVectorIndexOptions { MaxItemsPerChunk = value },
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new DurableVectorIndexOptions { IngestBatchSize = value },
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new DurableVectorIndexOptions { KeyReservationBlock = value },
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void A_clone_is_independent_of_the_instance_it_came_from()
    {
        var options = new DurableVectorIndexOptions
        {
            KeyPrefix = "a/",
            MaxItemsPerChunk = 7,
            IngestBatchSize = 11,
            KeyReservationBlock = 13,
            IngestSliceBudget = TimeSpan.FromSeconds(17),
            TimeProvider = new SteppingTimeProvider(TimeSpan.Zero),
            Index = new VectorIndexOptions { Dimensions = 4, Probes = 2 },
        };

        var clone = options.Clone();
        options.KeyPrefix = "b/";
        options.MaxItemsPerChunk = 99;
        options.IngestSliceBudget = TimeSpan.FromSeconds(99);
        options.TimeProvider = TimeProvider.System;
        options.Index.Probes = 9;

        Assert.Multiple(() =>
        {
            Assert.That(clone.KeyPrefix, Is.EqualTo("a/"));
            Assert.That(clone.MaxItemsPerChunk, Is.EqualTo(7));
            Assert.That(clone.IngestBatchSize, Is.EqualTo(11));
            Assert.That(clone.KeyReservationBlock, Is.EqualTo(13));
            Assert.That(clone.IngestSliceBudget, Is.EqualTo(TimeSpan.FromSeconds(17)));
            Assert.That(clone.TimeProvider, Is.InstanceOf<SteppingTimeProvider>(),
                "A clone that dropped the clock would silently put a fixture back on the wall clock.");
            Assert.That(clone.Index.Dimensions, Is.EqualTo(4));
            Assert.That(clone.Index.Probes, Is.EqualTo(2));
        });
    }

    [Test]
    public void The_slice_budget_defaults_to_a_value_a_grain_turn_can_afford()
    {
        var options = new DurableVectorIndexOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.IngestSliceBudget,
                Is.EqualTo(DurableVectorIndexOptions.DefaultIngestSliceBudget));
            Assert.That(options.IngestSliceBudget, Is.GreaterThan(TimeSpan.Zero),
                "The bound is on by default, because the deployment that needed it had configured nothing.");
            Assert.That(options.IngestSliceBudget, Is.LessThan(TimeSpan.FromSeconds(30)),
                "A slice has to fit inside the call timeout a reminder tick is delivered under.");
            Assert.That(options.TimeProvider, Is.SameAs(TimeProvider.System));
        });
    }

    [Test]
    public void A_null_clock_is_rejected_rather_than_left_to_fail_mid_build()
    {
        var options = new DurableVectorIndexOptions();

        Assert.Throws<ArgumentNullException>(() => options.TimeProvider = null!);
    }

    [Test]
    public void A_non_positive_slice_budget_is_accepted_as_the_way_to_turn_the_bound_off()
    {
        var options = new DurableVectorIndexOptions
        {
            IngestSliceBudget = TimeSpan.Zero,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.IngestSliceBudget, Is.EqualTo(TimeSpan.Zero));
            Assert.DoesNotThrow(() => options.IngestSliceBudget = TimeSpan.FromSeconds(-1));
        });
    }

    [Test]
    public void A_clone_of_options_with_no_index_configuration_still_produces_one()
    {
        var clone = new DurableVectorIndexOptions { Index = null! }.Clone();

        Assert.That(clone.Index, Is.Not.Null);
    }
}
