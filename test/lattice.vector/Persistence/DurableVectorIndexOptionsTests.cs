using Orleans.Lattice.Vector.Persistence;

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
            Index = new VectorIndexOptions { Dimensions = 4, Probes = 2 },
        };

        var clone = options.Clone();
        options.KeyPrefix = "b/";
        options.MaxItemsPerChunk = 99;
        options.Index.Probes = 9;

        Assert.Multiple(() =>
        {
            Assert.That(clone.KeyPrefix, Is.EqualTo("a/"));
            Assert.That(clone.MaxItemsPerChunk, Is.EqualTo(7));
            Assert.That(clone.IngestBatchSize, Is.EqualTo(11));
            Assert.That(clone.KeyReservationBlock, Is.EqualTo(13));
            Assert.That(clone.Index.Dimensions, Is.EqualTo(4));
            Assert.That(clone.Index.Probes, Is.EqualTo(2));
        });
    }

    [Test]
    public void A_clone_of_options_with_no_index_configuration_still_produces_one()
    {
        var clone = new DurableVectorIndexOptions { Index = null! }.Clone();

        Assert.That(clone.Index, Is.Not.Null);
    }
}
