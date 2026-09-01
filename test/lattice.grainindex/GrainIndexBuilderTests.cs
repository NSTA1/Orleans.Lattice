namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexBuilder{TGrain, TState}"/>: explicit
/// <c>Include</c> selection, the defaults it applies, and the declaration errors
/// it refuses to build.
/// </summary>
[TestFixture]
public sealed class GrainIndexBuilderTests
{
    private static GrainIndexBuilder<ITestStringKeyedGrain, TestGrainState> Builder() => new();

    [Test]
    public void Include_projects_only_the_properties_that_were_opted_in()
    {
        var definition = Builder()
            .WithName("users")
            .Include(x => x.Age)
            .Include(x => x.Country)
            .Build();

        Assert.Multiple(() =>
        {
            Assert.That(
                definition.PropertyDescriptors.Select(d => d.Name),
                Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(
                definition.PropertyDescriptors.Select(d => d.Name),
                Has.No.Member(nameof(TestGrainState.Secret)),
                "There is no default-all-properties mode: a property nobody included is not indexed.");
        });
    }

    [Test]
    public void Include_records_the_declared_clr_type_of_each_selected_property()
    {
        var definition = Builder()
            .WithName("users")
            .Include(x => x.Age)
            .Include(x => x.LastSeen)
            .Build();

        Assert.That(
            definition.Properties.Select(p => p.PropertyType),
            Is.EqualTo(new[] { typeof(int), typeof(DateTimeOffset?) }));
    }

    [Test]
    public void Include_stores_a_compiled_accessor_rather_than_the_expression()
    {
        var definition = Builder().WithName("users").Include(x => x.Age).Build();

        var property = definition.Properties[0] as TypedGrainIndexProperty<TestGrainState, int>;

        Assert.Multiple(() =>
        {
            Assert.That(property, Is.Not.Null,
                "The projection path reads through a strongly typed delegate, so an included "
                + "property must materialise as the typed form.");
            Assert.That(property!.GetTypedValue(new TestGrainState { Age = 41 }), Is.EqualTo(41));
        });
    }

    [Test]
    public void Include_returns_the_same_builder_so_the_surface_chains()
    {
        var builder = Builder();

        Assert.That(builder.Include(x => x.Age), Is.SameAs(builder));
    }

    [Test]
    public void Include_rejects_a_null_selector() =>
        Assert.That(
            () => Builder().Include<int>(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Include_rejects_a_selector_that_is_not_a_direct_property_access() =>
        Assert.That(
            () => Builder().Include(x => x.Age + 1),
            Throws.ArgumentException.With.Message.Contains("direct property access"));

    [Test]
    public void Include_rejects_a_selector_that_ignores_the_state_parameter() =>
        Assert.That(
            () => Builder().Include(x => DateTimeOffset.UnixEpoch),
            Throws.ArgumentException.With.Message.Contains("direct property access"));

    [Test]
    public void Include_rejects_the_same_property_twice() =>
        Assert.That(
            () => Builder().Include(x => x.Age).Include(x => x.Age),
            Throws.ArgumentException.With.Message.Contains("already included"));

    [Test]
    public void Build_defaults_the_index_name_to_the_grain_interface_type_name() =>
        Assert.That(
            Builder().Include(x => x.Age).Build().Name,
            Is.EqualTo(nameof(ITestStringKeyedGrain)));

    [Test]
    public void With_name_sets_the_index_name() =>
        Assert.That(Builder().WithName("users").Include(x => x.Age).Build().Name, Is.EqualTo("users"));

    [Test]
    public void With_name_rejects_a_null_name() =>
        Assert.That(() => Builder().WithName(null!), Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("   ")]
    public void With_name_rejects_an_empty_or_whitespace_name(string name) =>
        Assert.That(() => Builder().WithName(name), Throws.ArgumentException);

    [Test]
    public void Tree_name_is_unset_by_default_so_the_reserved_default_applies() =>
        Assert.That(Builder().TreeNameOverride, Is.Null);

    [Test]
    public void With_tree_name_records_the_override() =>
        Assert.That(
            Builder().WithTreeName("__grainindex/custom").TreeNameOverride,
            Is.EqualTo("__grainindex/custom"));

    [Test]
    public void With_tree_name_rejects_a_null_name() =>
        Assert.That(() => Builder().WithTreeName(null!), Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("   ")]
    public void With_tree_name_rejects_an_empty_or_whitespace_name(string treeName) =>
        Assert.That(() => Builder().WithTreeName(treeName), Throws.ArgumentException);

    [Test]
    public void Allow_replication_defaults_to_false() =>
        Assert.That(Builder().AllowReplicationValue, Is.False,
            "A grain index points at grain activations in one cluster, so the safe default is "
            + "cluster-local.");

    [Test]
    public void Allow_replication_opts_in_when_called() =>
        Assert.That(Builder().AllowReplication().AllowReplicationValue, Is.True);

    [Test]
    public void Allow_replication_can_be_set_back_to_false_explicitly() =>
        Assert.That(Builder().AllowReplication().AllowReplication(false).AllowReplicationValue, Is.False);

    [Test]
    public void Backfill_knobs_are_unset_by_default_so_the_option_defaults_apply()
    {
        var builder = Builder();

        Assert.Multiple(() =>
        {
            Assert.That(builder.BackfillBatchSizeOverride, Is.Null);
            Assert.That(builder.BackfillIntervalOverride, Is.Null);
        });
    }

    [Test]
    public void With_backfill_batch_size_records_the_override() =>
        Assert.That(Builder().WithBackfillBatchSize(64).BackfillBatchSizeOverride, Is.EqualTo(64));

    [TestCase(0)]
    [TestCase(-1)]
    public void With_backfill_batch_size_rejects_a_non_positive_value(int batchSize) =>
        Assert.That(
            () => Builder().WithBackfillBatchSize(batchSize),
            Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void With_backfill_interval_records_the_override() =>
        Assert.That(
            Builder().WithBackfillInterval(TimeSpan.FromMinutes(5)).BackfillIntervalOverride,
            Is.EqualTo(TimeSpan.FromMinutes(5)));

    [Test]
    public void With_backfill_interval_rejects_a_non_positive_value() =>
        Assert.That(
            () => Builder().WithBackfillInterval(TimeSpan.Zero),
            Throws.TypeOf<ArgumentOutOfRangeException>());

    [Test]
    public void Build_uses_the_built_in_codec_matching_the_grain_key_shape() =>
        Assert.That(
            Builder().Include(x => x.Age).Build().KeyCodec,
            Is.SameAs(StringGrainKeyCodec<ITestStringKeyedGrain>.Instance));

    [Test]
    public void With_key_codec_replaces_the_built_in_codec()
    {
        var custom = new ReversingStringGrainKeyCodec();

        var definition = Builder().WithKeyCodec(custom).Include(x => x.Age).Build();

        Assert.That(definition.KeyCodec, Is.SameAs(custom));
    }

    [Test]
    public void With_key_codec_rejects_a_null_codec() =>
        Assert.That(() => Builder().WithKeyCodec(null!), Throws.ArgumentNullException);

    [Test]
    public void Build_throws_a_typed_failure_when_no_built_in_codec_can_encode_the_grain_key() =>
        Assert.That(
            () => new GrainIndexBuilder<ITestCompoundKeyedGrain, TestGrainState>()
                .Include(x => x.Age)
                .Build(),
            Throws.TypeOf<GrainIndexKeyEncodingException>());

    [Test]
    public void Include_with_a_cast_expression_unwraps_to_the_underlying_property_name()
    {
        // Lines 195-196: the unary Convert wrapping x.Age is peeled off in the
        // while loop so the property name resolves correctly.
        var definition = Builder()
            .WithName("cast-test")
            .Include(x => (long)x.Age)
            .Build();

        Assert.Multiple(() =>
        {
            Assert.That(definition.PropertyDescriptors, Has.Count.EqualTo(1));
            Assert.That(definition.PropertyDescriptors[0].Name, Is.EqualTo("Age"));
        });
    }

    /// <summary>
    /// A custom codec used only to prove the pluggable seam is honoured: it
    /// reverses the string key, which no built-in codec does.
    /// </summary>
    private sealed class ReversingStringGrainKeyCodec : IGrainKeyCodec<ITestStringKeyedGrain>
    {
        public Type GrainInterfaceType => typeof(ITestStringKeyedGrain);

        public bool TryEncode(
            Runtime.GrainId grainId,
            [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? encodedKey)
        {
            var key = grainId.Key.ToString();
            if (string.IsNullOrEmpty(key))
            {
                encodedKey = null;
                return false;
            }

            var characters = key.ToCharArray();
            Array.Reverse(characters);
            encodedKey = new string(characters);
            return true;
        }

        public string Encode(Runtime.GrainId grainId) =>
            TryEncode(grainId, out var encodedKey)
                ? encodedKey
                : throw new GrainIndexKeyEncodingException("test", grainId.ToString(), "No key.");

        public ITestStringKeyedGrain Resolve(IGrainFactory grainFactory, string encodedKey) =>
            throw new NotSupportedException("The builder tests never resolve through this codec.");

        IGrain IGrainKeyCodec.Resolve(IGrainFactory grainFactory, string encodedKey) =>
            Resolve(grainFactory, encodedKey);
    }
}
