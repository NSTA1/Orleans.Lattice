using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexDeclarationOptionsValidator"/>: the checks over
/// the whole declaration set that no single index can make about itself.
/// </summary>
[TestFixture]
public sealed class GrainIndexDeclarationOptionsValidatorTests
{
    private static GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState> Definition(
        string name,
        bool withProperties = true) =>
        new(name,
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            withProperties
                ? [new TypedGrainIndexProperty<TestGrainState, int>("Age", static s => s.Age)]
                : []);

    private static ValidateOptionsResult Validate(params IGrainIndexDefinition[] definitions)
    {
        var options = new GrainIndexDeclarationOptions();
        foreach (var definition in definitions)
        {
            options.Definitions.Add(definition);
        }

        return new GrainIndexDeclarationOptionsValidator().Validate(Options.DefaultName, options);
    }

    [Test]
    public void An_empty_declaration_set_passes() =>
        Assert.That(Validate().Succeeded, Is.True);

    [Test]
    public void Distinctly_named_indexes_with_projections_pass() =>
        Assert.That(Validate(Definition("users"), Definition("orders")).Succeeded, Is.True);

    [Test]
    public void Null_options_are_rejected() =>
        Assert.That(
            () => new GrainIndexDeclarationOptionsValidator().Validate(null, null!),
            Throws.ArgumentNullException);

    [Test]
    public void A_duplicate_index_name_fails_and_names_the_offender()
    {
        var result = Validate(Definition("users"), Definition("users"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("users"));
            Assert.That(result.FailureMessage, Does.Contain("declared more than once"));
        });
    }

    [Test]
    public void Duplicate_detection_is_case_sensitive_because_the_name_is_an_options_key() =>
        Assert.That(Validate(Definition("users"), Definition("Users")).Succeeded, Is.True);

    [Test]
    public void An_empty_projection_set_fails_and_names_the_offender()
    {
        var result = Validate(Definition("users", withProperties: false));

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("users"));
            Assert.That(result.FailureMessage, Does.Contain("projects no properties"));
        });
    }

    [Test]
    public void A_null_declaration_fails_with_its_position()
    {
        var options = new GrainIndexDeclarationOptions();
        options.Definitions.Add(null!);

        var result = new GrainIndexDeclarationOptionsValidator().Validate(null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("position 0"));
        });
    }

    [Test]
    public void Every_offender_is_reported_in_one_pass()
    {
        var result = Validate(
            Definition("users"),
            Definition("users"),
            Definition("orders", withProperties: false));

        Assert.That(result.Failures?.Count(), Is.EqualTo(2),
            "A host with several misconfigured indexes should see all of them at once.");
    }

    [Test]
    public void An_index_with_an_empty_name_fails()
    {
        // Lines 33-34: the validator detects a definition with a null or empty name.
        var definition = Substitute.For<IGrainIndexDefinition>();
        definition.Name.Returns(string.Empty);
        definition.PropertyDescriptors.Returns(
            new List<GrainIndexPropertyDescriptor>
            {
                new("Age", "System.Int32")
            });

        var options = new GrainIndexDeclarationOptions();
        options.Definitions.Add(definition);

        var result = new GrainIndexDeclarationOptionsValidator().Validate(Options.DefaultName, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("has no index name"));
        });
    }
}
