using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexOptionsValidator"/>: the per-index checks that
/// fail a host at startup, and the requirement that every message names the
/// offending index.
/// </summary>
[TestFixture]
public sealed class GrainIndexOptionsValidatorTests
{
    private static GrainIndexOptions Valid() => new()
    {
        TreeName = "__grainindex/users",
        BackfillBatchSize = 32,
        BackfillInterval = TimeSpan.FromSeconds(1),
    };

    private static ValidateOptionsResult Validate(string? name, GrainIndexOptions options) =>
        new GrainIndexOptionsValidator().Validate(name, options);

    [Test]
    public void A_fully_specified_index_passes() =>
        Assert.That(Validate("users", Valid()).Succeeded, Is.True);

    [Test]
    public void The_unnamed_template_instance_is_skipped_because_it_backs_no_index() =>
        Assert.That(Validate(Options.DefaultName, new GrainIndexOptions()).Skipped, Is.True);

    [Test]
    public void Null_options_are_rejected() =>
        Assert.That(() => Validate("users", null!), Throws.ArgumentNullException);

    [Test]
    public void A_missing_tree_name_fails_and_names_the_index()
    {
        var options = Valid();
        options.TreeName = string.Empty;

        var result = Validate("users", options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("users"));
            Assert.That(result.FailureMessage, Does.Contain("__grainindex/users"));
        });
    }

    [Test]
    public void A_whitespace_tree_name_fails()
    {
        var options = Valid();
        options.TreeName = "   ";

        Assert.That(Validate("users", options).Failed, Is.True);
    }

    [Test]
    public void A_tree_name_outside_the_reserved_namespace_fails_and_names_the_index()
    {
        var options = Valid();
        options.TreeName = "app-users";

        var result = Validate("users", options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("users"));
            Assert.That(result.FailureMessage, Does.Contain(GrainIndexTreeNames.ReservedPrefix));
        });
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void A_non_positive_backfill_batch_size_fails_and_names_the_index(int batchSize)
    {
        var options = Valid();
        options.BackfillBatchSize = batchSize;

        var result = Validate("users", options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("users"));
            Assert.That(result.FailureMessage, Does.Contain(nameof(GrainIndexOptions.BackfillBatchSize)));
        });
    }

    [Test]
    public void A_non_positive_backfill_interval_fails_and_names_the_index()
    {
        var options = Valid();
        options.BackfillInterval = TimeSpan.Zero;

        var result = Validate("users", options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("users"));
            Assert.That(result.FailureMessage, Does.Contain(nameof(GrainIndexOptions.BackfillInterval)));
        });
    }

    [Test]
    public void Every_offender_is_reported_in_one_pass()
    {
        var options = new GrainIndexOptions
        {
            TreeName = "app-users",
            BackfillBatchSize = 0,
            BackfillInterval = TimeSpan.Zero,
        };

        var result = Validate("users", options);

        Assert.That(result.Failures?.Count(), Is.EqualTo(3),
            "Reporting one failure at a time turns a single misconfiguration into three "
            + "start-fix-restart cycles.");
    }
}
