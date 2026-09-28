using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="LatticeReplicationStatusOptions"/> (the documented
/// defaults) and <see cref="LatticeReplicationStatusOptionsValidator"/>.
/// </summary>
[TestFixture]
public sealed class LatticeReplicationStatusOptionsTests
{
    [Test]
    public void Defaults_match_the_documented_values()
    {
        var options = new LatticeReplicationStatusOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.LaggingEntriesBehind, Is.EqualTo(1_000));
            Assert.That(options.StalledEntriesBehind, Is.EqualTo(10_000));
            Assert.That(options.LaggingConsecutiveErrors, Is.EqualTo(5));
            Assert.That(options.StalledConsecutiveErrors, Is.EqualTo(50));
            Assert.That(options.LaggingAfterNoContact, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.StalledAfterNoContact, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.InboundLaggingAfterNoContact, Is.Null);
            Assert.That(options.InboundStalledAfterNoContact, Is.Null);
            Assert.That(LatticeReplicationStatusOptions.DefaultLaggingAfterNoContact, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(LatticeReplicationStatusOptions.DefaultStalledAfterNoContact, Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
    }

    [Test]
    public void Validator_accepts_the_defaults_and_disabled_bounds()
    {
        var validator = new LatticeReplicationStatusOptionsValidator();

        Assert.Multiple(() =>
        {
            Assert.That(validator.Validate(null, new LatticeReplicationStatusOptions()).Succeeded, Is.True);
            Assert.That(
                validator.Validate(null, new LatticeReplicationStatusOptions
                {
                    LaggingEntriesBehind = null,
                    StalledConsecutiveErrors = null,
                    InboundLaggingAfterNoContact = TimeSpan.FromMinutes(1),
                }).Succeeded,
                Is.True);
            Assert.That(
                validator.Validate(null, new LatticeReplicationStatusOptions
                {
                    LaggingEntriesBehind = 7,
                    StalledEntriesBehind = 7,
                }).Succeeded,
                Is.True,
                "equal bounds are allowed");
        });
    }

    [Test]
    public void Validator_rejects_negative_bounds()
    {
        var result = new LatticeReplicationStatusOptionsValidator().Validate(null, new LatticeReplicationStatusOptions
        {
            LaggingEntriesBehind = -1,
            StalledConsecutiveErrors = -1,
            StalledAfterNoContact = TimeSpan.FromSeconds(-1),
            InboundLaggingAfterNoContact = TimeSpan.FromSeconds(-1),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.Failures, Has.Some.Contains(nameof(LatticeReplicationStatusOptions.LaggingEntriesBehind)));
            Assert.That(result.Failures, Has.Some.Contains(nameof(LatticeReplicationStatusOptions.StalledConsecutiveErrors)));
            Assert.That(result.Failures, Has.Some.Contains(nameof(LatticeReplicationStatusOptions.StalledAfterNoContact)));
            Assert.That(result.Failures, Has.Some.Contains(nameof(LatticeReplicationStatusOptions.InboundLaggingAfterNoContact)));
        });
    }

    [Test]
    public void Validator_rejects_a_lagging_bound_above_its_stalled_bound()
    {
        var result = new LatticeReplicationStatusOptionsValidator().Validate(null, new LatticeReplicationStatusOptions
        {
            LaggingConsecutiveErrors = 10,
            StalledConsecutiveErrors = 9,
            InboundLaggingAfterNoContact = TimeSpan.FromMinutes(2),
            InboundStalledAfterNoContact = TimeSpan.FromMinutes(1),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.Failures, Has.Exactly(2).Items);
        });
    }

    [Test]
    public void Validator_null_options_throws()
    {
        Assert.That(
            () => new LatticeReplicationStatusOptionsValidator().Validate(null, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Validator_is_an_options_validator()
    {
        Assert.That(
            new LatticeReplicationStatusOptionsValidator(),
            Is.InstanceOf<IValidateOptions<LatticeReplicationStatusOptions>>());
    }
}
