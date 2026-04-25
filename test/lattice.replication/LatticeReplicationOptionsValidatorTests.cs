using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationOptionsValidatorTests
{
    private static readonly LatticeReplicationOptionsValidator Validator = new();

    [TestCase("")]
    [TestCase("   ")]
    [TestCase("\t")]
    public void Validate_fails_on_null_empty_or_whitespace_cluster_id(string clusterId)
    {
        var opts = new LatticeReplicationOptions { ClusterId = clusterId };

        var result = Validator.Validate(name: null, opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeReplicationOptions.ClusterId)));
        });
    }

    [Test]
    public void Validate_fails_on_null_cluster_id()
    {
        var opts = new LatticeReplicationOptions { ClusterId = null! };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_failure_message_mentions_named_options_instance()
    {
        var opts = new LatticeReplicationOptions();

        var result = Validator.Validate(name: "my-tree", opts);

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain("my-tree"));
        });
    }

    [Test]
    public void Validate_failure_message_calls_out_default_instance_explicitly()
    {
        var opts = new LatticeReplicationOptions();

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.FailureMessage, Does.Contain("default"));
    }

    [Test]
    public void Validate_fails_on_default_cluster_id()
    {
        var opts = new LatticeReplicationOptions();

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validate_succeeds_for_non_empty_cluster_id()
    {
        var opts = new LatticeReplicationOptions { ClusterId = "site-a" };

        var result = Validator.Validate(name: null, opts);

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validate_runs_per_named_options_instance()
    {
        var bothFail = Validator.Validate("named", new LatticeReplicationOptions());
        var bothPass = Validator.Validate("named", new LatticeReplicationOptions { ClusterId = "x" });

        Assert.Multiple(() =>
        {
            Assert.That(bothFail.Failed, Is.True);
            Assert.That(bothPass.Succeeded, Is.True);
        });
    }

    [Test]
    public void AddLatticeReplication_throws_OptionsValidationException_when_cluster_id_unset()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLatticeReplication(_ => { });

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        Assert.That(() => monitor.CurrentValue, Throws.TypeOf<OptionsValidationException>());
    }

    [Test]
    public void AddLatticeReplication_resolves_options_when_cluster_id_set()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        builder.AddLatticeReplication(opts => opts.ClusterId = "ok");

        var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();

        Assert.That(monitor.CurrentValue.ClusterId, Is.EqualTo("ok"));
    }
}

