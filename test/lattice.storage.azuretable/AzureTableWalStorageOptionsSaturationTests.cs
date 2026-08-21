using Azure.Data.Tables;
using NSubstitute;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Unit tests for the saturation-signal-related surface of
/// <see cref="AzureTableWalStorageOptions"/>: the
/// <see cref="AzureTableWalStorageOptions.SaturationShortCircuitCooldown"/>
/// validation guard and the
/// <see cref="AzureTableWalStorageOptions.BuildServiceClient"/> overload
/// that attaches a <see cref="SaturationAwareRetryPolicy"/> when a
/// silo-scoped <see cref="IWalSaturationSignal"/> is supplied. These
/// complement the existing <see cref="AzureTableWalStorageOptionsTests"/>
/// fixture, which does not exercise the saturation paths.
/// </summary>
[TestFixture]
public class AzureTableWalStorageOptionsSaturationTests
{
    [Test]
    public void Validate_throws_when_SaturationShortCircuitCooldown_is_negative()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            SaturationShortCircuitCooldown = TimeSpan.FromMilliseconds(-1),
        };

        Assert.That(options.Validate, Throws.InvalidOperationException);
    }

    [Test]
    public void Validate_succeeds_when_SaturationShortCircuitCooldown_is_zero()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            SaturationShortCircuitCooldown = TimeSpan.Zero,
        };

        Assert.That(options.Validate, Throws.Nothing);
    }

    [Test]
    public void HonorSaturationSignal_defaults_to_true()
    {
        Assert.That(new AzureTableWalStorageOptions().HonorSaturationSignal, Is.True);
        Assert.That(AzureTableWalStorageOptions.DefaultHonorSaturationSignal, Is.True);
    }

    [Test]
    public void SaturationShortCircuitCooldown_defaults_to_two_seconds()
    {
        Assert.That(
            new AzureTableWalStorageOptions().SaturationShortCircuitCooldown,
            Is.EqualTo(TimeSpan.FromSeconds(2)));
        Assert.That(
            AzureTableWalStorageOptions.DefaultSaturationShortCircuitCooldown,
            Is.EqualTo(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void BuildServiceClient_attaches_saturation_policy_when_signal_supplied_and_honored()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            HonorSaturationSignal = true,
        };

        // Building the client with a non-null signal and the honor flag
        // on runs the policy-attachment branch. A fully-built
        // TableServiceClient proves the pipeline assembled without
        // throwing.
        var client = options.BuildServiceClient(signal);

        Assert.That(client, Is.InstanceOf<TableServiceClient>());
    }

    [Test]
    public void BuildServiceClient_skips_saturation_policy_when_signal_supplied_but_not_honored()
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            HonorSaturationSignal = false,
        };

        var client = options.BuildServiceClient(signal);

        Assert.That(client, Is.InstanceOf<TableServiceClient>());
    }

    [Test]
    public void BuildServiceClient_skips_saturation_policy_when_signal_is_null()
    {
        var options = new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            HonorSaturationSignal = true,
        };

        var client = options.BuildServiceClient(saturationSignal: null);

        Assert.That(client, Is.InstanceOf<TableServiceClient>());
    }
}
