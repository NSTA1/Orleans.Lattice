using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for <see cref="RepoContextTtlOptions"/>, its
/// <see cref="RepoContextTtlOptionsValidator"/>, and the wiring that binds them
/// per repository through the named-options convention. Pure option/DI logic, so
/// it stays in the fast unit tier.
/// </summary>
[TestFixture]
public sealed class RepoContextTtlOptionsTests
{
    [Test]
    public void Defaults_leave_memory_durable_and_structural_records_permanent()
    {
        var options = new RepoContextTtlOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.DefaultMemoryTtl, Is.Null);
            Assert.That(options.StructuralRecordsNeverExpire, Is.True);
        });
    }

    [Test]
    public void Validator_accepts_a_null_default_ttl()
    {
        var result = Validate(new RepoContextTtlOptions { DefaultMemoryTtl = null });
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validator_accepts_a_positive_default_ttl()
    {
        var result = Validate(new RepoContextTtlOptions
        {
            DefaultMemoryTtl = TimeSpan.FromMinutes(15),
        });
        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validator_rejects_a_zero_default_ttl()
    {
        var result = Validate(new RepoContextTtlOptions { DefaultMemoryTtl = TimeSpan.Zero });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(RepoContextTtlOptions.DefaultMemoryTtl)));
        });
    }

    [Test]
    public void Validator_rejects_a_negative_default_ttl()
    {
        var result = Validate(new RepoContextTtlOptions
        {
            DefaultMemoryTtl = TimeSpan.FromSeconds(-1),
        });
        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_rejects_a_null_options_instance()
        => Assert.Throws<ArgumentNullException>(
            () => new RepoContextTtlOptionsValidator().Validate(name: null, options: null!));

    [Test]
    public void AddRepoContextTools_registers_the_ttl_options_validator()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();

        using var provider = services.BuildServiceProvider();
        var validators = provider.GetServices<IValidateOptions<RepoContextTtlOptions>>().ToList();

        Assert.That(validators, Has.Exactly(1).InstanceOf<RepoContextTtlOptionsValidator>());
    }

    [Test]
    public void Named_options_are_resolved_per_repository()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();
        services.Configure<RepoContextTtlOptions>(
            "repo-a", o => o.DefaultMemoryTtl = TimeSpan.FromMinutes(5));
        services.Configure<RepoContextTtlOptions>(
            "repo-b", o => o.StructuralRecordsNeverExpire = false);

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<RepoContextTtlOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get("repo-a").DefaultMemoryTtl, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(monitor.Get("repo-b").StructuralRecordsNeverExpire, Is.False);
            Assert.That(monitor.Get("unconfigured").DefaultMemoryTtl, Is.Null);
        });
    }

    [Test]
    public void A_misconfigured_named_instance_fails_validation_on_resolve()
    {
        var services = new ServiceCollection();
        services.AddRepoContextTools();
        services.Configure<RepoContextTtlOptions>(
            "bad-repo", o => o.DefaultMemoryTtl = TimeSpan.Zero);

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<RepoContextTtlOptions>>();

        Assert.Throws<OptionsValidationException>(() => monitor.Get("bad-repo"));
    }

    private static ValidateOptionsResult Validate(RepoContextTtlOptions options)
        => new RepoContextTtlOptionsValidator().Validate(name: null, options);
}
