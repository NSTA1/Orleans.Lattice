using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the WAL replay concurrency wiring (issue #2279): the deferring
/// default, the environment override and its fail-fast validation, and the fact
/// that the option lands globally rather than per tree.
/// </summary>
/// <remarks>
/// The defect these cover is not that the option had a bad value. It is that no
/// environment variable could set it at all, because this host binds every
/// <c>LATTICE_*</c> setting by hand and nothing bound this one - so a compose
/// file could name it, the container could carry it, and the ceiling would
/// still silently resolve from <see cref="Environment.ProcessorCount"/>.
/// </remarks>
[TestFixture]
public sealed class RepoContextReplayConcurrencyTests
{
    private static IConfiguration Configuration(string? raw)
    {
        var dict = new Dictionary<string, string?>();
        if (raw is not null)
        {
            dict[RepoContextReplayConcurrency.MaxConcurrentReplaysKey] = raw;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void The_host_default_defers_to_the_library_rather_than_asserting_a_quota()
        => Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextReplayConcurrency.DefaultMaxConcurrentReplays,
                Is.EqualTo(LatticeOptions.DefaultWalMaterialiserMaxConcurrentReplays),
                "this wiring adds a capability, not an opinion: a host default other than the "
                + "library's would assert a CPU quota the host cannot actually observe");
            Assert.That(
                RepoContextReplayConcurrency.DefaultMaxConcurrentReplays,
                Is.Zero,
                "zero is what the library reads as 'unset', so an un-opted-in deployment is unaffected");
        });

    [Test]
    public void An_absent_variable_defers_to_the_library()
        => Assert.That(
            RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration(null)),
            Is.EqualTo(RepoContextReplayConcurrency.DefaultMaxConcurrentReplays));

    [TestCase("auto")]
    [TestCase("AUTO")]
    [TestCase("  Auto  ")]
    public void The_auto_token_selects_the_librarys_derivation_deliberately(string raw)
        => Assert.That(
            RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration(raw)),
            Is.EqualTo(RepoContextReplayConcurrency.DefaultMaxConcurrentReplays),
            "`auto` must reach the same derivation an absent variable reaches, because the "
            + "acceptance runs that pin this knob are measuring that derivation and would be "
            + "measuring something else if the token took a second code path. The token exists "
            + "for the layer above (issue #2863): the tuning overlay guards this variable with a "
            + "compose presence check, which cannot inspect a value, so `0` satisfied the guard "
            + "while meaning exactly what the guard forbids and an operator who forgot to export "
            + "the variable was indistinguishable from one who chose the derivation.");

    [Test]
    public void The_auto_token_is_the_spelling_the_deployment_is_told_to_use()
        => Assert.That(
            RepoContextReplayConcurrency.AutoToken,
            Is.EqualTo("auto"),
            "the sample deployment's preflight and .env.example both name this literal, so "
            + "renaming it here without renaming it there produces a .env that passes its own "
            + "preflight and then throws at startup.");

    [Test]
    public void A_garbage_value_still_fails_loudly_rather_than_deriving_quietly()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration("atuo")));

        Assert.That(
            ex!.Message,
            Does.Contain(RepoContextReplayConcurrency.AutoToken),
            "a near miss of the token is the likeliest way to mistype it, so the refusal has to "
            + "name the spelling it wanted. Falling through to the derivation instead would make a "
            + "typo indistinguishable from the deliberate token, which is the same conflation "
            + "#2863 is about, reintroduced one layer down.");
    }

    [Test]
    public void A_blank_variable_defers_to_the_library()
        => Assert.That(
            RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration("   ")),
            Is.EqualTo(RepoContextReplayConcurrency.DefaultMaxConcurrentReplays));

    [Test]
    public void An_explicit_value_wins_over_the_default()
        => Assert.That(
            RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration(" 6 ")),
            Is.EqualTo(6));

    [Test]
    public void An_explicit_zero_is_accepted_as_deliberate_deferral()
        => Assert.That(
            RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration("0")),
            Is.Zero,
            "an operator who writes 0 is asking for the library default, which is a legitimate request "
            + "and must not be confused with an out-of-range value");

    [Test]
    public void One_is_accepted_so_a_single_CPU_deployment_can_serialise_replays()
        => Assert.That(
            RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration("1")),
            Is.EqualTo(1));

    [TestCase("-1")]
    [TestCase("six")]
    [TestCase("6.5")]
    [TestCase("257")]
    [TestCase("2147483648")]
    public void An_out_of_range_or_unparseable_value_fails_the_host_fast(string raw)
        => Assert.That(
            () => RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration(raw)),
            Throws.InvalidOperationException.With.Message.Contains(RepoContextReplayConcurrency.MaxConcurrentReplaysKey),
            "the container must refuse to start rather than silently ignore the operator's intent - "
            + "silent ignoring is precisely the defect this wiring exists to remove");

    [TestCase("0")]
    [TestCase("1")]
    [TestCase("6")]
    [TestCase("256")]
    public void Every_accepted_value_satisfies_the_library_validator(string raw)
    {
        var resolved = RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(Configuration(raw));

        Assert.That(
            resolved,
            Is.GreaterThanOrEqualTo(0),
            "LatticeOptionsValidator rejects a negative WalMaterialiserMaxConcurrentReplays, so a value "
            + "this resolver accepts but the validator refuses would turn an operator typo into a "
            + "startup failure attributed to the wrong component");
    }

    [Test]
    public void ResolveMaxConcurrentReplays_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextReplayConcurrency.ResolveMaxConcurrentReplays(null!),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextReplayConcurrency_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextReplayConcurrency.ConfigureRepoContextReplayConcurrency(null!, Configuration(null)),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextReplayConcurrency_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextReplayConcurrency.ConfigureRepoContextReplayConcurrency(new FakeSiloBuilder(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_option_lands_globally_because_the_gate_is_a_single_process_wide_semaphore()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextReplayConcurrency(Configuration("6"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get(string.Empty).WalMaterialiserMaxConcurrentReplays, Is.EqualTo(6));
            Assert.That(monitor.Get(RepoContextHostTrees.VectorMetadata).WalMaterialiserMaxConcurrentReplays, Is.EqualTo(6),
                "BPlusLeafGrain sizes one process-wide gate from whichever tree activates first, so a "
                + "registration that reached only some names would make the effective ceiling a race");
        });
    }

    [Test]
    public void An_un_opted_in_host_leaves_the_library_default_in_place()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextReplayConcurrency(Configuration(null));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.That(
            monitor.Get(string.Empty).WalMaterialiserMaxConcurrentReplays,
            Is.EqualTo(LatticeOptions.DefaultWalMaterialiserMaxConcurrentReplays),
            "wiring this class in must be a no-op for every deployment that has not set the variable");
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> that exposes only its service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
