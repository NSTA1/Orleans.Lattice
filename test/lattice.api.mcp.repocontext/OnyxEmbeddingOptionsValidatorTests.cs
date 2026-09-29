using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="OnyxEmbeddingOptionsValidator"/> and its registration by
/// <see cref="LatticeMcpRepoContextEmbeddingServiceCollectionExtensions.AddOnyxEmbeddingProvider"/>.
/// <see cref="OnyxEmbeddingProvider"/> assigns
/// <see cref="OnyxEmbeddingOptions.RequestTimeout"/> to <see cref="HttpClient.Timeout"/>
/// on every call, and that setter throws <see cref="ArgumentOutOfRangeException"/> -
/// which the provider's fail-closed catch filters do not include - for a value it
/// cannot represent. So an unvalidated timeout made every health probe and every
/// embed call throw instead of degrading. Each rejected value is checked against
/// <see cref="HttpClient"/> itself, so the tests cannot pass vacuously by rejecting
/// a value the client would have accepted.
/// </summary>
[TestFixture]
public sealed class OnyxEmbeddingOptionsValidatorTests
{
    private static ValidateOptionsResult Validate(TimeSpan? timeout)
        => new OnyxEmbeddingOptionsValidator().Validate(
            name: null, new OnyxEmbeddingOptions { RequestTimeout = timeout });

    private static IEnumerable<TimeSpan> TimeoutsTheHttpClientRefuses()
    {
        yield return TimeSpan.Zero;
        yield return TimeSpan.FromSeconds(-1);
        yield return TimeSpan.FromMilliseconds(int.MaxValue) + TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromDays(30);

        // The usual attempt to say "no timeout".
        yield return TimeSpan.MaxValue;
    }

    private static IEnumerable<TimeSpan> TimeoutsTheHttpClientAccepts()
    {
        yield return TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromSeconds(100);
        yield return TimeSpan.FromMilliseconds(int.MaxValue);
        yield return Timeout.InfiniteTimeSpan;
    }

    [Test]
    public void An_unset_request_timeout_is_valid()
        => Assert.That(Validate(timeout: null).Succeeded, Is.True);

    [TestCaseSource(nameof(TimeoutsTheHttpClientAccepts))]
    public void A_request_timeout_the_http_client_accepts_is_valid(TimeSpan timeout)
    {
        using var client = new HttpClient();

        Assert.Multiple(() =>
        {
            Assert.That(Validate(timeout).Succeeded, Is.True);
            Assert.That(() => client.Timeout = timeout, Throws.Nothing);
        });
    }

    [TestCaseSource(nameof(TimeoutsTheHttpClientRefuses))]
    public void A_request_timeout_the_http_client_refuses_is_rejected(TimeSpan timeout)
    {
        var result = Validate(timeout);
        using var client = new HttpClient();

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.StartWith(nameof(OnyxEmbeddingOptions.RequestTimeout)));
            Assert.That(
                () => client.Timeout = timeout,
                Throws.InstanceOf<ArgumentOutOfRangeException>(),
                "anti-vacuity: the rejected value is one HttpClient itself refuses");
        });
    }

    [Test]
    public void The_validator_rejects_null_options()
        => Assert.That(
            () => new OnyxEmbeddingOptionsValidator().Validate(name: null, options: null!),
            Throws.ArgumentNullException);

    [Test]
    public void A_registered_provider_with_a_timeout_the_http_client_refuses_fails_at_resolution()
    {
        // Before the validator was registered this resolved a provider whose every
        // call threw ArgumentOutOfRangeException out of its fail-closed contract.
        var services = new ServiceCollection();
        services.AddOnyxEmbeddingProvider(o => o.RequestTimeout = TimeSpan.FromDays(30));

        using var provider = services.BuildServiceProvider();

        Assert.That(
            () => provider.GetRequiredService<IEmbeddingProvider>(),
            Throws.InstanceOf<OptionsValidationException>()
                .With.Message.Contains(nameof(OnyxEmbeddingOptions.RequestTimeout)));
    }

    [Test]
    public async Task A_registered_provider_with_the_largest_admitted_timeout_answers_its_health_probe()
    {
        using var handler = new StubHttpMessageHandler(_ => new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        var services = new ServiceCollection();
        services.AddOnyxEmbeddingProvider(o => o.RequestTimeout = OnyxEmbeddingOptionsValidator.MaxRequestTimeout);
        services.AddHttpClient(OnyxEmbeddingProvider.HttpClientName)
            .ConfigurePrimaryHttpMessageHandler(() => handler);

        using var provider = services.BuildServiceProvider();
        var embedding = provider.GetRequiredService<IEmbeddingProvider>();

        Assert.That(await embedding.IsAvailableAsync(), Is.True);
    }

    [Test]
    public void Registering_the_provider_twice_registers_the_validator_once()
    {
        var services = new ServiceCollection();
        services.AddOnyxEmbeddingProvider();
        services.AddOnyxEmbeddingProvider();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IValidateOptions<OnyxEmbeddingOptions>)),
            Is.EqualTo(1));
    }
}
