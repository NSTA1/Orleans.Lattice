using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Round-trips the unauthenticated auth-scheme advertisement wire records and
/// pins their serialization aliases. The advertisement travels over the wire
/// from an unauthenticated probe, so its contract must be coherent and its
/// aliases stable and collision-free.
/// </summary>
[TestFixture]
public sealed class AuthSchemeAdvertisementSerializationTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp() =>
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void AuthSchemeAdvertisementRequest_round_trips()
        => Assert.That(RoundTrip(new AuthSchemeAdvertisementRequest()), Is.EqualTo(new AuthSchemeAdvertisementRequest()));

    [Test]
    public void AuthSchemeDescriptor_round_trips_with_parameters()
    {
        var original = new AuthSchemeDescriptor
        {
            SchemeId = "entra",
            DisplayName = "Microsoft Entra ID",
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["authority"] = "https://login.microsoftonline.com/contoso",
                ["clientId"] = "client-123",
                ["audience"] = "api://state-api",
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.SchemeId, Is.EqualTo("entra"));
            Assert.That(copy.DisplayName, Is.EqualTo("Microsoft Entra ID"));
            Assert.That(copy.Parameters["authority"], Is.EqualTo("https://login.microsoftonline.com/contoso"));
            Assert.That(copy.Parameters["clientId"], Is.EqualTo("client-123"));
            Assert.That(copy.Parameters["audience"], Is.EqualTo("api://state-api"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisement_round_trips_with_ordered_schemes()
    {
        var original = new AuthSchemeAdvertisement
        {
            Schemes = new[]
            {
                new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Microsoft Entra ID" },
                new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Username and password" },
            },
        };

        var copy = RoundTrip(original);
        Assert.Multiple(() =>
        {
            Assert.That(copy.Schemes.Select(s => s.SchemeId), Is.EqualTo(new[] { "entra", "basic" }));
            Assert.That(copy.Schemes[0].DisplayName, Is.EqualTo("Microsoft Entra ID"));
        });
    }

    [Test]
    public void AuthSchemeAdvertisement_round_trips_when_empty()
    {
        var copy = RoundTrip(new AuthSchemeAdvertisement());
        Assert.That(copy.Schemes, Is.Empty);
    }

    [Test]
    public void New_auth_aliases_use_the_grpc_prefix_and_are_distinct()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrpcStateTypeAliases.AuthSchemeAdvertisementRequest, Is.EqualTo("olag.asreq"));
            Assert.That(GrpcStateTypeAliases.AuthSchemeDescriptor, Is.EqualTo("olag.asdesc"));
            Assert.That(GrpcStateTypeAliases.AuthSchemeAdvertisement, Is.EqualTo("olag.asadv"));
        });
    }

    [Test]
    public void Every_alias_constant_is_unique()
    {
        var values = typeof(GrpcStateTypeAliases)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();

        Assert.That(values, Is.Unique);
    }
}
