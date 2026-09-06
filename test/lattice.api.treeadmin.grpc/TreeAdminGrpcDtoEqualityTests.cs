using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc.Tests;

/// <summary>
/// Guards value equality on <see cref="TreeAdminCreateViewRequest"/>, whose opaque
/// <c>Payload</c> the compiler-generated record equality would otherwise compare
/// with <see cref="EqualityComparer{T}.Default"/> (reference equality). Two
/// structurally identical requests carrying distinct-but-equal arrays - the shape a
/// request and its post-serialization self take - must compare equal and share a
/// hash code, and a difference in the byte content or any scalar must compare
/// unequal.
/// </summary>
[TestFixture]
public sealed class TreeAdminGrpcDtoEqualityTests
{
    private static TreeAdminCreateViewRequest Request(byte[] payload) => new()
    {
        ViewName = "v",
        SourceTreeId = "src",
        ProviderKey = "provider",
        Payload = payload,
    };

    [Test]
    public void CreateViewRequest_equal_content_with_distinct_arrays_are_equal()
    {
        var a = Request(new byte[] { 1, 2, 3 });
        var b = Request(new byte[] { 1, 2, 3 });

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Payload, b.Payload), Is.False);
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void CreateViewRequest_differing_payload_content_is_not_equal()
    {
        Assert.That(Request(new byte[] { 1, 2, 3 }), Is.Not.EqualTo(Request(new byte[] { 1, 2, 4 })));
    }

    [Test]
    public void CreateViewRequest_differing_scalar_is_not_equal()
    {
        var a = Request(new byte[] { 1 });
        var b = new TreeAdminCreateViewRequest
        {
            ViewName = "v",
            SourceTreeId = "src",
            ProviderKey = "other",
            Payload = new byte[] { 1 },
        };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void CreateViewRequest_round_trip_compares_equal_by_value()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<TreeAdminCreateViewRequest>>();
        var original = Request(new byte[] { 7, 8, 9 });

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy, Is.EqualTo(original));
    }
}
