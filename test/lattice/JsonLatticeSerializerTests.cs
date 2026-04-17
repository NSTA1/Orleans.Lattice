using System.Text.Json;

namespace Orleans.Lattice.Tests;

public class JsonLatticeSerializerTests
{
    private record TestPayload(string Name, int Value);

    [Test]
    public void Roundtrip_poco()
    {
        var serializer = JsonLatticeSerializer<TestPayload>.Default;
        var original = new TestPayload("hello", 42);

        var bytes = serializer.Serialize(original);
        var result = serializer.Deserialize(bytes);

        Assert.That(result, Is.EqualTo(original));
    }

    [Test]
    public void Roundtrip_string()
    {
        var serializer = JsonLatticeSerializer<string>.Default;
        var bytes = serializer.Serialize("test");
        Assert.That(serializer.Deserialize(bytes), Is.EqualTo("test"));
    }

    [Test]
    public void Roundtrip_int()
    {
        var serializer = JsonLatticeSerializer<int>.Default;
        var bytes = serializer.Serialize(99);
        Assert.That(serializer.Deserialize(bytes), Is.EqualTo(99));
    }

    [Test]
    public void Custom_options_are_respected()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        var serializer = new JsonLatticeSerializer<TestPayload>(options);

        var bytes = serializer.Serialize(new TestPayload("x", 1));
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        Assert.That(json, Does.Contain("\"name\""));
        Assert.That(json, Does.Contain("\"value\""));
    }

    [Test]
    public void Default_instance_is_singleton()
    {
        var a = JsonLatticeSerializer<int>.Default;
        var b = JsonLatticeSerializer<int>.Default;
        Assert.That(a, Is.SameAs(b));
    }
}
