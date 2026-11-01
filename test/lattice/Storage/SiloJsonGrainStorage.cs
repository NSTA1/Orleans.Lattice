using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.GrainReferences;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.Storage;

/// <summary>
/// Builds the grain-storage serializers exactly as a silo wires them for a JSON
/// provider such as Azure Table storage: Orleans' Newtonsoft settings
/// post-configured with its type binder and converters
/// (<see cref="ConfigureOrleansJsonSerializerOptions"/>), the
/// <see cref="JsonGrainStorageSerializer"/> over them, and Lattice's
/// <see cref="LatticeGrainStorageSerializer"/> in front, which is the
/// serializer the provider actually calls.
/// </summary>
internal sealed class SiloJsonGrainStorage : IDisposable
{
    private readonly ServiceProvider provider;

    public SiloJsonGrainStorage()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddOptions();
        services.AddSingleton<GrainReferenceActivator>();
        services.AddSingleton<IPostConfigureOptions<OrleansJsonSerializerOptions>, ConfigureOrleansJsonSerializerOptions>();
        services.AddSingleton<OrleansJsonSerializer>();

        this.provider = services.BuildServiceProvider();
        this.Json = ActivatorUtilities.CreateInstance<JsonGrainStorageSerializer>(this.provider);
        this.Lattice = new LatticeGrainStorageSerializer(this.provider.GetRequiredService<Serializer>(), this.Json);
    }

    /// <summary>The JSON grain-storage serializer alone.</summary>
    public JsonGrainStorageSerializer Json { get; }

    /// <summary>The serializer the silo's storage provider calls.</summary>
    public LatticeGrainStorageSerializer Lattice { get; }

    /// <summary>Writes then reads <paramref name="value"/> through <see cref="Lattice"/>.</summary>
    public T RoundTrip<T>(T value) => this.Lattice.Deserialize<T>(this.Lattice.Serialize(value));

    public void Dispose() => this.provider.Dispose();
}
