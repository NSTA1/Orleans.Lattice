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
/// (<see cref="ConfigureOrleansJsonSerializerOptions"/>), then Lattice's own
/// registration (<c>AddLatticeGrainStorageSerializer</c>), which installs
/// <see cref="LatticeGrainStorageSerializer"/> over the
/// <see cref="JsonGrainStorageSerializer"/> and adds its JSON read converters.
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
        services.AddLatticeGrainStorageSerializer();

        this.provider = services.BuildServiceProvider();
        this.Lattice = (LatticeGrainStorageSerializer)this.provider.GetRequiredService<IGrainStorageSerializer>();
        this.Json = (JsonGrainStorageSerializer)this.Lattice.Fallback;
    }

    /// <summary>The JSON grain-storage serializer alone.</summary>
    public JsonGrainStorageSerializer Json { get; }

    /// <summary>The serializer the silo's storage provider calls.</summary>
    public LatticeGrainStorageSerializer Lattice { get; }

    /// <summary>Writes then reads <paramref name="value"/> through <see cref="Lattice"/>.</summary>
    public T RoundTrip<T>(T value) => this.Lattice.Deserialize<T>(this.Lattice.Serialize(value));

    public void Dispose() => this.provider.Dispose();
}
