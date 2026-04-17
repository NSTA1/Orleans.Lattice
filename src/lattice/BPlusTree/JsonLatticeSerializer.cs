using System.Text.Json;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="ILatticeSerializer{T}"/> that uses <see cref="JsonSerializer"/>
/// with UTF-8 encoding. Suitable for most POCO types. For custom wire formats,
/// implement <see cref="ILatticeSerializer{T}"/> directly.
/// </summary>
/// <typeparam name="T">The value type to serialize.</typeparam>
public sealed class JsonLatticeSerializer<T>(JsonSerializerOptions? options = null) : ILatticeSerializer<T>
{
    /// <summary>Shared default instance (default <see cref="JsonSerializerOptions"/>).</summary>
    public static JsonLatticeSerializer<T> Default { get; } = new();

    /// <inheritdoc />
    public byte[] Serialize(T value) => JsonSerializer.SerializeToUtf8Bytes(value, options);

    /// <inheritdoc />
    public T Deserialize(byte[] bytes) => JsonSerializer.Deserialize<T>(bytes, options)!;
}
