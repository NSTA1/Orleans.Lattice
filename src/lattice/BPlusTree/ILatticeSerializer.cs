namespace Orleans.Lattice;

/// <summary>
/// Serializes and deserializes values of type <typeparamref name="T"/> to and
/// from <c>byte[]</c> for storage in a Lattice tree.
/// </summary>
/// <typeparam name="T">The value type to serialize.</typeparam>
public interface ILatticeSerializer<T>
{
    /// <summary>Serializes <paramref name="value"/> to a byte array.</summary>
    byte[] Serialize(T value);

    /// <summary>Deserializes a byte array back to <typeparamref name="T"/>.</summary>
    T Deserialize(byte[] bytes);
}
