using System.Runtime.CompilerServices;

namespace Orleans.Lattice;

/// <summary>
/// The content equality every value-equality override in the library applies to
/// its <see cref="byte"/>-array members. A record's compiler-generated equality
/// compares an array member by reference, so two structurally identical records
/// built from independently allocated buffers - and, in particular, a record and
/// its post-serialization self - would otherwise never compare equal.
/// </summary>
internal static class ByteArrayEquality
{
    /// <summary>
    /// Returns <see langword="true"/> when both arrays are the same instance, both
    /// are <see langword="null"/>, or both are non-null with identical contents.
    /// </summary>
    /// <param name="left">The first array; may be <see langword="null"/>.</param>
    /// <param name="right">The second array; may be <see langword="null"/>.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool ContentEquals(byte[]? left, byte[]? right) =>
        ReferenceEquals(left, right)
        || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));
}
