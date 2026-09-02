namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// A string-keyed grain used to exercise the string key codec and the
/// declaration surface. Never activated: the tests only need its interface
/// identity and key shape.
/// </summary>
public interface ITestStringKeyedGrain : IGrainWithStringKey;

/// <summary>A <see cref="Guid"/>-keyed grain used to exercise the Guid key codec.</summary>
public interface ITestGuidKeyedGrain : IGrainWithGuidKey;

/// <summary>An integer-keyed grain used to exercise the integer key codec.</summary>
public interface ITestIntegerKeyedGrain : IGrainWithIntegerKey;

/// <summary>
/// A compound-keyed grain, which no built-in codec can encode, used to exercise
/// the un-encodable-key failure.
/// </summary>
public interface ITestCompoundKeyedGrain : IGrainWithGuidCompoundKey;

/// <summary>
/// A grain declaring two key interfaces at once, used to exercise the ambiguous
/// key-shape failure.
/// </summary>
public interface ITestAmbiguouslyKeyedGrain : IGrainWithStringKey, IGrainWithGuidKey;

/// <summary>
/// A grain declaring integer and guid key interfaces at once, used to exercise
/// the ambiguous key-shape failure for integer-keyed grains specifically.
/// </summary>
public interface ITestIntegerAmbiguouslyKeyedGrain : IGrainWithIntegerKey, IGrainWithGuidKey;

/// <summary>
/// The grain state the declaration tests project from. Deliberately mixes a
/// value type, a reference type, and a nullable so the projected-property
/// descriptor is exercised across all three.
/// </summary>
public sealed class TestGrainState
{
    /// <summary>A value-type property.</summary>
    public int Age { get; set; }

    /// <summary>A reference-type property.</summary>
    public string Country { get; set; } = string.Empty;

    /// <summary>A nullable value-type property.</summary>
    public DateTimeOffset? LastSeen { get; set; }

    /// <summary>A property that is never included, so exclusion is observable.</summary>
    public string Secret { get; set; } = string.Empty;
}
