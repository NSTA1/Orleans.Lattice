namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="KeyBuilder"/>: the append-only character buffer that backs
/// every index-entry key, and in particular the <c>Grow</c> paths exercised when
/// the initial stack span fills up.
/// </summary>
/// <remarks>
/// <c>KeyBuilder</c> is a <c>ref struct</c> so every test method is synchronous.
/// </remarks>
[TestFixture]
public sealed class KeyBuilderTests
{
    [Test]
    public void Appending_a_single_char_past_the_initial_span_grows_the_buffer()
    {
        // Arrange: a one-character initial buffer so the second Append must grow.
        Span<char> initial = stackalloc char[1];
        var builder = new KeyBuilder(initial);
        builder.Append('A');

        // Act: fill the buffer then append one more character - triggers Grow(1).
        builder.Append('B');

        // Assert: both characters are present after the grow.
        Assert.That(builder.ToString(), Is.EqualTo("AB"));
        builder.Dispose();
    }

    [Test]
    public void Appending_a_span_past_the_initial_buffer_grows_the_buffer()
    {
        // Arrange: a one-character initial buffer.
        Span<char> initial = stackalloc char[1];
        var builder = new KeyBuilder(initial);

        // Act: appending a three-character span triggers Grow(3).
        builder.Append("XYZ".AsSpan());

        // Assert: all three characters are present after the grow.
        Assert.That(builder.ToString(), Is.EqualTo("XYZ"));
        builder.Dispose();
    }

    [Test]
    public void Get_span_past_the_initial_buffer_grows_the_buffer()
    {
        // Arrange: a one-character initial buffer.
        Span<char> initial = stackalloc char[1];
        var builder = new KeyBuilder(initial);

        // Act: requesting two characters triggers Grow(2) via GetSpan.
        var span = builder.GetSpan(2);
        span[0] = 'P';
        span[1] = 'Q';
        builder.Advance(2);

        Assert.That(builder.ToString(), Is.EqualTo("PQ"));
        builder.Dispose();
    }

    [Test]
    public void Multiple_grow_cycles_return_previous_rented_arrays()
    {
        // Grow twice to exercise the branch that returns the previously rented array.
        Span<char> initial = stackalloc char[1];
        var builder = new KeyBuilder(initial);
        builder.Append('A');            // fills the initial one-char span
        builder.Append('B');            // Grow(1) - rents the first array
        builder.Append("CCCCCCCC".AsSpan()); // Grow(8) - rents a second, returns the first

        Assert.That(builder.ToString(), Is.EqualTo("ABCCCCCCCC"));
        builder.Dispose();
    }
}
