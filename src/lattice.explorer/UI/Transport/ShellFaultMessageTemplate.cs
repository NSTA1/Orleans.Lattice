namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The fixed shape of a facade exception's message, learned from the exception
/// type itself, so a status detail that a binding carried over the wire can be
/// recognised as that exception and its arguments recovered without restating the
/// sentence here.
/// </summary>
/// <remarks>
/// <para>
/// The template is built once by asking the exception for its message with two
/// sentinel arguments, then splitting that message at the sentinels. A detail
/// matches when it is exactly the template with some text in place of each
/// sentinel. The client and the server share the abstractions assembly the
/// exception lives in, so the shape is the one the server rendered.
/// </para>
/// <para>
/// Matching allocates only the recovered arguments, and only on a fault path.
/// </para>
/// </remarks>
internal sealed class ShellFaultMessageTemplate
{
    private const string FirstSentinel = "\u0001";
    private const string SecondSentinel = "\u0002";

    private readonly string[] _literals;
    private readonly int[] _slots;

    private ShellFaultMessageTemplate(string[] literals, int[] slots)
    {
        _literals = literals;
        _slots = slots;
    }

    /// <summary>Learns the template of a message with one argument.</summary>
    /// <param name="message">Renders the message for an argument.</param>
    /// <returns>The template.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static ShellFaultMessageTemplate Create(Func<string, string> message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return Create(message(FirstSentinel));
    }

    /// <summary>Learns the template of a message with two arguments.</summary>
    /// <param name="message">Renders the message for the first and second argument.</param>
    /// <returns>The template.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static ShellFaultMessageTemplate Create(Func<string, string, string> message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return Create(message(FirstSentinel, SecondSentinel));
    }

    /// <summary>
    /// Whether <paramref name="detail"/> is this template's message, recovering the
    /// arguments it was rendered with.
    /// </summary>
    /// <param name="detail">The status detail.</param>
    /// <param name="first">The first argument, or empty when the detail does not match.</param>
    /// <param name="second">The second argument, or empty when the template has one argument or the detail does not match.</param>
    /// <returns><see langword="true"/> when the detail matches.</returns>
    public bool TryMatch(string? detail, out string first, out string second)
    {
        first = string.Empty;
        second = string.Empty;
        if (detail is null || !detail.StartsWith(_literals[0], StringComparison.Ordinal))
        {
            return false;
        }

        var position = _literals[0].Length;
        for (var slot = 0; slot < _slots.Length; slot++)
        {
            var next = _literals[slot + 1];
            int end;
            if (slot == _slots.Length - 1)
            {
                if (!detail.EndsWith(next, StringComparison.Ordinal) || detail.Length - next.Length < position)
                {
                    return false;
                }

                end = detail.Length - next.Length;
            }
            else
            {
                end = detail.IndexOf(next, position, StringComparison.Ordinal);
                if (end < 0)
                {
                    return false;
                }
            }

            var value = detail[position..end];
            if (_slots[slot] == 0)
            {
                first = value;
            }
            else
            {
                second = value;
            }

            position = end + next.Length;
        }

        return true;
    }

    private static ShellFaultMessageTemplate Create(string rendered)
    {
        var literals = new List<string>(3);
        var slots = new List<int>(2);
        var position = 0;
        while (true)
        {
            var first = rendered.IndexOf(FirstSentinel, position, StringComparison.Ordinal);
            var second = rendered.IndexOf(SecondSentinel, position, StringComparison.Ordinal);
            var next = first < 0 ? second : second < 0 ? first : Math.Min(first, second);
            if (next < 0)
            {
                literals.Add(rendered[position..]);
                break;
            }

            literals.Add(rendered[position..next]);
            slots.Add(next == first ? 0 : 1);
            position = next + 1;
        }

        if (slots.Count == 0)
        {
            throw new ArgumentException("The message does not carry its arguments.", nameof(rendered));
        }

        return new ShellFaultMessageTemplate([.. literals], [.. slots]);
    }
}
