namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A card as a plain-language sentence, such as
/// "<c>order.total</c> must be a number between 0 and 10,000". The subject is
/// kept apart from the rest so it can be set in mono; nested cards (every item,
/// any of) carry their own sentences.
/// </summary>
/// <param name="Subject">The member path set in mono, or <see langword="null"/> when <see cref="Lead"/> names the subject in words.</param>
/// <param name="Lead">The words before the subject, such as "The value" (with no subject) or "Every item of".</param>
/// <param name="Text">The rest of the sentence.</param>
/// <param name="Inner">Nested sentences: the item's card, or the alternatives.</param>
internal sealed record SchemaCardSentence(string? Subject, string Lead, string Text, IReadOnlyList<SchemaCardSentence> Inner)
{
    /// <summary>The whole sentence as plain text, nested sentences included.</summary>
    /// <returns>The text.</returns>
    public string Plain()
    {
        var parts = new List<string>(4);
        if (Lead.Length > 0)
        {
            parts.Add(Lead);
        }

        if (Subject is not null)
        {
            parts.Add(Subject);
        }

        if (Text.Length > 0)
        {
            parts.Add(Text);
        }

        if (Inner.Count > 0)
        {
            parts.Add(string.Join(" or ", Inner.Select(inner => inner.Plain())));
        }

        return string.Join(' ', parts);
    }
}
