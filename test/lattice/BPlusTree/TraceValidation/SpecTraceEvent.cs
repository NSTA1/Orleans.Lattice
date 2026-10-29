namespace Orleans.Lattice.Tests.BPlusTree.TraceValidation;

/// <summary>
/// One step of a trace in the vocabulary of <c>AtomicCommit.tla</c>: the action
/// it claims, the saga and key it names, and the values it observed. Rendered
/// as a TLA+ record by <see cref="ToTla"/>.
/// </summary>
internal sealed record SpecTraceEvent(string Event, string Tx, string? Key, string? Outcome, string[]? Keys, string[]? Acks)
{
    /// <summary>A prepare fan-out over <paramref name="keys"/>, of which <paramref name="acks"/> acked.</summary>
    public static SpecTraceEvent Prepare(string tx, string[] keys, string[] acks) => new("Prepare", tx, null, null, keys, acks);

    /// <summary>The registry recording <paramref name="outcome"/> (<c>committed</c> or <c>aborted</c>).</summary>
    public static SpecTraceEvent Decide(string tx, string outcome) => new("Decide", tx, null, outcome, null, null);

    /// <summary>One key's leaf applying the terminal <paramref name="outcome"/> (<c>commit</c> or <c>abort</c>).</summary>
    public static SpecTraceEvent Terminal(string tx, string key, string outcome) => new("Terminal", tx, key, outcome, null, null);

    /// <summary>A leaf discarding a late orphan bucket behind an applied terminal.</summary>
    public static SpecTraceEvent OrphanDrain(string tx, string key) => new("OrphanDrain", tx, key, null, null, null);

    /// <summary>The registry retiring the saga's decision.</summary>
    public static SpecTraceEvent Forget(string tx) => new("Forget", tx, null, null, null, null);

    /// <summary>Renders this event as a TLA+ record literal.</summary>
    public string ToTla()
    {
        var fields = new List<string> { $"event |-> \"{Event}\"", $"tx |-> \"{Tx}\"" };
        if (Key is not null)
        {
            fields.Add($"key |-> \"{Key}\"");
        }
        if (Outcome is not null)
        {
            fields.Add($"outcome |-> \"{Outcome}\"");
        }
        if (Keys is not null)
        {
            fields.Add($"keys |-> {Set(Keys)}");
        }
        if (Acks is not null)
        {
            fields.Add($"acks |-> {Set(Acks)}");
        }
        return "[" + string.Join(", ", fields) + "]";
    }

    /// <inheritdoc />
    public override string ToString() => ToTla();

    private static string Set(IEnumerable<string> values) =>
        "{" + string.Join(", ", values.Select(v => $"\"{v}\"")) + "}";
}
