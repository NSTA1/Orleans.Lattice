namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// A read result for a Schema-area lookup (the enforcement policy, the version
/// config, the remediation status, or the compliance report). Carries the outcome
/// <see cref="Status"/>, an optional <see cref="Message"/> (populated on a denial or
/// failure), and the <see cref="Value"/> when the read succeeded. The services fold
/// a server denial or a transport failure into a non-success view rather than
/// throwing, so the UI never leaks an exception even when the advisory capability
/// map believed the read was allowed.
/// </summary>
/// <typeparam name="T">The read value type.</typeparam>
public sealed record SchemaReadView<T>
{
    /// <summary>The outcome category of the read.</summary>
    public required SchemaOperationStatus Status { get; init; }

    /// <summary>A human-readable message, populated on a denial or failure.</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>
    /// The read value, or the type default (for example <see langword="null"/> for a
    /// reference type, or a default struct) on a denial or failure. A successful read
    /// of an absent policy / config also yields the default, distinguished by
    /// <see cref="IsSuccess"/> being <see langword="true"/>.
    /// </summary>
    public T? Value { get; init; }

    /// <summary><see langword="true"/> when the read succeeded.</summary>
    public bool IsSuccess => Status == SchemaOperationStatus.Succeeded;

    /// <summary>Creates a successful view carrying <paramref name="value"/>.</summary>
    /// <param name="value">The read value.</param>
    public static SchemaReadView<T> Succeeded(T? value) =>
        new() { Status = SchemaOperationStatus.Succeeded, Value = value };

    /// <summary>Creates a denial view with <paramref name="message"/>.</summary>
    /// <param name="message">The denial message.</param>
    public static SchemaReadView<T> Denied(string message) =>
        new() { Status = SchemaOperationStatus.Denied, Message = message };

    /// <summary>Creates a failure view with <paramref name="message"/>.</summary>
    /// <param name="message">The failure message.</param>
    public static SchemaReadView<T> Failed(string message) =>
        new() { Status = SchemaOperationStatus.Failed, Message = message };
}
