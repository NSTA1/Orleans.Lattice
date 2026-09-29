namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>The outcome of a replication read: a value, or the fault that stood in its way.</summary>
/// <typeparam name="T">The value's type.</typeparam>
internal sealed class ReplicationRead<T>
    where T : class
{
    private ReplicationRead(T? value, ReplicationFault? fault)
    {
        Value = value;
        Fault = fault;
    }

    /// <summary>The value, when the read succeeded.</summary>
    public T? Value { get; }

    /// <summary>The fault, when it did not.</summary>
    public ReplicationFault? Fault { get; }

    /// <summary>Whether the read succeeded.</summary>
    public bool Succeeded => Value is not null;

    /// <summary>A successful read.</summary>
    /// <param name="value">The value.</param>
    public static ReplicationRead<T> Success(T value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new(value, null);
    }

    /// <summary>A failed read.</summary>
    /// <param name="fault">The fault.</param>
    public static ReplicationRead<T> Failure(ReplicationFault fault)
    {
        ArgumentNullException.ThrowIfNull(fault);
        return new(null, fault);
    }
}
