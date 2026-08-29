namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The result of a tenancy operation that returns a value: the
/// <see cref="TenantOperationResult.Status"/> and
/// <see cref="TenantOperationResult.Message"/> of the base result, plus the
/// <see cref="Value"/> the operation produced.
/// <para>
/// <see cref="Value"/> is meaningful only when
/// <see cref="TenantOperationResult.IsSuccess"/> is <see langword="true"/>; on
/// any refusal it is <see langword="default"/>, so a caller must branch on the
/// status rather than on the value being present.
/// </para>
/// </summary>
/// <typeparam name="TValue">The value the operation produces.</typeparam>
public sealed record TenantOperationResult<TValue> : TenantOperationResult
{
    /// <summary>
    /// The produced value on success, and <see langword="default"/> on every
    /// refusal.
    /// </summary>
    public TValue? Value { get; init; }

    /// <summary>
    /// Creates a success result carrying <paramref name="value"/> and
    /// <paramref name="message"/>.
    /// </summary>
    /// <param name="value">The produced value.</param>
    /// <param name="message">The success message. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static TenantOperationResult<TValue> Success(TValue value, string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TenantOperationResult<TValue>
        {
            Status = TenantOperationStatus.Succeeded,
            Message = message,
            Value = value,
        };
    }

    /// <summary>
    /// Creates a non-success result with <paramref name="status"/> and
    /// <paramref name="message"/> and no value.
    /// </summary>
    /// <param name="status">The outcome category.</param>
    /// <param name="message">The description of the outcome. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static new TenantOperationResult<TValue> Failure(TenantOperationStatus status, string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new TenantOperationResult<TValue> { Status = status, Message = message };
    }
}
