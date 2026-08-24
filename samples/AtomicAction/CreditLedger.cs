using System.Collections.Concurrent;

/// <summary>
/// A stand-in for an <b>external</b> system a custom saga step touches - here an
/// in-process credit ledger. In a real application this would be a payment gateway
/// or another service; the saga does not know or care, it just runs the registered
/// handler's forward and compensating effects. Both effects are keyed on the saga's
/// operation id so they are idempotent across a crash-resume.
/// </summary>
public sealed class CreditLedger
{
    private readonly ConcurrentDictionary<string, decimal> _reservedByOperation = new();

    /// <summary>Reserves <paramref name="amount"/> for <paramref name="operationId"/> (idempotent).</summary>
    public void Reserve(string operationId, string account, decimal amount) =>
        _reservedByOperation[operationId] = amount;

    /// <summary>Releases any reservation held for <paramref name="operationId"/> (idempotent).</summary>
    public void Release(string operationId) =>
        _reservedByOperation.TryRemove(operationId, out _);

    /// <summary>The amount currently reserved for <paramref name="operationId"/>, or 0.</summary>
    public decimal Reserved(string operationId) =>
        _reservedByOperation.TryGetValue(operationId, out var amount) ? amount : 0m;
}
