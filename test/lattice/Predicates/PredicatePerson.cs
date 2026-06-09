namespace Orleans.Lattice.Tests.Predicates;

/// <summary>Shared POCO model for predicate push-down tests.</summary>
public sealed record PredicatePerson(
    string Name,
    int Age,
    bool Active,
    double Score,
    string? Nickname,
    PredicateAddress? Address);

/// <summary>Nested member used to exercise dotted member paths.</summary>
public sealed record PredicateAddress(string City, string Country);
