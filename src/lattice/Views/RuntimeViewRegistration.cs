namespace Orleans.Lattice.Views;

/// <summary>
/// Durable record of a materialised view created at runtime through
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>.
/// Persisted by the
/// <see cref="IViewRegistryGrain"/> so a runtime view can be re-registered into
/// the in-memory <see cref="IViewCatalog"/> and have its maintainer re-activated
/// after a silo restart, giving runtime views the same restart-durability that
/// startup-declared views get from <c>AddLatticeViews</c>.
/// <para>
/// A projection instance cannot be serialized. New records persist a configured
/// provider key and bounded opaque payload so the host can faithfully reconstruct
/// the complete definition. Legacy records retain the concrete CLR type
/// (<see cref="ProjectionTypeName"/>) for allow-listed type/DI reconstruction.
/// Both paths require the reconstructed projection to match the persisted
/// <see cref="ProjectionVersion"/> exactly.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RuntimeViewRegistration)]
[Immutable]
internal sealed record RuntimeViewRegistration
{
    /// <summary>The logical view name; the view tree is <c>view-{ViewName}</c>.</summary>
    [Id(0)]
    public required string ViewName { get; init; }

    /// <summary>The source tree id whose WAL the view tails.</summary>
    [Id(1)]
    public required string SourceTreeId { get; init; }

    /// <summary>
    /// The projection's concrete CLR type identity, captured as its version-free
    /// <see cref="System.Type.FullName"/> (the namespace-qualified type name) so it
    /// can be re-resolved from the silo service provider on re-hydration without
    /// pinning the projection assembly's version - a package bump must not strand
    /// the view. Records written by older builds hold an
    /// <see cref="System.Type.AssemblyQualifiedName"/> instead; re-hydration
    /// recovers the full name embedded in it, so those still resolve.
    /// </summary>
    [Id(2)]
    public required string ProjectionTypeName { get; init; }

    /// <summary>The projection's stable version at the time the view was created.</summary>
    [Id(3)]
    public required string ProjectionVersion { get; init; }

    /// <summary>Whether this view is an aggregation (grouped reduce) view.</summary>
    [Id(4)]
    public bool IsAggregation { get; init; }

    /// <summary>
    /// Whether this view is append-only (a durable history substrate). Restored
    /// onto the re-hydrated <see cref="ViewRegistration"/> so the maintainer keeps
    /// its non-destructive guard behaviour across a silo restart.
    /// </summary>
    [Id(5)]
    public bool Accumulative { get; init; }

    /// <summary>
    /// The host-configured provider key used for faithful reconstruction, or
    /// <see langword="null"/> for a legacy type-based registration.
    /// </summary>
    [Id(6)]
    public string? ProjectionProviderKey { get; init; }

    /// <summary>The bounded opaque state supplied to the configured provider.</summary>
    [Id(7)]
    public byte[]? ProjectionProviderPayload { get; init; }

    /// <summary>
    /// Compares two registrations by structure, including a content comparison of
    /// <see cref="ProjectionProviderPayload"/>. The compiler-generated record
    /// equality compares the <c>byte[]</c> payload with
    /// <see cref="EqualityComparer{T}.Default"/>, which for an array is reference
    /// equality. <see cref="LatticeRuntimeViewProjectionDescriptor.Payload"/>
    /// returns a fresh array on every access, so re-issuing the same
    /// <c>CreateAsync</c> would produce a registration whose payload is
    /// content-identical but a distinct instance - defeating the idempotent
    /// re-registration dedup guard in
    /// <see cref="IViewRegistryGrain.RegisterAsync(RuntimeViewRegistration)"/> and
    /// forcing a redundant durable write.
    /// </summary>
    public bool Equals(RuntimeViewRegistration? other) =>
        other is not null
        && string.Equals(ViewName, other.ViewName, StringComparison.Ordinal)
        && string.Equals(SourceTreeId, other.SourceTreeId, StringComparison.Ordinal)
        && string.Equals(ProjectionTypeName, other.ProjectionTypeName, StringComparison.Ordinal)
        && string.Equals(ProjectionVersion, other.ProjectionVersion, StringComparison.Ordinal)
        && IsAggregation == other.IsAggregation
        && Accumulative == other.Accumulative
        && string.Equals(ProjectionProviderKey, other.ProjectionProviderKey, StringComparison.Ordinal)
        && PayloadEqual(ProjectionProviderPayload, other.ProjectionProviderPayload);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ViewName, StringComparer.Ordinal);
        hash.Add(SourceTreeId, StringComparer.Ordinal);
        hash.Add(ProjectionTypeName, StringComparer.Ordinal);
        hash.Add(ProjectionVersion, StringComparer.Ordinal);
        hash.Add(IsAggregation);
        hash.Add(Accumulative);
        hash.Add(ProjectionProviderKey, StringComparer.Ordinal);
        if (ProjectionProviderPayload is { } payload)
        {
            hash.AddBytes(payload);
        }

        return hash.ToHashCode();
    }

    private static bool PayloadEqual(byte[]? left, byte[]? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null)
        {
            return false;
        }

        return left.AsSpan().SequenceEqual(right);
    }
}
