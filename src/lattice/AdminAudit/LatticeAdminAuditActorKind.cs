namespace Orleans.Lattice;

/// <summary>
/// How the actor of a <see cref="LatticeAdminAuditRecord"/> holds its authority.
/// Values are explicit and persisted, so a later finer split of
/// <see cref="Subject"/> only appends values.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAdminAuditActorKind)]
public enum LatticeAdminAuditActorKind
{
    /// <summary>An authenticated subject authorized by policy.</summary>
    Subject = 0,

    /// <summary>A configured bootstrap administrator: the root of trust, which bypasses policy.</summary>
    BootstrapAdministrator = 1,
}
