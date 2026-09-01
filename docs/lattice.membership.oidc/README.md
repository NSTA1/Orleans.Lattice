# Orleans.Lattice.Membership.Oidc

Generic OpenID Connect credential authenticator for [Orleans.Lattice.Membership](../lattice.membership/README.md).

## What is it?

`Orleans.Lattice.Membership.Oidc` adds a provider-agnostic OIDC authenticator that plugs into the membership credential-authenticator seam. It specializes the built-in JWT authenticator rather than reimplementing token validation, so it inherits the same audience, signing-key, and lifetime checks and only layers on the OIDC-specific concerns. Nothing in it is tied to a particular vendor: everything it needs about a provider is read from that provider's OpenID Connect discovery document, so Okta, Auth0, Keycloak, Ping, Google, and any other conformant issuer are configured the same way.

It is an **additive sibling** to [`Orleans.Lattice.Membership.Entra`](../lattice.membership.entra/README.md), not a replacement. Neither package depends on the other, and a silo can register both - plus the basic and anonymous authenticators - at the same time.

## What it does

- **Discovery-document-driven metadata and signing-key rotation.** Validation parameters are resolved from the provider's OpenID Connect discovery document. The JSON Web Key Set is cached and refreshed on its own interval through a configuration manager, so signing keys rotate automatically without a metadata fetch on every call.
- **Exact-issuer selection and validation.** The authenticator claims a credential only when the token's `iss` claim is an ordinal exact match for the configured `Issuer`, or when the credential carries the explicitly configured `SchemeHint`. There is no prefix form, no wildcard, and no catch-all, so registering two OIDC issuers on one silo is unambiguous regardless of registration order, and a generic OIDC authenticator never claims an Entra token. The same exactness is enforced during validation, not just selection: the issuer the discovery document advertises is *not* accepted unless it is also the configured issuer.
- **Fail-closed signature-algorithm pinning.** The accepted `alg` set is pinned on every validation. It comes from `Algorithms` when that is populated and from the discovery document's `id_token_signing_alg_values_supported` otherwise. An empty set means *reject every token*, never *accept any algorithm* - which is what closes the algorithm-confusion gap (CWE-347) that an unpinned validator leaves open, including the classic attack of re-signing a token with `HS256` using the provider's published public key as the HMAC secret.
- **Standard OIDC claim conventions.** The subject is taken from `sub`, and group membership from `groups`, `roles`, and `role`. Both lists are configurable per issuer, so a provider that emits membership under a namespaced claim such as `https://example.com/claims/teams` needs configuration, not code. Every other token claim is copied verbatim into the subject's flat claim bag.
- **Reserved-subject safety.** A validated token whose `sub` is missing, or collides with a reserved well-known sentinel (`anonymous` or `system`), resolves to the anonymous subject with no groups rather than to an anonymous-labelled principal that still carries the token's groups.

## Registration

Register the authenticator on the silo builder **after** the base membership services. Registration guards this ordering and fails fast with a clear message when membership is not present. When the add-on is not registered it has zero runtime cost.

```csharp verify
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Oidc;

siloBuilder.AddLatticeMembership();

siloBuilder.AddLatticeOidc(options =>
{
    options.Authority = "https://dev-123456.okta.com/oauth2/default";
    options.Issuer = "https://dev-123456.okta.com/oauth2/default";
    options.Audiences.Add("api://lattice");
});
```

Call it once per issuer. Several OIDC authenticators coexist, and because selection is an exact issuer match they never compete for the same token:

```csharp verify
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Oidc;

siloBuilder.AddLatticeMembership();

siloBuilder.AddLatticeOidc(options =>
{
    options.Authority = "https://dev-123456.okta.com/oauth2/default";
    options.Issuer = "https://dev-123456.okta.com/oauth2/default";
    options.Audiences.Add("api://lattice");
    options.SchemeHint = "okta";
});

siloBuilder.AddLatticeOidc(options =>
{
    options.Authority = "https://keycloak.example.com/realms/lattice";
    options.Issuer = "https://keycloak.example.com/realms/lattice";
    options.Audiences.Add("lattice-api");
    options.GroupClaimTypes.Clear();
    options.GroupClaimTypes.Add("groups");
    options.SchemeHint = "keycloak";
});
```

> **Claim types are matched by exact name, not by JSON path.** A claim type is
> compared against the claim names the validated token actually produced, and
> the token handler does not flatten nested JSON objects into dotted names. A
> Keycloak realm role therefore cannot be read with
> `GroupClaimTypes.Add("realm_access.roles")`: the token carries `realm_access`
> as a single nested object, no claim is ever named `realm_access.roles`, and
> the entry silently matches nothing - so the caller is resolved with **no**
> asserted groups. This fails closed (it under-grants, never over-grants), but
> it fails silently, so configure the provider to emit the memberships as a
> top-level claim instead. In Keycloak that is a dedicated group or realm-role
> protocol mapper on the client, with "Token Claim Name" set to a flat name such
> as `groups` and "Add to access token" enabled.

## Choosing between this package and the Entra package

| Situation | Use |
|---|---|
| Tokens are issued by Microsoft Entra ID (Azure AD) | [`Orleans.Lattice.Membership.Entra`](../lattice.membership.entra/README.md) - it adds tenant allow-listing, the templated multi-tenant issuer, `oid` subject mapping, and groups-overage resolution, none of which is expressible generically. |
| Tokens are issued by any other conformant OIDC provider | This package. |
| Both, on the same silo | Both. They are independent packages and their authenticators never claim each other's tokens. |

## Security notes

- **Audience validation is always on.** There is no `ValidateAudience` switch. `Audiences` must contain at least one entry, and an empty list throws at construction rather than silently accepting a token minted for a different relying party.
- **Algorithm pinning is always on.** See "fail-closed signature-algorithm pinning" above. Pin `Algorithms` explicitly when you want a set narrower than what the provider advertises.
- **A `SchemeHint` short-circuits issuer selection.** When a credential's scheme matches the hint, this authenticator claims the credential before the issuer is read. The token is still fully validated - a foreign token claimed this way fails validation and resolves to anonymous - but it no longer falls through to another authenticator. Leave `SchemeHint` unset unless a head genuinely tags its credentials.
- **Selection parses the token, and that parse is bounded.** Unlike the base JWT authenticator, a scheme that does not match the hint does not end selection: it falls through to the exact-issuer match, because the credential bridges stamp the scheme from operator configuration rather than from the caller, so an authenticator that leaves `SchemeHint` unset would otherwise never be selected. Selection therefore reads the token on a pre-authentication path that runs once per registered authenticator. To keep that from being an amplification lever for an unauthenticated caller, a credential longer than the validating handler's own maximum token size (256,000 characters) is declined without being parsed - a credential that large would have been rejected by validation regardless, so nothing that could have authenticated is ever turned away.

## Reference

- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Membership documentation](../lattice.membership/README.md) - the base identity and authorization add-on this package extends.
- [Entra authenticator](../lattice.membership.entra/README.md) - the Microsoft Entra ID sibling.
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the policy store and access gate that consume the subjects this package resolves.
