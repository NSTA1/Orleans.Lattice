# Entra ID setup guide (Azure CLI)

This guide provisions everything a silo needs to authenticate real Microsoft
Entra ID (Azure AD) users against Orleans.Lattice, using only the
[Azure CLI](https://learn.microsoft.com/cli/azure/). It is the companion setup
for the [`EntraAuthorization` sample](../../samples/EntraAuthorization/README.md),
which acquires a token for the signed-in `az` user and writes a value to a tree
as that Entra identity.

The [`Orleans.Lattice.Membership.Entra`](README.md) authenticator validates the
tokens produced here; the broader security model is described in
[docs/lattice/security.md](../lattice/security.md).

## What you will create

- An **app registration** that represents your Lattice service and defines the
  audience its tokens are issued for.
- An **exposed API scope** (`access_as_user`) so a user can request a token for
  that app.
- A **pre-authorization** of the Azure CLI so `az` (and the sample's
  `AzureCliCredential`) can acquire a token for your app without a separate
  consent prompt.

Everything is scoped to one tenant, uses delegated (user) tokens, and can be
deleted in a single command when you are done.

## Prerequisites

- The Azure CLI, signed in: `az login`.
- Permission to create app registrations in the tenant (the **Application
  Developer** role or higher). In a personal or trial tenant you already have
  this.

The commands below are written for **PowerShell**. They capture ids into
variables so you can paste the block as-is.

## Step 1 - Sign in and record the tenant

```powershell
az login
$tenantId = az account show --query tenantId -o tsv
Write-Host "Tenant: $tenantId"
```

## Step 2 - Create the app registration

```powershell
$appId = az ad app create `
  --display-name "Orleans.Lattice Entra Sample" `
  --sign-in-audience AzureADMyOrg `
  --query appId -o tsv
$objId = az ad app show --id $appId --query id -o tsv
Write-Host "App (client) id: $appId"
```

`AzureADMyOrg` makes the app single-tenant. `$appId` is both the client id and,
below, the token audience. `$objId` is the application's Graph object id, which
the `az rest` calls below patch.

> The `az rest` calls target the app by its object id
> (`applications/$objId`) and write the request body to a temp file
> (`--body "@file.json"`). On Windows this is more robust than the
> `applications(appId='...')` URI form (whose parentheses the shell can mangle)
> and than inline JSON (whose quotes the shell can mangle).

## Step 3 - Issue v2.0 access tokens

The authenticator expects Entra **v2.0** tokens (subject in the `oid` claim,
issuer `https://login.microsoftonline.com/{tenantid}/v2.0`). Set the app to emit
them:

```powershell
@{ api = @{ requestedAccessTokenVersion = 2 } } | ConvertTo-Json | Set-Content $env:TEMP\v2.json
az rest --method PATCH `
  --uri "https://graph.microsoft.com/v1.0/applications/$objId" `
  --body "@$env:TEMP\v2.json"
```

## Step 4 - Expose an API scope

Set the Application ID URI to `api://<appId>` and add a delegated
`access_as_user` scope that users can request a token for:

```powershell
az ad app update --id $appId --identifier-uris "api://$appId"

$scopeId = [guid]::NewGuid().ToString()
$scope = @{
  api = @{
    oauth2PermissionScopes = @(
      @{
        id = $scopeId
        value = "access_as_user"
        type = "User"
        isEnabled = $true
        adminConsentDisplayName = "Access Orleans.Lattice as the signed-in user"
        adminConsentDescription = "Allows the app to access Orleans.Lattice on behalf of the signed-in user."
        userConsentDisplayName = "Access Orleans.Lattice on your behalf"
        userConsentDescription = "Allows the app to access Orleans.Lattice on your behalf."
      }
    )
  }
}
$scope | ConvertTo-Json -Depth 6 | Set-Content $env:TEMP\scope.json
az rest --method PATCH `
  --uri "https://graph.microsoft.com/v1.0/applications/$objId" `
  --body "@$env:TEMP\scope.json"
```

## Step 5 - Pre-authorize the Azure CLI

So the Azure CLI can request a token for your scope without an interactive
consent, add the CLI's well-known client id
(`04b07795-8ddb-461a-bbee-02f9e1bf7b46`) as a pre-authorized application for the
scope you just created:

```powershell
$azureCliClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
$preauth = @{
  api = @{
    preAuthorizedApplications = @(
      @{ appId = $azureCliClientId; delegatedPermissionIds = @($scopeId) }
    )
  }
}
$preauth | ConvertTo-Json -Depth 6 | Set-Content $env:TEMP\preauth.json
az rest --method PATCH `
  --uri "https://graph.microsoft.com/v1.0/applications/$objId" `
  --body "@$env:TEMP\preauth.json"
```

Graph merges the `api` object, so this patch preserves the token version and
scope from the previous steps.

## Step 6 - (Optional) emit a groups claim

The sample authorizes a single user by object id, so groups are not required. If
you want the token to also carry the caller's security-group ids (so a rule can
target a group), turn on the groups claim and add yourself to a group:

```powershell
@{ groupMembershipClaims = "SecurityGroup" } | ConvertTo-Json | Set-Content $env:TEMP\groups.json
az rest --method PATCH `
  --uri "https://graph.microsoft.com/v1.0/applications/$objId" `
  --body "@$env:TEMP\groups.json"
```

Large group memberships overflow the token; that case is resolved out of band by
the separate [Graph group resolver](../lattice.membership.entra.graph/README.md).

## Step 7 - Export the ids for the sample

```powershell
$env:LATTICE_ENTRA_TENANT_ID = $tenantId
$env:LATTICE_ENTRA_CLIENT_ID = $appId
```

The sample defaults to the scope `api://<clientId>/.default`. To request the
explicit scope instead, also set `$env:LATTICE_ENTRA_SCOPE =
"api://$appId/access_as_user"`.

## Register the authenticator in a silo

Register the Entra authenticator **after** membership. The values map directly to
the ids above - one tenant in the allow-list, the app (client) id as the
audience:

```
siloBuilder.AddLatticeMembership();
siloBuilder.AddEntraCredentialAuthenticator(options =>
{
    options.Authority = $"https://login.microsoftonline.com/{tenantId}/v2.0";
    options.TenantIds.Add(tenantId);
    options.Audiences.Add(clientId);            // the app (client) id
    options.Audiences.Add($"api://{clientId}"); // and its Application ID URI
});
siloBuilder.AddLatticeAuth(options => options.DefaultEffect = LatticeEffect.Deny);
```

The authenticator selects a token by matching its tenant against `TenantIds` and
its issuer against the templated v2.0 issuer, then validates the audience,
signature (via OIDC/JWKS discovery from `Authority`), and lifetime. The full
list of options - multi-tenant allow-lists, a `SchemeHint`, clock skew, and JWKS
refresh intervals - is on `LatticeEntraAuthenticatorOptions`. See the
[`EntraAuthorization` sample](../../samples/EntraAuthorization/Program.cs) for a
complete, compiled host that acquires a token and enforces a rule end to end.

## Acquiring a token yourself

The sample uses `AzureCliCredential`, but you can mint the same token by hand to
inspect it:

```powershell
az account get-access-token --scope "api://$appId/.default" --query accessToken -o tsv
```

Paste the result into [jwt.ms](https://jwt.ms) and confirm `ver` is `2.0`, `aud`
is your app id, `iss` ends in `/v2.0`, and an `oid` claim is present - that `oid`
is the subject id Lattice resolves the caller to.

## (Optional) Graph app-only setup for the identity directory

The steps above provision **delegated (user) tokens** only - no client secret and
no application permissions. The Microsoft Graph-backed group-overflow resolver and
identity directory in
[`Orleans.Lattice.Membership.Entra.Graph`](../lattice.membership.entra.graph/README.md)
authenticate to Graph **app-only**, so they need extra setup on this same app
registration: either a client secret (the confidential-client path) or a
`TokenCredential` (the secret-less path), plus the `User.Read.All` and
`Group.Read.All` Microsoft Graph **application** permissions with admin consent.

Add a client secret for the confidential-client path (record the printed value
once - it is not retrievable later):

```powershell
$graphSecret = az ad app credential reset --id $appId --append `
  --display-name "lattice-graph-app-only" --query password -o tsv
Write-Host "Graph client secret: $graphSecret"
```

Add the two Microsoft Graph application permissions and grant admin consent
(`00000003-0000-0000-c000-000000000000` is the well-known Microsoft Graph app id;
the two GUIDs are the `User.Read.All` and `Group.Read.All` **application** roles):

```powershell
az ad app permission add --id $appId `
  --api 00000003-0000-0000-c000-000000000000 `
  --api-permissions df021288-bdef-4463-88db-98f22de89214=Role `
                    5b567255-7703-4780-807c-7be8301ae99b=Role
az ad app permission admin-consent --id $appId
```

These map to the resolver options: `$tenantId` -> `TenantId`, `$appId` ->
`ClientId`, `$graphSecret` -> `ClientSecret`. For the secret-less path, skip the
secret and set `Credential` to a `TokenCredential` instead (the application
permissions and admin consent are still required).

## Troubleshooting

- **`AADSTS65001` / consent required** - Step 5 was skipped or targeted the wrong
  scope id. Re-run it with the `$scopeId` from Step 4.
- **Token resolves to anonymous in the sample** - the token's `tid` does not match
  `LATTICE_ENTRA_TENANT_ID`, or `aud` is not in the configured `Audiences`. Decode
  the token at [jwt.ms](https://jwt.ms) and compare.
- **`ver` is `1.0`** - Step 3 did not apply; a v1 token's issuer is
  `https://sts.windows.net/{tid}/` and will not match the v2.0 issuer template.

## Clean up

```powershell
az ad app delete --id $appId
```

## Reference

- [`Orleans.Lattice.Membership.Entra`](README.md) - the authenticator this guide configures.
- [`EntraAuthorization` sample](../../samples/EntraAuthorization/README.md) - the runnable end-to-end demo.
- [Security overview](../lattice/security.md) - how identity, authorization, and enforcement fit together.
