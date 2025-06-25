## develop policy
```shell
opa eval --data policies/  --data data.json --input test-input.json "data.policies.rbac.allow" --explain=full


opa eval --data authz/policy.rego --data roles.json --data permissions.json --input test-input.json "data.authz.allow"

# format
opa fmt --write authz

# test
opa test authz --verbose --coverage
opa test authz --verbose
```