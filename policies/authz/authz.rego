# METADATA
# title: Example
# description: Example package with documentation
package authz

import future.keywords.in
import future.keywords.if

default allow := false

default action_allowed := false

default roles := {"anonymous"}

roles := input.roles if {
	count(input.roles) > 0
}

# whitelist
allow if {
	some action in data.whitelist
	regex.match(action, input.action)
}

allow if {
	action_allowed
}

action_allowed if {
	some role in roles
	some permission in data.roles[role]
	some path in data.permissions[permission]
	regex.match(path, input.action)
}
