package authz_test

import data.authz

# This test will pass.
test_ok := true

test_allow_whitelist if {
	authz.allow
		with input as { "roles": [], "action": "GET:/health" }
		with data.whitelist as ["GET:/health$"]
}

test_allow_viewer if {
	authz.allow
		with input as {"roles": ["viewer"], "action": "GET:/api/box"}
		with data.roles as {"viewer": ["templates.view_all"]}
		with data.permissions as {"templates.view_all": [ "GET:/api/box$"]}
}

test_allow_viewer_deny if {
	not authz.allow
		with input as {"roles": ["viewer"], "action": "GET:/api/entry/key?v=production/proxy"}
		with data.roles as {"viewer": ["templates.view_development_qa_global"]}
		with data.permissions as {"templates.view_development_qa_global": [ "^GET:/api/entry/key\\?v=(development|qa|global)/(.*)"]}
}
