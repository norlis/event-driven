#!/bin/sh

: "${GIT_HOOKS_PATH:=.git/hooks}"

echo "creating pre-commit ${GIT_HOOKS_PATH}/pre-commit"

cat <<EOF > "${GIT_HOOKS_PATH}/pre-commit"
#!/bin/sh
set -euo pipefail


echo "  _____ _____ _____ _____ _____ _____ _____ _____ _____ _____ "
echo "|  running                                                    |"
echo "|  -> lint-static-check                                       |"
echo "|  -> check-format                                           |"
echo "| _____ _____ _____ _____ _____ _____ _____ _____ _____ _____ |"


make lint-static-check check-format
EOF

chmod +x "${GIT_HOOKS_PATH}/pre-commit"