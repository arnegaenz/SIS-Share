const RAW_TEST_INSTANCE_NAMES = ["customer-dev"];

function canonicalInstance(value) {
  if (!value) return "";
  return value.toString().trim().toLowerCase().replace(/[^a-z0-9]/g, "");
}

const TEST_INSTANCE_SET = new Set(
  RAW_TEST_INSTANCE_NAMES.map((name) => canonicalInstance(name))
);

export function isTestInstanceName(value) {
  if (!value) return false;
  return TEST_INSTANCE_SET.has(canonicalInstance(value));
}

export function getTestInstanceNames() {
  return RAW_TEST_INSTANCE_NAMES.slice();
}
