export const TERMINATION_RULES = {
  BILLABLE: {
    label: "Successful",
    includeInHealth: true,
    includeInUx: false,
    severity: "success",
  },
  SITE_INTERACTION_FAILURE: {
    label: "Automation / site failed",
    includeInHealth: true,
    includeInUx: false,
    severity: "site-failure",
  },
  UNSUCCESSFUL: {
    label: "Ran but didn’t complete",
    includeInHealth: true,
    includeInUx: false,
    severity: "site-failure",
  },

  // ===== UX / user-driven stuff =====
  USER_DATA_FAILURE: {
    label: "Bad or missing user data",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  NEVER_STARTED: {
    label: "User didn’t proceed",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  TIMEOUT_CREDENTIALS: {
    label: "User didn’t finish login",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  TIMEOUT_TFA: {
    label: "User didn’t finish MFA",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  ABANDONED_QUICKSTART: {
    label: "User bailed from QuickStart",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  CANCELED: {
    label: "User canceled",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  ACCOUNT_SETUP_INCOMPLETE: {
    label: "User didn’t finish setup",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  TOO_MANY_LOGIN_FAILURES: {
    label: "User kept failing login",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  ACCOUNT_LOCKED: {
    label: "User account locked",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  PASSWORD_RESET_REQUIRED: {
    label: "Password reset needed",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },
  INVALID_CARD_DETAILS: {
    label: "Bad card info",
    includeInHealth: false,
    includeInUx: true,
    severity: "ux",
  },

  UNKNOWN: {
    label: "Unknown",
    includeInHealth: false,
    includeInUx: false,
    severity: "unknown",
  },
};
