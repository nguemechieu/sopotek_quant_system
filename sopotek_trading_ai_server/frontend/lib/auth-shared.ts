export const AUTH_COOKIE_NAME = "sopotek_platform_token";
export const AUTH_STORAGE_KEY = "sopotek-platform-session";
export const AUTH_CHANGE_EVENT = "sopotek-auth-change";
export const AUTH_ROUTE_PREFIXES = ["/login", "/register", "/forgot-password", "/reset-password"] as const;

export type UserRole = "admin" | "trader" | "viewer";

export type AuthUser = {
  id: string;
  email: string;
  username: string;
  full_name: string | null;
  role: UserRole;
  is_active: boolean;
  created_at: string;
  updated_at: string;
};

export type AuthSession = {
  access_token: string;
  token_type: string;
  user: AuthUser;
};

export type ForgotPasswordResponse = {
  message: string;
  reset_token?: string | null;
  reset_url?: string | null;
};
