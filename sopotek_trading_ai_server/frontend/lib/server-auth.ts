import "server-only";

import { cookies } from "next/headers";

import { AUTH_COOKIE_NAME } from "@/lib/auth-shared";

const envApiToken = process.env.SOPOTEK_API_TOKEN ?? process.env.NEXT_PUBLIC_SOPOTEK_API_TOKEN ?? "";

export async function getServerApiToken(): Promise<string> {
  const cookieStore = await cookies();
  return cookieStore.get(AUTH_COOKIE_NAME)?.value ?? envApiToken;
}
