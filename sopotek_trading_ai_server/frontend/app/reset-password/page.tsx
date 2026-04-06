import { AuthPage } from "@/components/auth/auth-page";
import { redirectIfAuthenticated } from "@/lib/server-session";

export default async function ResetPasswordPage({
  searchParams
}: {
  searchParams: Promise<{ token?: string }>;
}) {
  await redirectIfAuthenticated();
  const resolvedSearchParams = await searchParams;
  return <AuthPage mode="reset-password" initialToken={resolvedSearchParams.token ?? ""} />;
}
