import { AuthPage } from "@/components/auth/auth-page";
import { redirectIfAuthenticated } from "@/lib/server-session";

export default async function ForgotPasswordPage() {
  await redirectIfAuthenticated();
  return <AuthPage mode="forgot-password" />;
}
