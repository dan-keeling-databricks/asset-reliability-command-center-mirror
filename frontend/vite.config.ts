import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const backendUrl = env.VITE_BACKEND_URL || "http://127.0.0.1:8000";
  return {
    plugins: [react()],
    server: {
      host: "127.0.0.1",
      port: Number(env.FRONTEND_PORT || 5173),
      proxy: {
        "/api": backendUrl,
        "/mcp": backendUrl
      }
    }
  };
});
