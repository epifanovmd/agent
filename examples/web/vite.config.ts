// Разработка: интерфейс на :5173, API и файлы — на сервере примера (:8080).
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const server = process.env.DEMO_SERVER ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api/ws": { target: server, ws: true }, "/api": server, "/files": server } },
});
