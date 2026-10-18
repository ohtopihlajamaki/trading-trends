import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// In compose the gateway is reached by service name; every instance of a
// scaled gateway answers on it.
const gateway = process.env.GATEWAY_URL ?? 'http://localhost:8000'

export default defineConfig({
  plugins: [vue({ template: { compilerOptions: { isCustomElement: (tag) => tag.startsWith('perspective-') } } })],
  server: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    // Bind mounts from macOS and Windows hosts do not deliver file events
    watch: { usePolling: true, interval: 300 },
    proxy: {
      '/ws': { target: gateway.replace(/^http/, 'ws'), ws: true },
      '/history': gateway,
      '/signals': gateway,
      '/symbols': gateway,
    },
  },
  // The two Perspective packages that load WebAssembly are served as they
  // are; the datagrid (and its CommonJS dependencies) is pre-bundled
  optimizeDeps: { exclude: ['@finos/perspective', '@finos/perspective-viewer'] },
  build: { target: 'es2022' },
})
