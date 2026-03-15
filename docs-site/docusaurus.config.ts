import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'FireLite Developer & API Reference',
  tagline: 'Embedded document database docs for Rust, JS/TS, Tauri, C-FFI, and Pascal',
  url: 'https://firelite.dev',
  baseUrl: '/',
  organizationName: 'firelite',
  projectName: 'firelite-docs',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  i18n: {
    defaultLocale: 'en',
    locales: ['en']
  },
  presets: [
    [
      'classic',
      {
        docs: {
          path: 'docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.ts'
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css'
        }
      } satisfies Preset.Options
    ]
  ],
  themeConfig: {
    navbar: {
      title: 'FireLite Docs',
      items: [{ type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs' }]
    }
  } satisfies Preset.ThemeConfig
};

export default config;
