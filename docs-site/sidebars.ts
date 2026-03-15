import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/overview',
        'getting-started/rust',
        'getting-started/js-ts',
        'getting-started/tauri-react',
        'getting-started/c-ffi',
        'getting-started/pascal'
      ]
    },
    {
      type: 'category',
      label: 'API Reference',
      items: ['api/crud', 'api/queries', 'api/batches', 'api/transactions']
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/architecture-deep-dive',
        'guides/realtime',
        'guides/tauri-gateway',
        'guides/advanced-features'
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      items: ['reference/rust-api', 'reference/c-ffi-api']
    },
    {
      type: 'category',
      label: 'Appendices',
      items: ['appendices/storage-format', 'appendices/write-path']
    }
  ]
};

export default sidebars;
