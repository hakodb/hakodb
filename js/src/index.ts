// export {
//   FireLiteClient,
//   FireLiteConfig,      // NEW: Required for tuning durability/memory
//   DurabilityMode,      // NEW: Required for config
//   CollectionReference,
//   DocumentReference,
//   DocumentSnapshot,
//   Query,
//   WriteBatch,
//   type FireLiteDocData,
//   type FireLiteClientOptions,
//   type Unsubscribe,    // NEW: Useful for typing listener cleanup
//   type Primitive       // NEW: Useful for custom data types
// } from './client';

// export { 
//   loadNativeBindings, 
//   type NativeBindings,
//   type WatchCallback   // NEW: Useful for advanced FFI users
// } from './native';

// export {
//   TauriFireLite,
//   TauriCollectionReference,
//   TauriDocumentReference,
//   TauriDocumentSnapshot,
//   TauriQuery,
//   TauriWriteBatch,
//   type FireLiteRecord,
//   type FireLitePrimitive,
//   type FilterOperator
// } from './tauri';

export * from './client';
export * from './native';
export * from './tauri';