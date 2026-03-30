import { readdir, readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { stripOfIllegalChars } from '$lib/globalHelpers';

export interface FilamentDatabase {
  brands: Record<string, Brand>;
  stores: Record<string, Store>;
}

interface Brand {
  id: string;
  name: string;
  logo: string;
  website?: string;
  origin?: string;
  materials: Record<string, Material>;
}

interface Store {
  id: string;
  name: string;
  storefront_url: string;
  logo: string;
  ships_from: string[];
  ships_to: string[];
}

interface Material {
  material: string;
  filaments: Record<string, Filament>;
}

interface Filament {
  id: string;
  name: string;
  colors: Record<string, Color>;
}

interface Color {
  id: string;
  name: string;
  sizes: Size[];
  variant: Variant;
}

interface Size {
  filament_weight: number;
  diameter: number;
  ean: string;
  purchase_links: PurchaseLink[];
}

interface PurchaseLink {
  store_id: string;
  url: string;
}

interface Variant {
  [key: string]: any;
}

const allowedImageRegex = /\.(png|jpg|jpeg|svg)$/i;
const FILE_READ_CONCURRENCY = 5;
const DIR_CONCURRENCY = 5;

type KeyValueResult<T> = { key: string; value: T } | null;

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const currentIndex = nextIndex++;
      if (currentIndex >= items.length) break;
      results[currentIndex] = await fn(items[currentIndex], currentIndex);
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    () => worker(),
  );

  await Promise.all(workers);
  return results;
}

function compactKeyValueResults<T>(
  results: KeyValueResult<T>[],
): Record<string, T> {
  const record: Record<string, T> = {};

  for (const result of results) {
    if (result) {
      record[result.key] = result.value;
    }
  }

  return record;
}

async function readJsonFile<T>(filePath: string): Promise<T> {
  const content = await readFile(filePath, 'utf-8');
  return JSON.parse(content) as T;
}

async function findLogoFile(dirPath: string): Promise<string> {
  const files = await readdir(dirPath);
  const logoFile = files.find((file) => allowedImageRegex.test(file));
  return logoFile ?? '';
}

async function parseColor(
  colorPath: string,
  colorFolderName: string,
): Promise<KeyValueResult<Color>> {
  const sizesJsonPath = join(colorPath, 'sizes.json');
  const variantJsonPath = join(colorPath, 'variant.json');

  if (!existsSync(sizesJsonPath) || !existsSync(variantJsonPath)) {
    return null;
  }

  const [sizesData, variantData] = await Promise.all([
    readJsonFile<Size[]>(sizesJsonPath),
    readJsonFile<Variant>(variantJsonPath),
  ]);

  return {
    key: colorFolderName,
    value: {
      id: colorFolderName,
      name: variantData.name,
      sizes: sizesData,
      variant: variantData,
    },
  };
}

async function parseFilament(
  materialPath: string,
  filamentFolderName: string,
): Promise<KeyValueResult<Filament>> {
  const filamentPath = join(materialPath, filamentFolderName);
  const filamentJsonPath = join(filamentPath, 'filament.json');

  if (!existsSync(filamentJsonPath)) {
    return null;
  }

  const filamentData = await readJsonFile<Filament>(filamentJsonPath);

  const colorFolders = await readdir(filamentPath, { withFileTypes: true });
  const colorDirents = colorFolders.filter((dirent) => dirent.isDirectory());

  const colorResults = await mapWithConcurrency(
    colorDirents,
    DIR_CONCURRENCY,
    async (colorFolder) => {
      const colorPath = join(filamentPath, colorFolder.name);
      return parseColor(colorPath, colorFolder.name);
    },
  );

  return {
    key: filamentFolderName,
    value: {
      ...filamentData,
      id: filamentData.id,
      name: filamentData.name,
      colors: compactKeyValueResults(colorResults),
    },
  };
}

async function parseMaterial(
  brandPath: string,
  materialFolderName: string,
): Promise<KeyValueResult<Material>> {
  const materialPath = join(brandPath, materialFolderName);
  const materialJsonPath = join(materialPath, 'material.json');

  if (!existsSync(materialJsonPath)) {
    return null;
  }

  const materialData = await readJsonFile<Material>(materialJsonPath);

  const filamentFolders = await readdir(materialPath, { withFileTypes: true });
  const filamentDirents = filamentFolders.filter((dirent) => dirent.isDirectory());

  const filamentResults = await mapWithConcurrency(
    filamentDirents,
    DIR_CONCURRENCY,
    async (filamentFolder) => parseFilament(materialPath, filamentFolder.name),
  );

  return {
    key: materialFolderName,
    value: {
      ...materialData,
      material: materialData.material ?? materialFolderName,
      filaments: compactKeyValueResults(filamentResults),
    },
  };
}

async function parseBrand(
  dataPath: string,
  brandFolderName: string,
): Promise<KeyValueResult<Brand>> {
  const folderName = stripOfIllegalChars(brandFolderName);
  const brandPath = join(dataPath, folderName);
  const brandJsonPath = join(brandPath, 'brand.json');

  if (!existsSync(brandJsonPath)) {
    return null;
  }

  const [brandData, logo] = await Promise.all([
    readJsonFile<Partial<Brand>>(brandJsonPath),
    findLogoFile(brandPath),
  ]);

  const materialFolders = await readdir(brandPath, { withFileTypes: true });
  const materialDirents = materialFolders.filter((dirent) => dirent.isDirectory());

  const materialResults = await mapWithConcurrency(
    materialDirents,
    DIR_CONCURRENCY,
    async (materialFolder) => parseMaterial(brandPath, materialFolder.name),
  );

  return {
    key: folderName,
    value: {
      id: brandData.id ?? folderName,
      name: brandData.name ?? folderName,
      logo,
      website: brandData.website ?? '',
      origin: brandData.origin ?? '',
      materials: compactKeyValueResults(materialResults),
    },
  };
}

async function parseStore(
  storesPath: string,
  storeFolderName: string,
): Promise<KeyValueResult<Store>> {
  const storePath = join(storesPath, storeFolderName);
  const storeJsonPath = join(storePath, 'store.json');

  if (!existsSync(storeJsonPath)) {
    return null;
  }

  const [storeData, logo] = await Promise.all([
    readJsonFile<Partial<Store>>(storeJsonPath),
    findLogoFile(storePath),
  ]);

  return {
    key: storeFolderName,
    value: {
      id: storeData.id ?? storeFolderName,
      name: storeData.name ?? storeFolderName,
      storefront_url: storeData.storefront_url ?? '',
      logo,
      ships_from: storeData.ships_from ?? [],
      ships_to: storeData.ships_to ?? [],
    },
  };
}

export async function loadFilamentDatabase(
  dataPath: string,
  storesPath: string,
): Promise<FilamentDatabase> {
  console.log('Running optimized parser...');
  const startMem = process.memoryUsage().heapUsed;

  try {
    const brandFolders = await readdir(dataPath, { withFileTypes: true });
    const brandDirents = brandFolders.filter((dirent) => dirent.isDirectory());

    const brandResults = await mapWithConcurrency(
      brandDirents,
      FILE_READ_CONCURRENCY,
      async (brandFolder) => parseBrand(dataPath, brandFolder.name),
    );

    const storesFolders = await readdir(storesPath, { withFileTypes: true });
    const storesDirents = storesFolders.filter((dirent) => dirent.isDirectory());

    const storeResults = await mapWithConcurrency(
      storesDirents,
      FILE_READ_CONCURRENCY,
      async (storeFolder) => parseStore(storesPath, storeFolder.name),
    );

    const brands = compactKeyValueResults(brandResults);
    const stores = compactKeyValueResults(storeResults);

    const endMem = process.memoryUsage().heapUsed;
    console.log(
      `Filament DB: ${(endMem / 1024 / 1024).toFixed(2)} MB used (${(
        (endMem - startMem) /
        1024 /
        1024
      ).toFixed(2)} MB delta)`,
    );

    return { brands, stores };
  } catch (error) {
    console.error('Error loading filament database:', error);
    throw error;
  }
}