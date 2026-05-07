const axios = require('axios');
const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');

const CONFIG = {
    CSV_FIRME_URL: 'https://serv-10.carrefour.com.ar:446/DYN/routes/GCP_DYN_DownloadExport',
    XML_MARKETPLACE_URL: 'https://www.carrefour.com.ar/XMLData/test-dy.xml',
    OUTPUT_FILE: 'feed_unificado.csv',
    BASE_URL: 'https://www.carrefour.com.ar',
    // Concurrencia para la simulation. 15 simultáneas mantiene el balance entre
    // velocidad y no saturar el servidor (que ya nos tiró 504s en otras corridas).
    SIMULATION_CONCURRENCY: 15,
    // Lotes para el pase de catalog (mapeo SKU -> sellerId)
    CATALOG_BATCH_SIZE: 40
};

// Helper: lee un campo en cualquier capitalización (VTEX es inconsistente)
function pick(obj, ...keys) {
    for (const k of keys) {
        if (obj && obj[k] !== undefined && obj[k] !== null) return obj[k];
    }
    return undefined;
}

// Helper: GET con reintentos + backoff exponencial.
// El parámetro `timeout` es opcional; algunas URLs (XML pesado de Carrefour)
// necesitan timeouts más largos.
async function getWithRetry(url, attempt = 1, timeout = 15000) {
    const MAX_ATTEMPTS = 3;
    try {
        const res = await axios.get(url, { timeout });
        return res.data;
    } catch (e) {
        if (attempt < MAX_ATTEMPTS) {
            const wait = 1000 * Math.pow(2, attempt); // 2s, 4s, 8s
            console.log(`   ↻ Reintento ${attempt}/${MAX_ATTEMPTS - 1} (${e.response?.status || e.code || 'error'}) en ${wait}ms...`);
            await new Promise(r => setTimeout(r, wait));
            return getWithRetry(url, attempt + 1, timeout);
        }
        throw e;
    }
}

// Helper: POST con reintentos + backoff exponencial
async function postWithRetry(url, body, attempt = 1) {
    const MAX_ATTEMPTS = 3;
    try {
        const res = await axios.post(url, body, {
            timeout: 15000,
            headers: { 'Content-Type': 'application/json' }
        });
        return res.data;
    } catch (e) {
        if (attempt < MAX_ATTEMPTS) {
            const wait = 500 * Math.pow(2, attempt);
            await new Promise(r => setTimeout(r, wait));
            return postWithRetry(url, body, attempt + 1);
        }
        throw e;
    }
}

// PASE 1: Mapear SKU -> sellerId desde el catalog API.
// La simulation API requiere el sellerId en el body, así que primero lo armamos.
// Esto es un sweep rápido en lotes de 40 (no 1 por SKU).
async function buildSkuToSellerMap(skuIds) {
    console.log(`🗺️  Pase 1/2: Mapeando SKU -> sellerId (${skuIds.length} SKUs en lotes de ${CONFIG.CATALOG_BATCH_SIZE})...`);
    const skuToSeller = {};
    let lotesFallidos = 0;

    for (let i = 0; i < skuIds.length; i += CONFIG.CATALOG_BATCH_SIZE) {
        const chunk = skuIds.slice(i, i + CONFIG.CATALOG_BATCH_SIZE);
        const chunkSet = new Set(chunk);
        const queryParams = chunk.map(id => `fq=skuId:${id}`).join('&');
        const apiUrl = `${CONFIG.BASE_URL}/api/catalog_system/pub/products/search?${queryParams}`;

        try {
            const data = await getWithRetry(apiUrl);

            for (const product of data) {
                if (!product.items) continue;
                for (const item of product.items) {
                    if (!chunkSet.has(item.itemId)) continue;

                    // Buscamos el seller que efectivamente vende el producto:
                    // 1ro el sellerDefault disponible, sino cualquiera disponible, sino el primero.
                    const sellers = item.sellers || [];
                    let chosenSeller = sellers.find(s =>
                        s.sellerDefault === true && s.commertialOffer?.IsAvailable
                    );
                    if (!chosenSeller) {
                        chosenSeller = sellers.find(s => s.commertialOffer?.IsAvailable);
                    }
                    if (!chosenSeller) {
                        chosenSeller = sellers[0];
                    }

                    if (chosenSeller && chosenSeller.sellerId) {
                        skuToSeller[item.itemId] = chosenSeller.sellerId;
                    }
                }
            }
        } catch (e) {
            lotesFallidos++;
            const status = e.response?.status || 'NO_RESPONSE';
            console.log(`⚠️ Lote catalog fallido (status: ${status}) | SKUs: ${chunk.slice(0, 3).join(',')}... (+${chunk.length - 3})`);
        }

        // Progreso cada ~10 lotes
        const lotesProcesados = Math.floor(i / CONFIG.CATALOG_BATCH_SIZE) + 1;
        if (lotesProcesados % 10 === 0 || i + CONFIG.CATALOG_BATCH_SIZE >= skuIds.length) {
            console.log(`   ${Object.keys(skuToSeller).length} mapeados de ${skuIds.length}...`);
        }

        await new Promise(r => setTimeout(r, 200));
    }

    console.log(`✅ Pase 1 listo: ${Object.keys(skuToSeller).length}/${skuIds.length} SKUs mapeados.`);
    if (lotesFallidos > 0) console.log(`⚠️ ${lotesFallidos} lote(s) fallaron en el mapeo.`);

    return skuToSeller;
}

// Extrae el máximo de cuotas SIN interés de la respuesta de simulation.
// Estructura: data.paymentData.installmentOptions[].installments[]
// Solo contamos interestRate === 0 estricto Y count > 1 (descartamos pago contado).
function extractMaxCuotasFromSimulation(simulationData) {
    let maxCuotas = 0;

    const paymentData = simulationData?.paymentData || {};
    const installmentOptions = paymentData.installmentOptions || [];

    for (const option of installmentOptions) {
        const installments = option.installments || [];
        for (const inst of installments) {
            const interestRate = pick(inst, 'interestRate', 'InterestRate');
            const count = pick(inst, 'count', 'Count', 'NumberOfInstallments');

            // ESTRICTO: interestRate exactamente 0 numérico, y más de 1 cuota
            if (interestRate === 0 && count > 1 && count > maxCuotas) {
                maxCuotas = count;
            }
        }
    }

    return maxCuotas;
}

// PASE 2: Simulation API por SKU, con concurrencia controlada.
// Esta es la fuente de verdad: el JSON viene con los interestRate REALES tal
// como los cobra cada medio de pago en el checkout, sin valores inflados de display.
async function getRealInstallments(skuToSeller) {
    const entries = Object.entries(skuToSeller); // [[skuId, sellerId], ...]
    console.log(`🛒 Pase 2/2: Simulation por SKU (${entries.length} SKUs, concurrencia ${CONFIG.SIMULATION_CONCURRENCY})...`);

    const realInstallmentsMap = {};
    let procesados = 0;
    let fallidos = 0;
    const startTime = Date.now();

    // Worker que toma SKUs de la cola hasta vaciarla
    let cursor = 0;
    async function worker() {
        while (cursor < entries.length) {
            const idx = cursor++;
            const [skuId, sellerId] = entries[idx];

            const url = `${CONFIG.BASE_URL}/api/checkout/pub/orderforms/simulation?sc=1`;
            const body = {
                items: [{ id: skuId, quantity: 1, seller: sellerId }],
                country: 'ARG'
            };

            try {
                const data = await postWithRetry(url, body);
                const cuotas = extractMaxCuotasFromSimulation(data);
                if (cuotas > 1) {
                    realInstallmentsMap[skuId] = cuotas;
                }
            } catch (e) {
                fallidos++;
            }

            procesados++;
            // Log de progreso cada 500 SKUs
            if (procesados % 500 === 0) {
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
                const rate = (procesados / elapsed).toFixed(1);
                console.log(`   ${procesados}/${entries.length} procesados | ${rate} SKU/s | con cuotas: ${Object.keys(realInstallmentsMap).length}`);
            }
        }
    }

    // Lanzamos N workers en paralelo
    const workers = [];
    for (let i = 0; i < CONFIG.SIMULATION_CONCURRENCY; i++) {
        workers.push(worker());
    }
    await Promise.all(workers);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(`✅ Pase 2 listo en ${elapsed}s: ${Object.keys(realInstallmentsMap).length} productos con cuotas sin interés.`);
    if (fallidos > 0) console.log(`⚠️ ${fallidos} simulation(es) fallaron tras todos los reintentos.`);

    return realInstallmentsMap;
}

async function run() {
    console.log('🚀 Iniciando unificación...');

    try {
        console.log('📥 Descargando XML de Marketplace...');
        // Timeout extendido (60s) + retries: el servidor de Carrefour suele tirar 504 en este endpoint.
        const xmlData = await getWithRetry(`${CONFIG.XML_MARKETPLACE_URL}?nocache=${Date.now()}`, 1, 60000);
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(xmlData);
        const mktpItems = jsonObj.DY.channel.item;
        console.log(`✅ ${mktpItems.length} productos de marketplace listos.`);

        // Extraer SKUs del XML
        const skuIds = [];
        for (const item of mktpItems) {
            const match = item.link.match(/idsku=(\d+)/);
            if (match) skuIds.push(match[1]);
        }

        // Pase 1: SKU -> sellerId
        const skuToSeller = await buildSkuToSellerMap(skuIds);

        // Pase 2: simulation por SKU
        const realInstallmentsMap = await getRealInstallments(skuToSeller);

        const outputStream = fs.createWriteStream(CONFIG.OUTPUT_FILE, { encoding: 'utf8' });
        // BOM para que Excel/Dynamic Yield interpreten correctamente UTF-8
        outputStream.write('\uFEFF');

        console.log('📥 Procesando CSV de Firme...');
        const csvRes = await axios({
            method: 'get',
            url: CONFIG.CSV_FIRME_URL,
            responseType: 'stream'
        });

        let headers = [];
        let isFirstLine = true;
        let remainder = '';
        let fileSeparator = ';';

        for await (const chunk of csvRes.data) {
            const lines = (remainder + chunk.toString()).split(/\r?\n/);
            remainder = lines.pop();

            for (let line of lines) {
                if (isFirstLine) {
                    if (line.includes('\t')) fileSeparator = '\t';
                    else if (line.includes(';')) fileSeparator = ';';
                    else if (line.includes(',')) fileSeparator = ',';

                    headers = line.split(fileSeparator).map(h => h.trim());
                    outputStream.write(line + '\n');
                    isFirstLine = false;
                } else {
                    outputStream.write(line + '\n');
                }
            }
        }

        console.log('➕ Agregando productos de Marketplace...');
        for (const item of mktpItems) {
            const row = buildMktpRow(item, headers, fileSeparator, realInstallmentsMap);
            outputStream.write(row + '\n');
        }

        outputStream.end();
        console.log('✨ Proceso finalizado con éxito.');

    } catch (err) {
        console.error('❌ Error crítico:', err.message);
        process.exit(1);
    }
}

function parseArsPrice(p) {
    if (!p) return "0.00";
    const cleaned = String(p).replace(/ARS\s*/gi, '').replace(/\./g, '').replace(',', '.').replace(/[^0-9.-]+/g, '');
    return parseFloat(cleaned).toFixed(2);
}

function buildMktpRow(item, headers, fileSeparator, realInstallmentsMap) {
    const price = parseArsPrice(item.sale_price || item.price);
    const inStock = item.availability === 'in stock' ? 'true' : 'false';
    const brand = item.brand || '';

    let ribbonValue = '';
    const match = item.link.match(/idsku=(\d+)/);
    if (match) {
        const skuId = match[1];
        const cuotasReales = realInstallmentsMap[skuId];
        if (cuotasReales) {
            ribbonValue = `${cuotasReales} Cuotas sin interés`;
        }
    }

    return headers.map(h => {
        switch (h) {
            case 'sku': return item.id;
            case 'group_id': return item.id;
            case 'name': return `"${item.title.replace(/"/g, '""')}"`;
            case 'url': return item.link;
            case 'image_url': return item.image_link;
            case 'categories': return `"${(item.product_type || 'Marketplace').replace(/ > /g, '|')}"`;
            case 'ribbons': return ribbonValue ? `"${ribbonValue}"` : '';
            case 'keywords': return `"${brand}"`;
            case 'price': return price;
            case 'in_stock': return inStock;
            default:
                if (h.startsWith('lng:carrefourar')) {
                    if (h.endsWith(':price')) return price;
                    if (h.endsWith(':in_stock')) return inStock;
                }
                return '';
        }
    }).join(fileSeparator);
}

run();
